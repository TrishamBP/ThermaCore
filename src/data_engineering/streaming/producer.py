"""Publish telemetry events to Kafka (topic `datacenter.telemetry` by default).

    uv run python -m data_engineering.streaming.producer simulate --assets 3 --duration-minutes 10
    uv run python -m data_engineering.streaming.producer publish-stored
    uv run python -m data_engineering.streaming.producer simulate --dry-run --max-events 2

Modes
    simulate        generate events with the Phase 1 simulator while publishing (Mode B)
    publish-stored  stream a simulation run already stored in SQLite (Mode A)

Exit status
    0  every event was validated and confirmed delivered by the broker
    1  some events were rejected (schema), failed delivery, or were still unconfirmed at flush
    2  configuration, data-source, broker or topic problem before publishing
"""

import argparse
import copy
import signal
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Callable, Iterable

from data_engineering.config import load_config
from data_engineering.db import connect
from data_engineering.logging_utils import get_logger
from data_engineering.paths import CONFIG_PATH, db_path
from data_engineering.simulation.generate_telemetry import SimulationConfigError, parse_utc
from data_engineering.streaming.events import SCHEMA_VERSION, serialize_event, validate_event
from data_engineering.streaming.settings import ProducerSettings, SettingsError, load_settings
from data_engineering.streaming.sources import (
    SourceError, build_simulator, latest_run_id, simulated_events, stored_run_events, stored_run_size)

log = get_logger("kafka_producer")
EXIT_OK, EXIT_PUBLISH_FAILED, EXIT_CONFIG = 0, 1, 2


class BrokerError(RuntimeError):
    pass


@dataclass
class PublishStats:
    attempted: int = 0      # events read from the source
    rejected: int = 0       # failed schema validation or serialization; never sent
    queued: int = 0         # accepted into the client's local buffer (NOT yet delivered)
    delivered: int = 0      # broker acknowledged (per acks setting)
    failed: int = 0         # delivery callback reported an error, or produce() raised
    undelivered: int = 0    # still unconfirmed when the final flush timed out
    queue_full_waits: int = 0

    @property
    def ok(self) -> bool:
        return self.rejected == 0 and self.failed == 0 and self.undelivered == 0


class Pacer:
    """Releases events on a schedule: a fixed rate, or event time scaled by `speedup`."""

    def __init__(self, rate: float = 0.0, speedup: float = 0.0,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.rate, self.speedup, self.clock = rate, speedup, clock
        self._t0: float | None = None
        self._first_ts: datetime | None = None

    def due_in(self, index: int, event_timestamp: str) -> float:
        """Seconds to wait before releasing event number `index` (0-based)."""
        now = self.clock()
        if self._t0 is None:
            self._t0, self._first_ts = now, parse_utc(event_timestamp)
        if self.rate > 0:
            due = self._t0 + index / self.rate
        elif self.speedup > 0:
            due = self._t0 + (parse_utc(event_timestamp) - self._first_ts).total_seconds() / self.speedup
        else:
            return 0.0
        return max(0.0, due - now)


class TelemetryPublisher:
    """Wraps a confluent_kafka.Producer: validation, keyed produce, delivery tracking, flush."""

    def __init__(self, settings: ProducerSettings, producer_factory: Callable[[dict], object] | None = None) -> None:
        if producer_factory is None:
            from confluent_kafka import Producer
            producer_factory = Producer
        self.settings = settings
        self.stats = PublishStats()
        self._stop_requested = False
        self._fatal_error = None
        self._error_codes_logged: set = set()
        config = settings.producer_config()
        config["error_cb"] = self._on_error
        self.producer = producer_factory(config)

    # ----- callbacks (run inside poll()/flush()) -----
    def _on_delivery(self, err, msg) -> None:
        if err is None:
            self.stats.delivered += 1
            return
        self.stats.failed += 1
        if self.stats.failed <= 10 or self.stats.failed % 1000 == 0:
            log.error("delivery failed", extra={"context": {
                "error": str(err), "key": (msg.key() or b"").decode("utf-8", "replace") if msg else None,
                "failed_so_far": self.stats.failed}})

    def _on_error(self, err) -> None:
        fatal = getattr(err, "fatal", lambda: False)()
        if fatal:
            self._fatal_error = err
            self._stop_requested = True
            log.error("fatal Kafka client error; stopping", extra={"context": {"error": str(err)}})
        elif err.code() not in self._error_codes_logged:
            self._error_codes_logged.add(err.code())
            log.warning("Kafka client error (librdkafka keeps retrying until delivery.timeout.ms)",
                        extra={"context": {"error": str(err)}})

    def request_stop(self) -> None:
        self._stop_requested = True

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested

    @property
    def fatal_error(self):
        return self._fatal_error

    # ----- broker / topic check -----
    def check_topic(self) -> int:
        """Return the topic's partition count, or raise BrokerError with a clear reason."""
        from confluent_kafka import KafkaException
        topic = self.settings.topic
        try:
            metadata = self.producer.list_topics(topic=topic, timeout=self.settings.metadata_timeout_s)
        except KafkaException as exc:
            raise BrokerError(
                f"cannot reach Kafka at {self.settings.bootstrap_servers} within "
                f"{self.settings.metadata_timeout_s}s ({exc}). Is the broker running "
                "(`docker compose up -d kafka`) and is KAFKA_BOOTSTRAP_SERVERS correct?") from exc
        info = metadata.topics.get(topic)
        if info is None or info.error is not None:
            reason = info.error if info is not None else "not in metadata"
            raise BrokerError(f"topic {topic!r} is not available ({reason}). Create it with "
                              "`uv run python -m data_engineering.streaming.topic_setup`.")
        return len(info.partitions)

    # ----- publishing -----
    def _produce(self, key: bytes, value: bytes, headers: list) -> bool:
        """Queue one message, waiting while the local queue is full. Returns False if it failed."""
        from confluent_kafka import KafkaException
        waited = 0.0
        while True:
            try:
                self.producer.produce(self.settings.topic, key=key, value=value, headers=headers,
                                      on_delivery=self._on_delivery)
                return True
            except BufferError:
                # Local queue full: serve delivery callbacks to free space, then retry.
                self.stats.queue_full_waits += 1
                if waited >= self.settings.delivery_timeout_ms / 1000:
                    log.error("local queue stayed full past delivery timeout")
                    return False
                self.producer.poll(0.5)
                waited += 0.5
            except KafkaException as exc:
                log.error("produce() rejected message", extra={"context": {"error": str(exc)}})
                return False

    def _idle(self, seconds: float) -> None:
        """Wait while serving delivery callbacks; wakes early if a stop is requested."""
        end = time.monotonic() + seconds
        while not self._stop_requested:
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            self.producer.poll(min(remaining, 0.1))

    def publish(self, events: Iterable[dict], max_events: int | None = None,
                pacer: Pacer | None = None) -> PublishStats:
        try:
            for event in events:
                if self._stop_requested or (max_events is not None and self.stats.attempted >= max_events):
                    break
                if pacer is not None:
                    wait = pacer.due_in(self.stats.attempted, event.get("event_timestamp", ""))
                    if wait > 0:
                        self._idle(wait)
                        if self._stop_requested:
                            break
                self.stats.attempted += 1
                problems = validate_event(event)
                try:
                    payload = None if problems else serialize_event(event)
                except (TypeError, ValueError) as exc:
                    problems = [f"serialization: {exc}"]
                if problems:
                    self.stats.rejected += 1
                    log.error("event rejected by schema validation", extra={"context": {
                        "event_id": event.get("event_id"), "problems": problems[:5]}})
                    continue
                headers = [("schema_version", SCHEMA_VERSION.encode()),
                           ("source_type", event["source_type"].encode()),
                           ("content-type", b"application/json")]
                if self._produce(event["asset_id"].encode("utf-8"), payload, headers):
                    self.stats.queued += 1
                else:
                    self.stats.failed += 1
                self.producer.poll(0)  # serve delivery callbacks without blocking
        finally:
            remaining = self.producer.flush(self.settings.flush_timeout_s)
            self.stats.undelivered = remaining
            if remaining:
                log.error("messages still unconfirmed after flush timeout",
                          extra={"context": {"undelivered": remaining}})
        return self.stats


def _sim_config(args) -> dict:
    config = copy.deepcopy(load_config(args.config))
    sim = config["simulation"]
    for key, value in (("asset_count", args.assets), ("duration_minutes", args.duration_minutes),
                       ("frequency_seconds", args.interval_seconds), ("start_ts_utc", args.start),
                       ("seed", args.seed), ("missing_rate", args.missing_rate)):
        if value is not None:
            sim[key] = value
    return config


def _install_signal_handlers(publisher: TelemetryPublisher) -> None:
    def handler(signum, _frame):
        log.warning("stop requested; flushing outstanding messages", extra={"context": {"signal": signum}})
        publisher.request_stop()
    # SIGINT = Ctrl+C; SIGTERM = `docker stop`/kill on Linux; SIGBREAK = Ctrl+Break on Windows.
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), handler)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m data_engineering.streaming.producer",
                                 description="Publish SYNTHETIC data center telemetry events to Kafka.")
    sub = ap.add_subparsers(dest="mode", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--bootstrap-servers", help="override KAFKA_BOOTSTRAP_SERVERS")
    common.add_argument("--topic", help="override KAFKA_TOPIC (default datacenter.telemetry)")
    common.add_argument("--max-events", type=int, help="stop after this many events")
    pace = common.add_mutually_exclusive_group()
    pace.add_argument("--rate", type=float, help="events per second (default: unthrottled)")
    pace.add_argument("--speedup", type=float,
                      help="simulated seconds per real second (1 = real time, 60 = 1 simulated minute/s)")
    common.add_argument("--dry-run", action="store_true",
                        help="validate and print events as JSON lines instead of sending to Kafka")
    common.add_argument("--config", default=None, help=f"simulation config (default {CONFIG_PATH.name})")

    sim = sub.add_parser("simulate", parents=[common], help="generate events while publishing (Mode B)")
    sim.add_argument("--assets", type=int, help="number of simulated assets")
    sim.add_argument("--duration-minutes", type=int, help="simulated duration (one window in continuous mode)")
    sim.add_argument("--interval-seconds", type=int, help="seconds between readings (default 60)")
    sim.add_argument("--start", help="simulation start, UTC, e.g. 2026-01-01T00:00:00Z")
    sim.add_argument("--seed", type=int, help="random seed")
    sim.add_argument("--missing-rate", type=float, help="probability that a slot has no reading")
    sim.add_argument("--continuous", action="store_true",
                     help="keep generating past the duration until stopped (Ctrl+C) or --max-events")

    stored = sub.add_parser("publish-stored", parents=[common], help="publish a run stored in SQLite (Mode A)")
    stored.add_argument("--run-id", help="simulation run to publish (default: latest)")
    return ap


def main(argv: list[str] | None = None, producer_factory=None) -> int:
    args = build_parser().parse_args(argv)
    if args.max_events is not None and args.max_events < 1:
        print("error: --max-events must be >= 1", file=sys.stderr)
        return EXIT_CONFIG
    if args.dry_run and getattr(args, "continuous", False) and args.max_events is None:
        print("error: --dry-run with --continuous needs --max-events", file=sys.stderr)
        return EXIT_CONFIG
    try:
        settings = load_settings(bootstrap_servers=args.bootstrap_servers, topic=args.topic,
                                 rate_events_per_s=args.rate, speedup=args.speedup)
        config = _sim_config(args) if args.mode == "simulate" else load_config(args.config)
        data_center_id = config.get("streaming", {}).get("data_center_id", "dc-na-sim-001")
        if not db_path().exists():
            raise SourceError(f"{db_path()} not found. Run `uv run python -m data_engineering.pipeline` first.")
        conn = connect()
    except (SettingsError, SourceError, SimulationConfigError, FileNotFoundError) as exc:
        log.error("configuration error", extra={"context": {"error": str(exc)}})
        return EXIT_CONFIG

    try:
        if args.mode == "simulate":
            simulator = build_simulator(conn, config)
            events = simulated_events(simulator, data_center_id, continuous=args.continuous)
            run_id, expected = simulator.run_id, None if args.continuous else simulator.n_slots * len(simulator.assets)
        else:
            run_id = args.run_id or latest_run_id(conn)
            expected = stored_run_size(conn, run_id)
            events = stored_run_events(conn, run_id, data_center_id)
        if args.max_events is not None:
            expected = args.max_events if expected is None else min(expected, args.max_events)
        log.info("producer starting", extra={"context": {
            "mode": args.mode, "run_id": run_id, "expected_events": expected, "topic": settings.topic,
            "bootstrap_servers": settings.bootstrap_servers, "dry_run": args.dry_run,
            "rate": settings.rate_events_per_s, "speedup": settings.speedup}})

        pacer = Pacer(settings.rate_events_per_s, settings.speedup) if (
            settings.rate_events_per_s or settings.speedup) else None
        if args.dry_run:
            return _dry_run(events, args.max_events, pacer)

        publisher = TelemetryPublisher(settings, producer_factory)
        partitions = publisher.check_topic()
        log.info("broker and topic verified", extra={"context": {"topic": settings.topic, "partitions": partitions}})
        _install_signal_handlers(publisher)
        start = time.monotonic()
        stats = publisher.publish(events, args.max_events, pacer)
    except (SourceError, SimulationConfigError, BrokerError, SettingsError) as exc:
        log.error("producer could not start", extra={"context": {"error": str(exc)}})
        return EXIT_CONFIG
    finally:
        conn.close()

    status = "SUCCESS" if stats.ok and not publisher.fatal_error else "FAILED"
    log.info("producer finished", extra={"context": {
        **asdict(stats), "status": status, "stopped_early": publisher.stop_requested,
        "seconds": round(time.monotonic() - start, 3), "run_id": run_id}})
    print(f"{status}: attempted={stats.attempted} delivered={stats.delivered} rejected={stats.rejected} "
          f"failed={stats.failed} undelivered={stats.undelivered} (queued={stats.queued})", file=sys.stderr)
    return EXIT_OK if status == "SUCCESS" else EXIT_PUBLISH_FAILED


def _dry_run(events: Iterable[dict], max_events: int | None, pacer: Pacer | None) -> int:
    count = rejected = 0
    for event in events:
        if max_events is not None and count >= max_events:
            break
        if pacer is not None:
            time.sleep(pacer.due_in(count, event["event_timestamp"]))
        count += 1
        problems = validate_event(event)
        if problems:
            rejected += 1
            log.error("event rejected by schema validation", extra={"context": {"problems": problems[:5]}})
            continue
        sys.stdout.write(serialize_event(event).decode("utf-8") + "\n")
    print(f"DRY RUN: {count} events validated, {rejected} rejected, nothing sent to Kafka", file=sys.stderr)
    return EXIT_OK if rejected == 0 else EXIT_PUBLISH_FAILED


if __name__ == "__main__":
    sys.exit(main())
