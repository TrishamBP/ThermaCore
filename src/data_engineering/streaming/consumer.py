"""Kafka consumer: `datacenter.telemetry` -> telemetry store (SQLite/PostgreSQL) -> Parquet.

    uv run python -m data_engineering.streaming.consumer                     # run until Ctrl+C
    uv run python -m data_engineering.streaming.consumer --idle-timeout-s 10  # stop after 10 s with no messages
    uv run python -m data_engineering.streaming.consumer --max-messages 500   # finite demo run

Per batch (up to CONSUMER_BATCH_MAX_MESSAGES messages or CONSUMER_BATCH_MAX_WAIT_S seconds):
  1. validate every message against the producer's schema (event_validation.parse_message)
  2. write all valid events in ONE database transaction; idempotent on event_id;
     transient database errors are retried with bounded exponential backoff + jitter
  3. publish invalid messages to the dead-letter topic and wait for broker confirmation
  4. commit offsets: per partition, only up to the last contiguous message that was
     stored or dead-lettered, never past an unhandled one
  5. export to Parquet from the database ledger when the size/age thresholds are reached

Delivery is at-least-once: a crash between step 2 and step 4 redelivers the batch, and the
store deduplicates it by event_id. There is no exactly-once across Kafka and the database.

Exit status: 0 ok; 1 runtime failure (store/DLQ retries exhausted, or Parquet exports still
pending at shutdown); 2 configuration or startup problem.
"""

import argparse
import logging
import random
import signal
import sys
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Callable

from data_engineering.logging_utils import get_logger
from data_engineering.storage.base import (
    BatchRecord, KafkaPosition, open_store, prepare_event, utcnow)
from data_engineering.storage.parquet_sink import ParquetExporter
from data_engineering.streaming.dead_letter import DeadLetterPublisher, build_dead_letter
from data_engineering.streaming.event_validation import Rejection, parse_message
from data_engineering.streaming.settings import ConsumerSettings, load_consumer_settings

log = get_logger("kafka_consumer")
EXIT_OK, EXIT_RUNTIME, EXIT_CONFIG = 0, 1, 2
_LOG_FIRST_N = 5


class FatalConsumerError(RuntimeError):
    """Processing cannot continue safely; offsets of unhandled messages stay uncommitted."""


class StartupError(RuntimeError):
    pass


@dataclass
class ConsumerStats:
    messages_consumed: int = 0
    kafka_errors: int = 0
    batches: int = 0
    events_valid: int = 0
    events_rejected: int = 0
    rejected_by_reason: Counter = field(default_factory=Counter)
    rows_inserted: int = 0
    rows_deduplicated: int = 0
    rows_conflicting: int = 0
    anomaly_rows_inserted: int = 0
    dlq_published: int = 0
    dlq_unconfirmed: int = 0
    retries: int = 0
    failures: int = 0
    commits: int = 0
    commit_failures: int = 0
    key_mismatches: int = 0
    consumer_lag: int | None = None

    def as_dict(self) -> dict:
        d = asdict(self)
        d["rejected_by_reason"] = dict(self.rejected_by_reason)
        return d


class OffsetTracker:
    """Which offsets of each partition are handled (stored or dead-lettered).

    committable() returns, per partition, the offset to commit: one past the last message
    of the *contiguous* handled run starting at the lowest tracked offset. An unhandled
    message blocks every later offset of its partition from being committed.
    """

    def __init__(self) -> None:
        self._state: dict[tuple[str, int], dict[int, bool]] = {}

    def add(self, topic: str, partition: int, offset: int) -> None:
        self._state.setdefault((topic, partition), {})[offset] = False

    def mark_handled(self, topic: str, partition: int, offset: int) -> None:
        self._state[(topic, partition)][offset] = True

    def committable(self) -> dict[tuple[str, int], int]:
        out = {}
        for tp, offsets in self._state.items():
            next_offset = None
            for offset in sorted(offsets):
                if not offsets[offset]:
                    break
                next_offset = offset + 1
            if next_offset is not None:
                out[tp] = next_offset
        return out

    def forget_through(self, tp: tuple[str, int], next_offset: int) -> None:
        offsets = self._state.get(tp, {})
        for offset in [o for o in offsets if o < next_offset]:
            del offsets[offset]
        if not offsets:
            self._state.pop(tp, None)

    def forget_partition(self, tp: tuple[str, int]) -> int:
        return len(self._state.pop(tp, {}))

    def unhandled(self) -> int:
        return sum(1 for offsets in self._state.values() for done in offsets.values() if not done)


class TelemetryConsumer:
    def __init__(self, settings: ConsumerSettings, store, exporter: ParquetExporter | None = None,
                 consumer_factory: Callable[[dict], object] | None = None,
                 dlq_producer_factory: Callable[[dict], object] | None = None,
                 sleep: Callable[[float], None] = time.sleep, rng: Callable[[], float] = random.random) -> None:
        if consumer_factory is None:
            from confluent_kafka import Consumer
            consumer_factory = Consumer
        self.settings, self.store, self.exporter = settings, store, exporter
        self.consumer = consumer_factory(settings.consumer_config())
        self.dlq = DeadLetterPublisher(settings.dlq_topic, settings.dlq_producer_config(),
                                       settings.dlq_flush_timeout_s, dlq_producer_factory)
        self.tracker = OffsetTracker()
        self.stats = ConsumerStats()
        self._sleep, self._rng = sleep, rng
        self._stop = False
        self._assigned: set = set()
        self._last_activity = time.monotonic()   # last message or partition assignment

    # ----- control -----
    def request_stop(self) -> None:
        self._stop = True

    def check_topics(self) -> None:
        from confluent_kafka import KafkaException
        try:
            metadata = self.consumer.list_topics(timeout=self.settings.metadata_timeout_s)
        except KafkaException as exc:
            raise StartupError(f"cannot reach Kafka at {self.settings.bootstrap_servers}: {exc}. "
                               "Is the broker running (`docker compose up -d --wait kafka`)?") from exc
        missing = [t for t in (self.settings.topic, self.settings.dlq_topic)
                   if t not in metadata.topics or metadata.topics[t].error is not None]
        if missing:
            raise StartupError(f"topic(s) {missing} not found. Create them with "
                               "`uv run python -m data_engineering.streaming.topic_setup`.")

    def _on_assign(self, _consumer, partitions) -> None:
        self._assigned |= {(p.topic, p.partition) for p in partitions}
        self._last_activity = time.monotonic()
        log.info("partitions assigned", extra={"context": {
            "partitions": sorted(p.partition for p in partitions), "group": self.settings.group_id}})

    def _on_revoke(self, _consumer, partitions) -> None:
        # Batches are processed and committed before the next poll, so normally nothing is
        # pending here. Anything still tracked (e.g. after a failed commit) will be redelivered
        # to the partition's new owner and deduplicated by event_id.
        dropped = 0
        for p in partitions:
            self._assigned.discard((p.topic, p.partition))
            dropped += self.tracker.forget_partition((p.topic, p.partition))
        log.info("partitions revoked", extra={"context": {
            "partitions": sorted(p.partition for p in partitions), "uncommitted_offsets_dropped": dropped}})

    # ----- retries -----
    def _backoff(self, attempt: int) -> float:
        cap = min(self.settings.retry_backoff_max_s, self.settings.retry_backoff_initial_s * (2 ** attempt))
        return cap / 2 + self._rng() * cap / 2          # "equal jitter": spread retries, never zero

    def _with_retries(self, what: str, op: Callable, is_transient: Callable[[BaseException], bool],
                      on_retry: Callable[[], None] | None = None):
        for attempt in range(self.settings.max_retries + 1):
            try:
                return op()
            except Exception as exc:
                transient = is_transient(exc)
                if not transient or attempt == self.settings.max_retries or self._stop:
                    self.stats.failures += 1
                    kind = "transient, retries exhausted" if transient else "permanent"
                    raise FatalConsumerError(f"{what} failed ({kind}): {type(exc).__name__}: {exc}") from exc
                delay = self._backoff(attempt)
                self.stats.retries += 1
                log.warning(f"{what} failed; retrying", extra={"context": {
                    "attempt": attempt + 1, "max_retries": self.settings.max_retries,
                    "delay_s": round(delay, 3), "error": f"{type(exc).__name__}: {exc}"}})
                self._sleep(delay)
                if on_retry is not None:
                    try:
                        on_retry()
                    except Exception as reconnect_exc:
                        log.warning("reconnect attempt failed", extra={"context": {"error": str(reconnect_exc)}})

    # ----- batch processing -----
    def process_batch(self, messages: list) -> None:
        started, t0 = utcnow(), time.monotonic()
        batch_id = uuid.uuid4().hex
        prepared, rejects, offsets = [], [], {}
        for msg in messages:
            topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
            self.tracker.add(topic, partition, offset)
            first_last = offsets.setdefault(partition, [offset, offset])
            first_last[0], first_last[1] = min(first_last[0], offset), max(first_last[1], offset)
            result = parse_message(msg.value())
            if isinstance(result, Rejection):
                rejects.append((msg, result))
                self.stats.rejected_by_reason[result.reason] += 1
                if self.stats.rejected_by_reason[result.reason] <= _LOG_FIRST_N:
                    log.warning("message rejected", extra={"context": {
                        "reason": result.reason, "details": list(result.details[:3]),
                        "partition": partition, "offset": offset}})
                continue
            key = msg.key()
            if key is not None and key.decode("utf-8", "replace") != result.event["asset_id"]:
                self.stats.key_mismatches += 1
                if self.stats.key_mismatches <= _LOG_FIRST_N:
                    log.warning("message key differs from asset_id (stored anyway; per-asset ordering "
                                "is not guaranteed for such messages)",
                                extra={"context": {"partition": partition, "offset": offset}})
            prepared.append((msg, prepare_event(result, KafkaPosition(topic, partition, offset))))

        batch = BatchRecord(batch_id, self.settings.group_id, self.settings.topic, started,
                            len(messages), len(rejects), offsets)
        # 1) database: one transaction, idempotent, retried on transient errors
        result = self._with_retries("database write",
                                    lambda: self.store.persist_batch(batch, [p for _, p in prepared]),
                                    self.store.is_transient, on_retry=self.store.reconnect)
        for msg, _ in prepared:
            self.tracker.mark_handled(msg.topic(), msg.partition(), msg.offset())
        if result.conflicting:
            log.warning("conflicting duplicates: same event_id, different content; first stored version kept",
                        extra={"context": {"count": result.conflicting, "examples": result.conflict_event_ids[:3]}})

        # 2) dead-letter queue for permanently invalid messages
        unconfirmed = []
        if rejects:
            letters = [build_dead_letter(m, r, self.settings.group_id) for m, r in rejects]
            remaining = letters
            for attempt in range(self.settings.max_retries + 1):
                remaining = self.dlq.publish(remaining)
                if not remaining or attempt == self.settings.max_retries or self._stop:
                    break
                self.stats.retries += 1
                delay = self._backoff(attempt)
                log.warning("DLQ publish not confirmed; retrying", extra={"context": {
                    "unconfirmed": len(remaining), "attempt": attempt + 1, "delay_s": round(delay, 3),
                    "errors": list(getattr(self.dlq, "last_errors", {}).values())[:3]}})
                self._sleep(delay)
            unconfirmed = remaining
            confirmed = [letter for letter in letters if letter not in unconfirmed]
            for letter in confirmed:
                self.tracker.mark_handled(self.settings.topic, letter.source_partition, letter.source_offset)
            self.stats.dlq_published += len(confirmed)
            self.stats.dlq_unconfirmed += len(unconfirmed)

        # 3) commit what is safe (contiguous handled prefix per partition)
        committed = self._commit()
        status = "committed" if committed and not unconfirmed else "commit_failed" if not committed else "persisted"
        try:
            self.store.mark_batch(batch_id, status, len(rejects) - len(unconfirmed))
        except Exception as exc:  # bookkeeping only; the data itself is already committed
            log.warning("could not update batch status", extra={"context": {"batch_id": batch_id, "error": str(exc)}})

        self.stats.batches += 1
        self.stats.messages_consumed += len(messages)
        self.stats.events_valid += len(prepared)
        self.stats.events_rejected += len(rejects)
        self.stats.rows_inserted += result.inserted
        self.stats.rows_deduplicated += result.deduplicated
        self.stats.rows_conflicting += result.conflicting
        self.stats.anomaly_rows_inserted += result.anomaly_rows
        self.stats.consumer_lag = self._lag()
        log.info("batch processed", extra={"context": {
            "batch_id": batch_id, "messages": len(messages), "valid": len(prepared), "rejected": len(rejects),
            "inserted": result.inserted, "deduplicated": result.deduplicated, "conflicting": result.conflicting,
            "anomaly_rows": result.anomaly_rows, "dlq_published": len(rejects) - len(unconfirmed),
            "committed": committed, "partitions": {str(p): r for p, r in sorted(offsets.items())},
            "lag": self.stats.consumer_lag, "seconds": round(time.monotonic() - t0, 3)}})

        if unconfirmed:
            raise FatalConsumerError(
                f"{len(unconfirmed)} invalid message(s) could not be published to {self.settings.dlq_topic}; "
                "their offsets (and every later offset of the same partitions) were NOT committed, "
                "so they will be redelivered after the DLQ is available again")

    def _commit(self) -> bool:
        from confluent_kafka import KafkaError, KafkaException, TopicPartition
        offsets = self.tracker.committable()
        if not offsets:
            return True
        tps = [TopicPartition(topic, partition, offset) for (topic, partition), offset in sorted(offsets.items())]
        for attempt in range(self.settings.max_retries + 1):
            try:
                self.consumer.commit(offsets=tps, asynchronous=False)
            except KafkaException as exc:
                err = exc.args[0] if exc.args else None
                self.stats.commit_failures += 1
                retriable = bool(err is not None and err.retriable()) and err.code() not in (
                    KafkaError.REBALANCE_IN_PROGRESS, KafkaError.ILLEGAL_GENERATION, KafkaError.UNKNOWN_MEMBER_ID)
                log.warning("offset commit failed", extra={"context": {
                    "error": str(err), "retriable": retriable, "attempt": attempt + 1,
                    "offsets": {f"{t}:{p}": o for (t, p), o in sorted(offsets.items())}}})
                if not retriable or attempt == self.settings.max_retries or self._stop:
                    log.warning("giving up on this commit: the already-stored events will be redelivered "
                                "and deduplicated by event_id (a later successful commit also covers them)")
                    return False
                self._sleep(self._backoff(attempt))
                continue
            self.stats.commits += 1
            for tp, offset in offsets.items():
                self.tracker.forget_through(tp, offset)
            return True
        return False

    def _lag(self) -> int | None:
        """Sum over assigned partitions of (high watermark - committed position), if available."""
        if not self._assigned or not hasattr(self.consumer, "get_watermark_offsets"):
            return None
        from confluent_kafka import TopicPartition
        try:
            tps = [TopicPartition(t, p) for t, p in sorted(self._assigned)]
            positions = {(tp.topic, tp.partition): tp.offset for tp in self.consumer.position(tps)}
            total = 0
            for tp in tps:
                _low, high = self.consumer.get_watermark_offsets(tp, cached=True)
                pos = positions.get((tp.topic, tp.partition), -1)
                if high < 0 or pos < 0:
                    return None
                total += max(0, high - pos)
            return total
        except Exception:
            return None

    # ----- main loop -----
    def run(self, max_messages: int | None = None, max_batches: int | None = None,
            idle_timeout_s: float | None = None) -> int:
        self.consumer.subscribe([self.settings.topic], on_assign=self._on_assign,
                                on_revoke=self._on_revoke, on_lost=self._on_revoke)
        code = EXIT_OK
        started = self._last_activity = time.monotonic()
        # Joining a group can take up to the session timeout (e.g. while a crashed member has not
        # been evicted yet), so the idle timer only runs once partitions are assigned.
        join_grace_s = self.settings.session_timeout_ms / 1000
        try:
            if self.exporter is not None:
                self.exporter.recover()          # finish exports interrupted by a previous crash
            while not self._stop:
                want = self.settings.batch_max_messages
                if max_messages is not None:
                    want = min(want, max_messages - self.stats.messages_consumed)
                batch = []
                for msg in self.consumer.consume(num_messages=want, timeout=self.settings.batch_max_wait_s):
                    err = msg.error()
                    if err is None:
                        batch.append(msg)
                        continue
                    self.stats.kafka_errors += 1
                    if err.fatal():
                        raise FatalConsumerError(f"fatal Kafka error: {err}")
                    log.warning("Kafka error while consuming", extra={"context": {"error": str(err)}})
                if batch:
                    self.process_batch(batch)
                    self._last_activity = time.monotonic()
                if self.exporter is not None:
                    self.exporter.maybe_export()
                if max_messages is not None and self.stats.messages_consumed >= max_messages:
                    break
                if max_batches is not None and self.stats.batches >= max_batches:
                    break
                if idle_timeout_s is not None:
                    now = time.monotonic()
                    if self._assigned and now - self._last_activity >= idle_timeout_s:
                        log.info("idle timeout reached; stopping", extra={"context": {"idle_timeout_s": idle_timeout_s}})
                        break
                    if not self._assigned and now - started >= join_grace_s + idle_timeout_s:
                        log.warning("no partitions were assigned; stopping", extra={"context": {
                            "waited_s": round(now - started, 1), "group": self.settings.group_id}})
                        break
        except FatalConsumerError as exc:
            log.error("consumer stopping after unrecoverable error", extra={"context": {"error": str(exc)}})
            code = EXIT_RUNTIME
        except Exception:
            # A bug or an unclassified failure: stop without committing anything further.
            log.exception("consumer stopping after unexpected error; uncommitted messages will be redelivered")
            code = EXIT_RUNTIME
        finally:
            code = self._shutdown(code)
        return code

    def _shutdown(self, code: int) -> int:
        # Best-effort cleanup that can never skip consumer.close(): close() makes the consumer leave
        # the group at once; without it the broker keeps the dead member until the session timeout
        # and a restarted consumer gets no partitions in the meantime.
        deadline = time.monotonic() + self.settings.shutdown_timeout_s
        try:
            if self.exporter is not None:
                try:
                    self.exporter.flush_all(deadline)
                except Exception as exc:
                    log.error("final parquet export failed",
                              extra={"context": {"error": f"{type(exc).__name__}: {exc}"}})
                try:
                    backlog = self.exporter.backlog()
                except Exception as exc:   # e.g. the database is down
                    backlog = {"unknown": f"{type(exc).__name__}: {exc}"}
                if backlog.get("unknown") or backlog.get("unassigned_rows") or backlog.get("exports_writing"):
                    log.error("parquet export backlog remains (or cannot be checked) at shutdown; it stays "
                              "in the ledger and is exported on the next start", extra={"context": backlog})
                    code = code or EXIT_RUNTIME
            try:
                self.dlq.flush(max(0.0, deadline - time.monotonic()))
            except Exception as exc:
                log.error("DLQ flush failed at shutdown", extra={"context": {"error": str(exc)}})
        finally:
            self.consumer.close()               # auto-commit is off: close() commits nothing
        summary = self.stats.as_dict()
        if self.exporter is not None:
            summary["parquet"] = {k: v for k, v in asdict(self.exporter.stats).items() if k != "files"}
        log.info("consumer stopped", extra={"context": {**summary, "exit_code": code,
                                                         "uncommitted_unhandled": self.tracker.unhandled()}})
        return code


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m data_engineering.streaming.consumer",
                                 description="Consume datacenter.telemetry into the telemetry store and Parquet.")
    ap.add_argument("--max-messages", type=int, help="stop after this many messages (finite run)")
    ap.add_argument("--max-batches", type=int, help="stop after this many batches")
    ap.add_argument("--idle-timeout-s", type=float, help="stop after this many seconds without messages")
    ap.add_argument("--bootstrap-servers", help="override KAFKA_BOOTSTRAP_SERVERS")
    ap.add_argument("--topic", help="override KAFKA_TOPIC")
    ap.add_argument("--dlq-topic", help="override KAFKA_DLQ_TOPIC")
    ap.add_argument("--group-id", help="override KAFKA_GROUP_ID")
    ap.add_argument("--db-backend", choices=["sqlite", "postgres"], help="override TELEMETRY_DB_BACKEND")
    ap.add_argument("--sqlite-path", help="override TELEMETRY_SQLITE_PATH")
    ap.add_argument("--parquet-dir", help="override PARQUET_OUTPUT_DIR")
    ap.add_argument("--no-parquet", action="store_true", help="disable the Parquet sink (PARQUET_ENABLED=false)")
    return ap


def build_exporter(settings: ConsumerSettings, store) -> ParquetExporter | None:
    if not settings.parquet_enabled:
        return None
    return ParquetExporter(store, settings.resolve_path(settings.parquet_dir), settings.parquet_partitioning,
                           settings.parquet_flush_rows, settings.parquet_flush_interval_s,
                           settings.parquet_max_rows_per_file, settings.parquet_compression)


def _install_signal_handlers(consumer: TelemetryConsumer) -> None:
    def handler(signum, _frame):
        log.warning("stop requested; finishing the current batch", extra={"context": {"signal": signum}})
        consumer.request_stop()
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), handler)


def main(argv: list[str] | None = None, consumer_factory=None, dlq_producer_factory=None) -> int:
    args = build_parser().parse_args(argv)
    for name in ("max_messages", "max_batches"):
        if getattr(args, name) is not None and getattr(args, name) < 1:
            print(f"error: --{name.replace('_', '-')} must be >= 1", file=sys.stderr)
            return EXIT_CONFIG
    store = consumer = None
    try:
        settings = load_consumer_settings(
            bootstrap_servers=args.bootstrap_servers, topic=args.topic, dlq_topic=args.dlq_topic,
            group_id=args.group_id, db_backend=args.db_backend, sqlite_path=args.sqlite_path,
            parquet_dir=args.parquet_dir, parquet_enabled=False if args.no_parquet else None)
        logging.getLogger("dco").setLevel(settings.log_level.upper())
        store = open_store(settings)
        store.check_initialized()
        consumer = TelemetryConsumer(settings, store, build_exporter(settings, store),
                                     consumer_factory, dlq_producer_factory)
        consumer.check_topics()
    except Exception as exc:   # settings, uninitialized/unreachable store, broker, missing topics
        log.error("consumer could not start", extra={"context": {"error": f"{type(exc).__name__}: {exc}"}})
        if consumer is not None:
            consumer.consumer.close()
        if store is not None:
            store.close()
        return EXIT_CONFIG

    log.info("consumer starting", extra={"context": {
        "topic": settings.topic, "dlq_topic": settings.dlq_topic, "group_id": settings.group_id,
        "bootstrap_servers": settings.bootstrap_servers, "store": store.describe(),
        "parquet_dir": str(settings.resolve_path(settings.parquet_dir)) if settings.parquet_enabled else None,
        "batch_max_messages": settings.batch_max_messages, "batch_max_wait_s": settings.batch_max_wait_s}})
    _install_signal_handlers(consumer)
    try:
        return consumer.run(args.max_messages, args.max_batches, args.idle_timeout_s)
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
