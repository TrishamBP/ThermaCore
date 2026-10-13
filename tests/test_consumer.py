"""Consumer tests with a scripted fake Kafka consumer and a fake DLQ producer (no broker).

The store is a real SQLite database, so these tests check what actually gets persisted.
"""

import json
import sqlite3
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest
from confluent_kafka import KafkaError

from kafka_fakes import FakeKafkaMessage, consumer_factory, factory
from telemetry_helpers import TOPIC, make_events, to_messages
from data_engineering.storage.parquet_sink import ParquetExporter
from data_engineering.streaming import consumer as consumer_module
from data_engineering.streaming.consumer import (
    EXIT_CONFIG, EXIT_OK, EXIT_RUNTIME, OffsetTracker, StartupError, TelemetryConsumer, main)
from data_engineering.streaming.settings import load_consumer_settings

DLQ = "datacenter.telemetry.dlq"


@pytest.fixture
def events(loaded_conn, sim_config):
    return make_events(loaded_conn, sim_config, assets=3, minutes=20, missing_rate=0.1)


@pytest.fixture
def settings():
    return load_consumer_settings(env={"CONSUMER_BATCH_MAX_MESSAGES": "100", "CONSUMER_MAX_RETRIES": "3"})


class FlakyStore:
    """Wraps a real store; persist_batch raises the given errors first, then works."""

    def __init__(self, store, errors):
        self.store, self.errors, self.reconnects = store, list(errors), 0

    def persist_batch(self, batch, events):
        if self.errors:
            raise self.errors.pop(0)
        return self.store.persist_batch(batch, events)

    def reconnect(self):
        self.reconnects += 1

    def __getattr__(self, name):
        return getattr(self.store, name)


def _consumer(settings, store, batches, *, exporter=None, dlq_kwargs=None, **consumer_kwargs):
    make_consumer, holder = consumer_factory(batches=batches, **consumer_kwargs)
    make_dlq, dlq_holder = factory(**(dlq_kwargs or {}))
    sleeps = []
    c = TelemetryConsumer(settings, store, exporter, make_consumer, make_dlq,
                          sleep=sleeps.append, rng=lambda: 0.5)
    return c, holder["consumer"], dlq_holder["producer"], sleeps


def _ends(messages):
    out = {}
    for m in messages:
        out[(m.topic(), m.partition())] = max(out.get((m.topic(), m.partition()), 0), m.offset() + 1)
    return out


def _bad(partition, offset, value=b"{not json"):
    return FakeKafkaMessage(TOPIC, partition, offset, value, b"SIM-NA-001")


# ---------- happy path ----------

def test_valid_batch_is_stored_then_committed(settings, sqlite_store, events):
    msgs = to_messages(events)
    c, kafka, _, _ = _consumer(settings, sqlite_store, [msgs[:30], msgs[30:]])
    assert c.run(max_messages=len(msgs)) == EXIT_OK
    assert sqlite_store.counts()["telemetry_readings"] == len(events) == 60
    assert kafka.committed_offsets() == _ends(msgs)          # next offset to read, per partition
    assert len(kafka.commits) == 2 and kafka.closed
    assert c.stats.rows_inserted == 60 and c.stats.events_rejected == 0
    batches = sqlite_store._fetchall("SELECT status FROM ingestion_batches")
    assert [b["status"] for b in batches] == ["committed", "committed"]


def test_consumer_config_disables_auto_commit(settings):
    cfg = settings.consumer_config()
    assert cfg["enable.auto.commit"] is False and cfg["enable.auto.offset.store"] is False
    assert cfg["group.id"] == "dco-telemetry-sink"


def test_group_id_comes_from_environment():
    assert load_consumer_settings(env={"KAFKA_GROUP_ID": "my-sink"}).consumer_config()["group.id"] == "my-sink"


# ---------- no premature commits / database failures ----------

def test_permanent_database_error_commits_nothing(settings, sqlite_store, events):
    store = FlakyStore(sqlite_store, [sqlite3.IntegrityError("simulated permanent failure")])
    c, kafka, _, sleeps = _consumer(settings, store, [to_messages(events)])
    assert c.run(max_messages=60) == EXIT_RUNTIME
    assert kafka.commit_attempts == 0 and sleeps == []        # no retry for permanent errors
    assert sqlite_store.counts()["telemetry_readings"] == 0


def test_transient_database_error_is_retried_with_backoff(settings, sqlite_store, events):
    store = FlakyStore(sqlite_store, [sqlite3.OperationalError("database is locked")] * 2)
    c, kafka, _, sleeps = _consumer(settings, store, [to_messages(events)])
    assert c.run(max_messages=60) == EXIT_OK
    assert c.stats.retries == 2 and store.reconnects == 2
    assert sleeps == [0.375, 0.75]                             # equal jitter: cap/2 + 0.5*cap/2, cap doubles
    assert sqlite_store.counts()["telemetry_readings"] == 60 and kafka.commit_attempts == 1


def test_retry_exhaustion_stops_without_committing(settings, sqlite_store, events):
    store = FlakyStore(sqlite_store, [sqlite3.OperationalError("database is locked")] * 10)
    c, kafka, _, sleeps = _consumer(settings, store, [to_messages(events)])
    assert c.run(max_messages=60) == EXIT_RUNTIME
    assert len(sleeps) == settings.max_retries and kafka.commit_attempts == 0
    assert c.stats.failures == 1


def test_backoff_is_bounded(settings):
    c, *_ = _consumer(settings, None, [])
    assert max(c._backoff(a) for a in range(20)) <= settings.retry_backoff_max_s


# ---------- dead-letter queue ----------

def test_invalid_messages_go_to_the_dlq_and_are_committed(settings, sqlite_store, events):
    msgs = to_messages(events)
    bad = [_bad(0, 1000), _bad(1, 1000, b'{"schema_version": "9.9"}')]
    c, kafka, dlq, _ = _consumer(settings, sqlite_store, [msgs + bad])
    assert c.run(max_messages=len(msgs) + 2) == EXIT_OK
    assert sqlite_store.counts()["telemetry_readings"] == 60
    assert [m.topic() for m in dlq.sent] == [DLQ, DLQ]
    first = json.loads(dlq.sent[0].value())
    assert first["failure_reason"] == "malformed_json"
    assert (first["source_topic"], first["source_partition"], first["source_offset"]) == (TOPIC, 0, 1000)
    assert first["original_payload_utf8"] == "{not json" and first["consumer_group"] == settings.group_id
    assert first["failed_at"].endswith("Z") and dlq.sent[0].key() == b"SIM-NA-001"
    assert json.loads(dlq.sent[1].value())["failure_reason"] == "unsupported_schema_version"
    assert kafka.committed_offsets() == _ends(msgs + bad)      # dead-lettered = handled
    assert c.stats.dlq_published == 2 and c.stats.rejected_by_reason == {
        "malformed_json": 1, "unsupported_schema_version": 1}


def test_non_utf8_payload_is_preserved_as_base64(settings, sqlite_store):
    c, _, dlq, _ = _consumer(settings, sqlite_store, [[_bad(0, 0, b"\xff\xfe binary")]])
    c.run(max_messages=1)
    record = json.loads(dlq.sent[0].value())
    assert record["original_payload_utf8"] is None
    import base64
    assert base64.b64decode(record["original_payload_base64"]) == b"\xff\xfe binary"


def test_dlq_failure_blocks_commits_past_the_invalid_message(settings, sqlite_store, events):
    msgs = to_messages(events)                       # partition 0 holds offsets 0..19 of one asset
    p0 = [m for m in msgs if m.partition() == 0]
    bad = _bad(0, p0[5].offset())                    # replace offset 5 of partition 0 with garbage
    batch = [bad if (m.partition(), m.offset()) == (0, 5) else m for m in msgs]
    c, kafka, dlq, _ = _consumer(settings, sqlite_store, [batch], dlq_kwargs={"fail_all": True})
    assert c.run(max_messages=len(batch)) == EXIT_RUNTIME
    committed = kafka.committed_offsets()
    assert committed[(TOPIC, 0)] == 5                # stops right before the unpublished DLQ message
    assert committed[(TOPIC, 1)] == 20 and committed[(TOPIC, 2)] == 20
    assert sqlite_store.counts()["telemetry_readings"] == 59    # valid events are stored (idempotent on replay)
    assert c.stats.dlq_unconfirmed == 1 and dlq.sent == []
    status = sqlite_store._fetchall("SELECT status FROM ingestion_batches")[0]["status"]
    assert status == "persisted"                     # not marked as fully processed


# ---------- commit failures and redelivery ----------

def test_commit_failure_then_redelivery_is_deduplicated(settings, sqlite_store, events):
    msgs = to_messages(events)
    c, kafka, _, _ = _consumer(settings, sqlite_store, [msgs, msgs],
                               commit_errors=[KafkaError(KafkaError.ILLEGAL_GENERATION)])
    assert c.run(max_messages=2 * len(msgs)) == EXIT_OK
    assert c.stats.commit_failures == 1 and len(kafka.commits) == 1
    assert sqlite_store.counts()["telemetry_readings"] == 60   # not 120
    assert c.stats.rows_inserted == 60 and c.stats.rows_deduplicated == 60
    statuses = [b["status"] for b in sqlite_store._fetchall(
        "SELECT status FROM ingestion_batches ORDER BY persisted_at")]
    assert statuses == ["commit_failed", "committed"]
    assert kafka.committed_offsets() == _ends(msgs)


def test_retriable_commit_error_is_retried(settings, sqlite_store, events):
    msgs = to_messages(events)
    c, kafka, _, sleeps = _consumer(settings, sqlite_store, [msgs],
                                    commit_errors=[KafkaError(KafkaError._TIMED_OUT, retriable=True)])
    assert c.run(max_messages=len(msgs)) == EXIT_OK
    assert c.stats.commit_failures == 1 and len(kafka.commits) == 1 and len(sleeps) == 1


def test_offset_tracker_never_skips_an_unhandled_offset():
    t = OffsetTracker()
    for o in range(5):
        t.add(TOPIC, 0, o)
    for o in (0, 1, 3, 4):
        t.mark_handled(TOPIC, 0, o)
    assert t.committable() == {(TOPIC, 0): 2}
    t.mark_handled(TOPIC, 0, 2)
    assert t.committable() == {(TOPIC, 0): 5}
    t.forget_through((TOPIC, 0), 5)
    assert t.committable() == {} and t.unhandled() == 0


def test_revoked_partitions_are_forgotten(settings, sqlite_store):
    c, *_ = _consumer(settings, sqlite_store, [])
    c.tracker.add(TOPIC, 1, 7)
    c._on_revoke(None, [SimpleNamespace(topic=TOPIC, partition=1)])
    assert c.tracker.committable() == {} and c.tracker.unhandled() == 0


# ---------- shutdown, finite runs, Parquet ----------

def test_graceful_stop_finishes_the_current_batch(settings, sqlite_store, events):
    msgs = to_messages(events)
    holder = {}

    def stop_on_second_poll(_kafka, call):
        if call == 2:
            holder["c"].request_stop()           # what the SIGINT/SIGTERM handler does
    c, kafka, _, _ = _consumer(settings, sqlite_store, [msgs[:30], msgs[30:]], on_consume=stop_on_second_poll)
    holder["c"] = c
    assert c.run() == EXIT_OK
    assert sqlite_store.counts()["telemetry_readings"] == 60   # the batch in hand is completed ...
    assert kafka.committed_offsets() == _ends(msgs)          # ... and committed before closing
    assert kafka.closed and kafka.consume_calls == 2


def test_idle_timeout_ends_a_finite_run(settings, sqlite_store, events, monkeypatch):
    clock = iter(range(0, 1000, 5))
    monkeypatch.setattr(consumer_module.time, "monotonic", lambda: next(clock))
    c, kafka, _, _ = _consumer(settings, sqlite_store, [to_messages(events)])
    assert c.run(idle_timeout_s=10) == EXIT_OK
    assert sqlite_store.counts()["telemetry_readings"] == 60 and kafka.consume_calls >= 2


def test_kafka_error_messages_are_skipped(settings, sqlite_store, events):
    err = FakeKafkaMessage(TOPIC, 0, -1, None, error=KafkaError(KafkaError._TRANSPORT))
    c, _, _, _ = _consumer(settings, sqlite_store, [[err], to_messages(events)])
    assert c.run(max_batches=1) == EXIT_OK
    assert c.stats.kafka_errors == 1 and sqlite_store.counts()["telemetry_readings"] == 60


def test_parquet_is_flushed_on_shutdown(settings, sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    exporter = ParquetExporter(sqlite_store, root, flush_rows=10**6, flush_interval_s=3600)
    c, *_ = _consumer(settings, sqlite_store, [to_messages(events)], exporter=exporter)
    assert c.run(max_messages=60) == EXIT_OK
    rows = sum(pq.read_metadata(f).num_rows for f in root.rglob("*.parquet"))
    assert rows == 60 and exporter.backlog()["unassigned_rows"] == 0


def test_parquet_failure_is_reported_and_kept_for_retry(settings, sqlite_store, events, tmp_path, monkeypatch):
    from data_engineering.storage import parquet_sink

    def broken(*_args, **_kwargs):
        raise OSError("disk full (simulated)")
    monkeypatch.setattr(parquet_sink, "write_export", broken)
    exporter = ParquetExporter(sqlite_store, tmp_path / "telemetry", flush_rows=10)
    c, kafka, _, _ = _consumer(settings, sqlite_store, [to_messages(events)], exporter=exporter)
    assert c.run(max_messages=60) == EXIT_RUNTIME            # pending export at shutdown is an error ...
    assert kafka.committed_offsets()                          # ... but the DB is durable: offsets committed
    assert exporter.stats.export_failures >= 1
    assert exporter.backlog()["rows_in_writing_exports"] == 60


# ---------- startup ----------

def test_missing_topics_are_reported(settings, sqlite_store):
    c, *_ = _consumer(settings, sqlite_store, [], topics=("datacenter.telemetry",))
    with pytest.raises(StartupError, match="topic_setup"):
        c.check_topics()


def test_unreachable_broker_is_reported(settings, sqlite_store):
    c, *_ = _consumer(settings, sqlite_store, [], unreachable=True)
    with pytest.raises(StartupError, match="cannot reach Kafka"):
        c.check_topics()


def test_main_refuses_an_uninitialized_store(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEMETRY_SQLITE_PATH", str(tmp_path / "fresh.db"))
    make_consumer, _ = consumer_factory()
    assert main(["--max-messages", "1"], consumer_factory=make_consumer) == EXIT_CONFIG


def test_main_end_to_end_with_fakes(tmp_path, monkeypatch, events):
    from data_engineering.storage.sqlite_store import SqliteTelemetryStore
    db = tmp_path / "store.db"
    SqliteTelemetryStore(db).initialize()
    monkeypatch.setenv("TELEMETRY_SQLITE_PATH", str(db))
    monkeypatch.setenv("PARQUET_OUTPUT_DIR", str(tmp_path / "pq"))
    monkeypatch.setattr(consumer_module, "_install_signal_handlers", lambda c: None)
    make_consumer, holder = consumer_factory(batches=[to_messages(events)])
    make_dlq, _ = factory()
    assert main(["--max-messages", "60"], consumer_factory=make_consumer, dlq_producer_factory=make_dlq) == EXIT_OK
    store = SqliteTelemetryStore(db)
    assert store.counts()["telemetry_readings"] == 60
    store.close()
    assert sum(pq.read_metadata(f).num_rows for f in (tmp_path / "pq").rglob("*.parquet")) == 60


def test_shutdown_always_closes_the_consumer_even_if_the_store_is_down(settings, sqlite_store, events, tmp_path):
    # Regression: a failing backlog query at shutdown used to skip close(), so the broker kept the
    # dead member until the session timeout and a restarted consumer got no partitions.
    store = FlakyStore(sqlite_store, [sqlite3.IntegrityError("permanent")])
    exporter = ParquetExporter(store, tmp_path / "pq")

    def broken_backlog():
        raise sqlite3.OperationalError("database is locked")
    exporter.backlog = broken_backlog
    c, kafka, _, _ = _consumer(settings, store, [to_messages(events)], exporter=exporter)
    assert c.run(max_messages=60) == EXIT_RUNTIME
    assert kafka.closed


def test_idle_timer_waits_for_partition_assignment(settings, sqlite_store, events, monkeypatch):
    clock = iter(range(0, 10_000, 5))
    monkeypatch.setattr(consumer_module.time, "monotonic", lambda: next(clock))
    # Partitions arrive only on the 6th poll (slow group join); messages follow on the 7th.
    c, kafka, _, _ = _consumer(settings, sqlite_store, [[]] * 6 + [to_messages(events)], assign_on_call=6)
    assert c.run(idle_timeout_s=10) == EXIT_OK
    assert sqlite_store.counts()["telemetry_readings"] == 60


def test_idle_run_without_assignment_eventually_gives_up(settings, sqlite_store, monkeypatch):
    clock = iter(range(0, 10_000, 5))
    monkeypatch.setattr(consumer_module.time, "monotonic", lambda: next(clock))
    c, kafka, _, _ = _consumer(settings, sqlite_store, [], assign_on_call=None)
    assert c.run(idle_timeout_s=10) == EXIT_OK
    assert kafka.closed and kafka.consume_calls >= 10


def test_unexpected_errors_stop_cleanly_without_committing(settings, sqlite_store, events):
    class Broken(FlakyStore):
        def persist_batch(self, batch, evs):
            raise ZeroDivisionError("bug")

        def is_transient(self, exc):
            raise RuntimeError("classification bug too")
    c, kafka, _, _ = _consumer(settings, Broken(sqlite_store, []), [to_messages(events)])
    assert c.run(max_messages=60) == EXIT_RUNTIME
    assert kafka.commit_attempts == 0 and kafka.closed
