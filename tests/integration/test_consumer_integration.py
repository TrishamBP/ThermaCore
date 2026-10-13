"""End-to-end against the local broker: producer -> Kafka -> consumer -> SQLite + DLQ + Parquet -> DuckDB.

Run with:  uv run pytest -m integration
Skipped when no broker answers on KAFKA_BOOTSTRAP_SERVERS (default 127.0.0.1:9092).
Uses throwaway topics and consumer groups, deleted afterwards.
"""

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from telemetry_helpers import make_events
from data_engineering.analytics.duckdb_queries import TelemetryAnalytics
from data_engineering.storage.parquet_sink import ParquetExporter
from data_engineering.storage.sqlite_store import SqliteTelemetryStore
from data_engineering.streaming.consumer import EXIT_OK, TelemetryConsumer
from data_engineering.streaming.producer import TelemetryPublisher
from data_engineering.streaming.settings import load_consumer_settings, load_settings
from data_engineering.streaming.topic_setup import ensure_topic

pytestmark = pytest.mark.integration
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")


def _broker_available() -> bool:
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient
    try:
        AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=3)
        return True
    except KafkaException:
        return False


@pytest.fixture
def topics():
    if not _broker_available():
        pytest.skip(f"no Kafka broker at {BOOTSTRAP}; start one with `docker compose up -d kafka`")
    from confluent_kafka.admin import AdminClient
    suffix = uuid.uuid4().hex[:8]
    names = (f"datacenter.telemetry.it-{suffix}", f"datacenter.telemetry.it-{suffix}.dlq")
    settings = load_settings(env={"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP})
    for name in names:
        ensure_topic(settings, topic=name)
    yield names
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    for future in admin.delete_topics(list(names), operation_timeout=10).values():
        future.result()


def _consume(topic, dlq, group, store, parquet_root) -> TelemetryConsumer:
    settings = load_consumer_settings(env={
        "KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP, "KAFKA_TOPIC": topic, "KAFKA_DLQ_TOPIC": dlq,
        "KAFKA_GROUP_ID": group, "CONSUMER_BATCH_MAX_MESSAGES": "50", "CONSUMER_BATCH_MAX_WAIT_S": "1"})
    exporter = ParquetExporter(store, parquet_root, flush_rows=10_000, flush_interval_s=3600)
    consumer = TelemetryConsumer(settings, store, exporter)
    consumer.check_topics()
    assert consumer.run(idle_timeout_s=8) == EXIT_OK
    return consumer


def _read_all(topic, timeout_s=20) -> list:
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"it-read-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out, deadline = [], time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        msg = c.poll(1.0)
        if msg is None:
            if out:
                break
            continue
        if msg.error() is None:
            out.append(msg)
    c.close()
    return out


def test_end_to_end_with_dlq_restart_and_replay(loaded_conn, sim_config, topics, tmp_path):
    from confluent_kafka import Consumer, Producer, TopicPartition
    topic, dlq = topics
    events = make_events(loaded_conn, sim_config, assets=4, minutes=30, missing_rate=0.1)

    # Publish with the real producer, plus two messages that must end up in the DLQ.
    publisher = TelemetryPublisher(load_settings(env={"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP, "KAFKA_TOPIC": topic}))
    assert publisher.publish(events).delivered == 120
    raw = Producer({"bootstrap.servers": BOOTSTRAP})
    raw.produce(topic, key=b"SIM-NA-001", value=b"{not json")
    raw.produce(topic, key=b"SIM-NA-002", value=json.dumps({"schema_version": "9.9"}).encode())
    assert raw.flush(10) == 0

    store = SqliteTelemetryStore(tmp_path / "store.db")
    store.initialize()
    parquet_root = tmp_path / "telemetry"
    group = f"it-sink-{uuid.uuid4().hex[:8]}"

    first = _consume(topic, dlq, group, store, parquet_root)
    assert first.stats.messages_consumed == 122
    assert (first.stats.rows_inserted, first.stats.events_rejected, first.stats.dlq_published) == (120, 2, 2)
    assert store.counts()["telemetry_readings"] == 120
    labeled = sum(len(e["labels"]["injected_anomalies"]) for e in events)
    assert store.counts()["anomaly_records"] == labeled

    # DLQ records really are on the broker, with the source coordinates.
    dlq_records = [json.loads(m.value()) for m in _read_all(dlq)]
    assert sorted(r["failure_reason"] for r in dlq_records) == ["malformed_json", "unsupported_schema_version"]
    assert all(r["source_topic"] == topic and r["source_offset"] >= 0 for r in dlq_records)

    # Committed offsets equal the end of every partition.
    probe = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group})
    meta = probe.list_topics(topic, timeout=10).topics[topic]
    tps = [TopicPartition(topic, p) for p in meta.partitions]
    committed = {tp.partition: tp.offset for tp in probe.committed(tps, timeout=10)}
    ends = {tp.partition: probe.get_watermark_offsets(tp, timeout=10)[1] for tp in tps}
    probe.close()
    assert committed == ends and sum(ends.values()) == 122

    # Parquet history, queried with DuckDB, matches the database.
    analytics = TelemetryAnalytics(parquet_root)
    overview = analytics.overview()
    assert overview["rows"] == overview["distinct_event_ids"] == 120
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    c = analytics.completeness("SIM-NA-001", start, start + timedelta(minutes=30))
    assert c["absent_slots"] == 0 and c["events"] == 30

    # Restart with the same group: nothing new to read, nothing duplicated.
    second = _consume(topic, dlq, group, store, parquet_root)
    assert second.stats.messages_consumed == 0

    # Replay everything with a NEW group: all duplicates, still 120 rows, no extra Parquet rows.
    replay = _consume(topic, dlq, f"it-replay-{uuid.uuid4().hex[:8]}", store, parquet_root)
    assert replay.stats.messages_consumed == 122
    assert (replay.stats.rows_inserted, replay.stats.rows_deduplicated) == (0, 120)
    assert store.counts()["telemetry_readings"] == 120
    assert TelemetryAnalytics(parquet_root).overview()["rows"] == 120
    store.close()
