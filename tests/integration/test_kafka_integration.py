"""Integration test against a real local broker (docker compose up -d kafka).

Run with:  uv run pytest -m integration
Skipped automatically when no broker answers on KAFKA_BOOTSTRAP_SERVERS (default 127.0.0.1:9092).
Uses a throwaway topic and consumer group, deleted afterwards.
"""

import copy
import json
import os
import time
import uuid

import pytest

from data_engineering.streaming.producer import TelemetryPublisher
from data_engineering.streaming.settings import load_settings
from data_engineering.streaming.sources import build_simulator, simulated_events
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
def topic():
    if not _broker_available():
        pytest.skip(f"no Kafka broker at {BOOTSTRAP}; start one with `docker compose up -d kafka`")
    from confluent_kafka.admin import AdminClient
    name = f"datacenter.telemetry.it-{uuid.uuid4().hex[:8]}"
    yield name
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})  # keep a reference until the futures resolve
    futures = admin.delete_topics([name], operation_timeout=10)
    for future in futures.values():
        future.result()  # wait, so the throwaway topic is really gone


def test_publish_and_read_back(loaded_conn, sim_config, topic):
    from confluent_kafka import Consumer

    settings = load_settings(env={"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP, "KAFKA_TOPIC": topic})
    assert ensure_topic(settings)["created"] is True
    assert ensure_topic(settings)["created"] is False            # idempotent setup

    config = copy.deepcopy(sim_config)
    config["simulation"].update(asset_count=4, duration_minutes=15)
    events = list(simulated_events(build_simulator(loaded_conn, config), "dc-test"))

    publisher = TelemetryPublisher(settings)
    assert publisher.check_topic() == 3
    stats = publisher.publish(events)
    assert stats.ok and stats.delivered == len(events) == 60

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"it-{uuid.uuid4().hex[:8]}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])
    received, deadline = [], time.monotonic() + 30
    while len(received) < len(events) and time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is not None and msg.error() is None:
            received.append(msg)
    consumer.close()

    assert len(received) == len(events)
    by_key: dict[bytes, list[dict]] = {}
    partitions_by_key: dict[bytes, set[int]] = {}
    for msg in received:
        event = json.loads(msg.value())
        assert msg.key() == event["asset_id"].encode()
        by_key.setdefault(msg.key(), []).append(event)
        partitions_by_key.setdefault(msg.key(), set()).add(msg.partition())
    assert all(len(p) == 1 for p in partitions_by_key.values())            # one partition per asset
    for key_events in by_key.values():                                      # order kept per asset
        assert [e["slot_index"] for e in key_events] == list(range(15))
    assert {e["event_id"] for e in events} == {json.loads(m.value())["event_id"] for m in received}
