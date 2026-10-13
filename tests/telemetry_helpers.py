"""Helpers shared by the consumer / storage / Parquet / DuckDB tests.

Events come from the real producer path (TelemetrySimulator -> build_event) seeded with
the synthetic pytest fixture dataset, so the tests exercise the actual message format.
"""

import copy
import uuid
from datetime import datetime, timezone

from kafka_fakes import FakeKafkaMessage
from data_engineering.storage.base import BatchRecord, KafkaPosition, prepare_event
from data_engineering.streaming.event_validation import ValidEvent, parse_message
from data_engineering.streaming.events import serialize_event
from data_engineering.streaming.sources import build_simulator, simulated_events

TOPIC = "datacenter.telemetry"


def make_events(conn, sim_config, *, assets=3, minutes=120, missing_rate=0.05, start=None, seed=None) -> list[dict]:
    config = copy.deepcopy(sim_config)
    config["simulation"].update(asset_count=assets, duration_minutes=minutes, missing_rate=missing_rate)
    if start is not None:
        config["simulation"]["start_ts_utc"] = start
    if seed is not None:
        config["simulation"]["seed"] = seed
    return list(simulated_events(build_simulator(conn, config), "dc-test"))


def to_messages(events, topic=TOPIC, partitions=3, offsets=None) -> list[FakeKafkaMessage]:
    """Assign each asset to a fixed partition (like key hashing) with increasing offsets."""
    offsets = {} if offsets is None else offsets
    assets = sorted({e["asset_id"] for e in events})
    out = []
    for e in events:
        partition = assets.index(e["asset_id"]) % partitions
        offset = offsets.get(partition, 0)
        offsets[partition] = offset + 1
        out.append(FakeKafkaMessage(topic, partition, offset, serialize_event(e), e["asset_id"].encode()))
    return out


def prepare(events, topic=TOPIC) -> list:
    prepared = []
    for offset, event in enumerate(events):
        valid = parse_message(serialize_event(event))
        assert isinstance(valid, ValidEvent), valid
        prepared.append(prepare_event(valid, KafkaPosition(topic, 0, offset)))
    return prepared


def batch_record(messages=0, rejected=0) -> BatchRecord:
    return BatchRecord(uuid.uuid4().hex, "test-group", TOPIC, datetime.now(timezone.utc),
                       messages, rejected, {0: [0, max(0, messages - 1)]})
