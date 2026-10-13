"""Telemetry store tests. Every test runs on SQLite; the [postgres] variants run only when
TELEMETRY_TEST_POSTGRES_URL is set (otherwise they are reported as skipped)."""

import copy
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from telemetry_helpers import batch_record, make_events, prepare
from data_engineering.simulation.generate_telemetry import METRICS
from data_engineering.storage.base import StoreNotInitialized


@pytest.fixture
def events(loaded_conn, sim_config):
    return make_events(loaded_conn, sim_config, assets=3, minutes=120, missing_rate=0.05)


def _persist(store, events):
    prepared = prepare(events)
    return store.persist_batch(batch_record(len(prepared)), prepared)


def test_initialize_is_repeatable_and_versioned(store):
    store.initialize()
    store.initialize()
    assert store.schema_version() == 1
    store.check_initialized()
    assert set(store.counts()) == {"assets", "telemetry_readings", "anomaly_records",
                                   "ingestion_batches", "parquet_exports"}


def test_sqlite_uses_wal_and_foreign_keys(sqlite_store):
    assert sqlite_store.conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert sqlite_store.conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_uninitialized_database_is_refused(tmp_path):
    from data_engineering.storage.sqlite_store import SqliteTelemetryStore
    store = SqliteTelemetryStore(tmp_path / "empty.db")
    with pytest.raises(StoreNotInitialized):
        store.check_initialized()
    store.close()


def test_batch_is_persisted_with_counts(store, events):
    result = _persist(store, events)
    labeled = sum(len(e["labels"]["injected_anomalies"]) for e in events)
    assert (result.inserted, result.deduplicated, result.conflicting) == (len(events), 0, 0)
    assert result.anomaly_rows == labeled > 0
    counts = store.counts()
    assert counts["telemetry_readings"] == len(events)
    assert counts["assets"] == 3
    assert counts["anomaly_records"] == labeled         # only labeled events, not every event
    assert counts["ingestion_batches"] == 1


def test_missing_measurements_are_stored_as_null(store, events):
    _persist(store, events)
    missing = [e for e in events if e["reading_status"] == "missing"]
    assert missing
    e = missing[0]
    ts = datetime.strptime(e["event_timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    row = store.readings_between(e["asset_id"], ts, ts + timedelta(seconds=1))[0]
    assert row["reading_status"] == "missing"
    assert all(row[m] is None for m in METRICS)
    assert row["event_timestamp_raw"] == e["event_timestamp"]


def test_redelivery_is_idempotent(store, events):
    _persist(store, events)
    again = _persist(store, events)
    assert (again.inserted, again.deduplicated, again.conflicting) == (0, len(events), 0)
    assert store.counts()["telemetry_readings"] == len(events)
    assert store.counts()["anomaly_records"] == sum(len(e["labels"]["injected_anomalies"]) for e in events)


def test_duplicates_inside_one_batch(store, events):
    result = _persist(store, events[:10] + events[:10])
    assert (result.inserted, result.deduplicated) == (10, 10)


def test_replay_from_stored_run_is_a_duplicate_not_a_conflict(store, events):
    _persist(store, events[:5])
    replayed = copy.deepcopy(events[:5])
    for e in replayed:
        e["provenance"]["published_from"] = "stored_run"
    result = _persist(store, replayed)
    assert (result.deduplicated, result.conflicting) == (5, 0)


def test_conflicting_duplicate_keeps_the_first_version(store, events):
    observed = next(e for e in events if e["reading_status"] == "observed")
    _persist(store, [observed])
    changed = copy.deepcopy(observed)
    changed["metrics"]["cpu_utilization"] = observed["metrics"]["cpu_utilization"] + 10
    result = _persist(store, [changed])
    assert (result.inserted, result.conflicting, result.conflict_event_ids) == (0, 1, [observed["event_id"]])
    ts = datetime.strptime(observed["event_timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    row = store.readings_between(observed["asset_id"], ts, ts + timedelta(seconds=1))[0]
    assert row["cpu_utilization"] == observed["metrics"]["cpu_utilization"]


def test_failed_batch_rolls_back_completely(store, events):
    prepared = prepare(events[:20])
    bad = prepared[-1]
    bad.reading["reading_status"] = "missing"          # CHECK: missing readings must have NULL metrics
    bad.reading["cpu_utilization"] = 1.0
    with pytest.raises(Exception):
        store.persist_batch(batch_record(20), prepared)
    assert store.counts() == {"assets": 0, "telemetry_readings": 0, "anomaly_records": 0,
                              "ingestion_batches": 0, "parquet_exports": 0}
    _persist(store, events[:20])                        # store still usable afterwards
    assert store.counts()["telemetry_readings"] == 20


def test_asset_metadata_tracks_first_and_last_and_ignores_older_updates(store, events):
    asset = events[0]["asset_id"]
    mine = [e for e in events if e["asset_id"] == asset]
    _persist(store, mine[50:60])
    older = copy.deepcopy(mine[:10])
    for e in older:
        e["asset_attributes"]["cooling_type"] = "CHANGED-BY-OLD-EVENT"
    _persist(store, older)
    _persist(store, mine[100:110])
    row = store._fetchall("SELECT * FROM assets WHERE asset_id = ?", (asset,))[0]
    first = datetime.strptime(mine[0]["event_timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    last = datetime.strptime(mine[109]["event_timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    assert (row["first_event_timestamp"], row["last_event_timestamp"]) == (first, last)
    assert row["cooling_type"] == mine[0]["asset_attributes"]["cooling_type"]
    assert row["asset_type"] == mine[0]["asset_attributes"]["server_type"]
    assert row["updated_at"] >= row["created_at"]


def test_time_range_query_is_start_inclusive_end_exclusive(store, events):
    _persist(store, events)
    asset = events[0]["asset_id"]
    start = datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc)
    rows = store.readings_between(asset, start, start + timedelta(minutes=5))
    assert [r["slot_index"] for r in rows] == [10, 11, 12, 13, 14]
    assert all(r["asset_id"] == asset for r in rows)
    assert store.readings_between(asset, start, start + timedelta(minutes=5), limit=2)[1]["slot_index"] == 11


def test_mark_batch_records_commit_status(store, events):
    prepared = prepare(events[:3])
    batch = batch_record(3)
    store.persist_batch(batch, prepared)
    assert store.batch(batch.batch_id)["status"] == "persisted"
    store.mark_batch(batch.batch_id, "committed", dlq_published=2)
    row = store.batch(batch.batch_id)
    assert (row["status"], row["dlq_published"]) == ("committed", 2) and row["committed_at"] is not None


def test_sqlite_transient_error_classification(sqlite_store):
    assert sqlite_store.is_transient(sqlite3.OperationalError("database is locked"))
    assert not sqlite_store.is_transient(sqlite3.OperationalError("no such table: x"))
    assert not sqlite_store.is_transient(sqlite3.IntegrityError("CHECK constraint failed"))
