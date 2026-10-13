import sqlite3

import pyarrow.parquet as pq
import pytest

from telemetry_helpers import batch_record, make_events, prepare
from data_engineering.simulation.generate_telemetry import METRICS
from data_engineering.storage import parquet_sink
from data_engineering.storage.parquet_sink import ParquetExporter, file_schema, safe_partition_value


@pytest.fixture
def events(loaded_conn, sim_config):
    return make_events(loaded_conn, sim_config, assets=3, minutes=120, missing_rate=0.05)


def _persist(store, events):
    prepared = prepare(events)
    return store.persist_batch(batch_record(len(prepared)), prepared)


def _all_rows(root):
    files = sorted(root.rglob("*.parquet"))
    return files, [r for f in files for r in pq.read_table(f).to_pylist()]


def _exporter(store, root, **kw):
    kw.setdefault("flush_rows", 10_000)
    kw.setdefault("flush_interval_s", 3600)
    return ParquetExporter(store, root, **kw)


def test_export_writes_partitioned_files_with_every_row(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    _persist(sqlite_store, events)
    exporter = _exporter(sqlite_store, root)
    exporter.maybe_export(force=True)
    files, rows = _all_rows(root)
    assert len(rows) == len(events) and exporter.stats.rows_exported == len(events)
    assert all(f.relative_to(root).parts[:2] == ("region_code=NA", "event_date=2026-01-01") for f in files)
    assert {r["event_id"] for r in rows} == {e["event_id"] for e in events}
    assert sqlite_store.pending_exports() == [] and sqlite_store.unexported_summary()["rows"] == 0


def test_schema_is_explicit_and_identical_across_exports(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    exporter = _exporter(sqlite_store, root)
    _persist(sqlite_store, events[:100])
    exporter.maybe_export(force=True)
    _persist(sqlite_store, events[100:])
    exporter.maybe_export(force=True)
    files, _ = _all_rows(root)
    assert len(files) == 2
    expected = file_schema("region_date")
    for f in files:
        assert pq.read_schema(f).equals(expected, check_metadata=False)
    assert "region_code" not in pq.read_schema(files[0]).names        # lives in the directory name


def test_missing_readings_and_anomaly_labels_survive_export(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    _persist(sqlite_store, events)
    _exporter(sqlite_store, root).maybe_export(force=True)
    by_id = {r["event_id"]: r for r in _all_rows(root)[1]}
    for e in events:
        row = by_id[e["event_id"]]
        for m in METRICS:
            assert row[m] == e["metrics"][m]            # None stays None (never 0)
        assert [a["anomaly_id"] for a in row["injected_anomalies"]] == \
               [a["anomaly_id"] for a in e["labels"]["injected_anomalies"]]
        assert row["event_timestamp_raw"] == e["event_timestamp"]


def test_events_across_midnight_land_in_two_date_partitions(sqlite_store, loaded_conn, sim_config, tmp_path):
    events = make_events(loaded_conn, sim_config, assets=2, minutes=60, start="2026-01-01T23:30:00Z")
    _persist(sqlite_store, events)
    root = tmp_path / "telemetry"
    _exporter(sqlite_store, root).maybe_export(force=True)
    dates = {f.parent.name for f in root.rglob("*.parquet")}
    assert dates == {"event_date=2026-01-01", "event_date=2026-01-02"}
    assert len(_all_rows(root)[1]) == len(events)


def test_date_only_partitioning_keeps_region_in_the_file(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    _persist(sqlite_store, events)
    _exporter(sqlite_store, root, partitioning="date").maybe_export(force=True)
    files, rows = _all_rows(root)
    assert files[0].relative_to(root).parts[0] == "event_date=2026-01-01"
    assert {r["region_code"] for r in rows} == {"NA"}


def test_thresholds_avoid_tiny_files(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    exporter = _exporter(sqlite_store, root, flush_rows=300)      # 360 events in total
    _persist(sqlite_store, events[:100])
    exporter.maybe_export()
    assert not list(root.rglob("*.parquet"))           # below threshold and not old: keep buffering
    _persist(sqlite_store, events[100:])
    exporter.maybe_export()
    assert list(root.rglob("*.parquet"))


def test_large_partitions_are_split_into_bounded_files(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    _persist(sqlite_store, events)
    _exporter(sqlite_store, root, max_rows_per_file=100).maybe_export(force=True)
    files, rows = _all_rows(root)
    assert len(files) == -(-len(events) // 100) and len(rows) == len(events)
    assert all(pq.read_metadata(f).num_rows <= 100 for f in files)


def test_crash_before_export_is_recovered_on_restart(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    _persist(sqlite_store, events)
    plan = sqlite_store.claim_export(10_000)            # "crash" right after claiming
    assert [p.export_id for p in sqlite_store.pending_exports()] == [plan.export_id]
    restarted = _exporter(sqlite_store, root)
    assert restarted.recover() is True
    assert len(_all_rows(root)[1]) == len(events)
    assert sqlite_store.pending_exports() == []


def test_failed_write_is_recorded_retried_and_never_duplicated(sqlite_store, events, tmp_path, monkeypatch):
    root = tmp_path / "telemetry"
    _persist(sqlite_store, events)
    real = parquet_sink.write_export
    calls = {"n": 0}

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("disk full (simulated)")
        return real(*args, **kwargs)
    monkeypatch.setattr(parquet_sink, "write_export", flaky)
    now = [0.0]
    exporter = _exporter(sqlite_store, root, clock=lambda: now[0], retry_backoff_s=5)
    exporter.maybe_export(force=True)
    assert exporter.stats.export_failures == 1 and not list(root.rglob("*.parquet"))
    pending = sqlite_store._fetchall("SELECT status, attempts, last_error FROM parquet_exports")
    assert pending[0]["status"] == "writing" and "disk full" in pending[0]["last_error"]
    exporter.maybe_export(force=True)                   # within backoff: not retried yet
    assert calls["n"] == 1
    now[0] = 10.0
    exporter.maybe_export(force=True)
    assert len(_all_rows(root)[1]) == len(events)


def test_rerunning_a_finished_export_overwrites_instead_of_duplicating(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    _persist(sqlite_store, events)
    plan = sqlite_store.claim_export(10_000)
    rows, anomalies = sqlite_store.export_rows(plan.export_id)
    for _ in range(2):    # e.g. crash after writing files but before complete_export, then redo
        parquet_sink.write_export(rows, anomalies, plan.export_id, root, "region_date", 250_000, "zstd")
    files, out = _all_rows(root)
    assert len(out) == len(events) and len({r["event_id"] for r in out}) == len(events)


def test_redelivered_duplicates_are_not_exported_again(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    exporter = _exporter(sqlite_store, root)
    _persist(sqlite_store, events)
    exporter.maybe_export(force=True)
    _persist(sqlite_store, events)                      # Kafka redelivery: 0 new rows
    exporter.maybe_export(force=True)
    assert len(_all_rows(root)[1]) == len(events)


def test_leftover_tmp_files_are_removed_and_never_read(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    _persist(sqlite_store, events)
    plan = sqlite_store.claim_export(10_000)
    leftover = root / "region_code=NA" / "event_date=2026-01-01" / f"part-{plan.export_id}-0000.parquet.tmp"
    leftover.parent.mkdir(parents=True)
    leftover.write_bytes(b"half-written")
    _exporter(sqlite_store, root).recover()
    assert not leftover.exists()
    assert len(_all_rows(root)[1]) == len(events)


def test_shutdown_flush_exports_everything_pending(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    exporter = _exporter(sqlite_store, root, flush_rows=10**9)
    _persist(sqlite_store, events)
    exporter.maybe_export()
    assert not list(root.rglob("*.parquet"))
    exporter.flush_all()
    assert len(_all_rows(root)[1]) == len(events)
    assert exporter.backlog() == {"unassigned_rows": 0, "unassigned_batches": 0,
                                  "exports_writing": 0, "rows_in_writing_exports": 0}


@pytest.mark.parametrize("value, expected", [
    ("NA", "NA"), ("N A", "N_A"), ("../../etc", ".._.._etc"), ("..", "unknown"), ("", "unknown"),
    (None, "unknown"), ("a/b\\c:d", "a_b_c_d"), ("2026-01-01", "2026-01-01"),
])
def test_partition_values_are_sanitized(value, expected):
    assert safe_partition_value(value) == expected
    assert "/" not in safe_partition_value(value) and "\\" not in safe_partition_value(value)


def test_ledger_outage_postpones_export_instead_of_raising(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    _persist(sqlite_store, events)
    now = [0.0]
    exporter = _exporter(sqlite_store, root, clock=lambda: now[0], retry_backoff_s=5)
    real = sqlite_store.pending_exports

    def down():
        raise sqlite3.OperationalError("disk I/O error")
    sqlite_store.pending_exports = down
    exporter.maybe_export(force=True)                   # must not raise
    assert exporter.stats.export_failures == 1 and not list(root.rglob("*.parquet"))
    sqlite_store.pending_exports = real
    now[0] = 10.0
    exporter.maybe_export(force=True)
    assert len(_all_rows(root)[1]) == len(events)
