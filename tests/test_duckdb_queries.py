from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest

from telemetry_helpers import batch_record, make_events, prepare
from data_engineering.analytics.duckdb_queries import TelemetryAnalytics
from data_engineering.storage.parquet_sink import ParquetExporter

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _ts(event) -> datetime:
    return datetime.strptime(event["event_timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


@pytest.fixture
def events(loaded_conn, sim_config):
    return make_events(loaded_conn, sim_config, assets=3, minutes=180, missing_rate=0.1)


@pytest.fixture
def analytics(sqlite_store, events, tmp_path):
    root = tmp_path / "telemetry"
    for i in range(0, len(events), 200):            # several batches and exports
        chunk = prepare(events[i:i + 200])
        sqlite_store.persist_batch(batch_record(len(chunk)), chunk)
        ParquetExporter(sqlite_store, root, flush_rows=300, flush_interval_s=3600).maybe_export(force=True)
    return TelemetryAnalytics(root)


def test_overview_matches_events(analytics, events):
    o = analytics.overview()
    assert o["rows"] == o["distinct_event_ids"] == len(events)
    assert o["assets"] == 3 and o["files"] >= 2
    assert o["first_event"] == T0 and o["last_event"] == T0 + timedelta(minutes=179)
    assert o["missing_readings"] == sum(e["reading_status"] == "missing" for e in events)


def test_readings_window_is_start_inclusive_end_exclusive(analytics, events):
    rows = analytics.readings("SIM-NA-002", T0 + timedelta(minutes=30), T0 + timedelta(minutes=40),
                              ["cpu_utilization", "pue"])
    expected = [e for e in events if e["asset_id"] == "SIM-NA-002" and 30 <= e["slot_index"] < 40]
    assert [r["event_id"] for r in rows] == [e["event_id"] for e in expected]
    assert [r["cpu_utilization"] for r in rows] == [e["metrics"]["cpu_utilization"] for e in expected]
    assert set(rows[0]) == {"event_id", "simulation_run_id", "asset_id", "event_timestamp", "reading_status",
                            "cpu_utilization", "pue"}
    assert rows[0]["event_timestamp"].tzinfo is not None


def test_metric_stats_match_python_and_exclude_nulls(analytics, events):
    start, end = T0 + timedelta(minutes=60), T0 + timedelta(minutes=120)
    stats = {(r["asset_id"], r["metric"]): r
             for r in analytics.metric_stats(["power_consumption_kw", "inlet_temperature_c"], start, end)}
    for asset in ("SIM-NA-001", "SIM-NA-002", "SIM-NA-003"):
        window = [e for e in events if e["asset_id"] == asset and start <= _ts(e) < end]
        for metric in ("power_consumption_kw", "inlet_temperature_c"):
            values = [e["metrics"][metric] for e in window if e["metrics"][metric] is not None]
            row = stats[(asset, metric)]
            assert row["readings"] == len(values)
            assert row["nulls"] == len(window) - len(values)
            assert row["avg"] == pytest.approx(sum(values) / len(values))
            assert (row["min"], row["max"]) == (min(values), max(values))


def test_metric_stats_across_all_assets(analytics, events):
    (row,) = analytics.metric_stats(["cpu_utilization"], per_asset=False)
    values = [e["metrics"]["cpu_utilization"] for e in events if e["metrics"]["cpu_utilization"] is not None]
    assert row["asset_id"] == "all" and row["readings"] == len(values)


def test_event_counts_per_hour(analytics, events):
    rows = analytics.event_counts(bucket="hour", asset_id="SIM-NA-001")
    assert [r["bucket_start"] for r in rows] == [T0, T0 + timedelta(hours=1), T0 + timedelta(hours=2)]
    assert all(r["events"] == 60 and r["observed"] + r["missing"] == 60 for r in rows)
    mine = [e for e in events if e["asset_id"] == "SIM-NA-001"]
    assert sum(r["missing"] for r in rows) == sum(e["reading_status"] == "missing" for e in mine)


def test_anomaly_summary_counts_explicit_labels(analytics, events):
    rows = analytics.anomaly_summary(bucket="day")
    expected = Counter((e["asset_id"], a["anomaly_type"]) for e in events for a in e["labels"]["injected_anomalies"])
    assert expected, "fixture config injects anomalies"
    assert {(r["asset_id"], r["anomaly_type"]): r["labeled_events"] for r in rows} == dict(expected)
    assert all(r["period"] == T0 for r in rows)


def test_completeness_reports_missing_and_absent_slots(sqlite_store, events, tmp_path):
    root = tmp_path / "gappy"
    mine = [e for e in events if e["asset_id"] == "SIM-NA-001"]
    dropped = {10, 11, 12, 50}                         # never stored: absent from history
    kept = [e for e in mine if e["slot_index"] not in dropped]
    sqlite_store.persist_batch(batch_record(len(kept)), prepare(kept))
    ParquetExporter(sqlite_store, root).flush_all()
    c = TelemetryAnalytics(root).completeness("SIM-NA-001", T0, T0 + timedelta(minutes=180))
    assert c["expected_slots"] == 180 and c["events"] == 176 and c["absent_slots"] == 4
    assert c["missing_readings"] == sum(e["reading_status"] == "missing" for e in kept)
    assert c["observed_readings"] + c["missing_readings"] == 176
    assert c["absent_intervals"] == [[T0 + timedelta(minutes=10), T0 + timedelta(minutes=12)],
                                     [T0 + timedelta(minutes=50), T0 + timedelta(minutes=50)]]
    assert c["off_grid_events"] == 0 and c["duplicate_events"] == 0


def test_empty_dataset_returns_empty_results(tmp_path):
    a = TelemetryAnalytics(tmp_path / "nothing-here")
    assert a.overview()["rows"] == 0
    assert a.readings("SIM-NA-001", T0, T0 + timedelta(hours=1)) == []
    assert a.metric_stats() == [] and a.event_counts() == [] and a.anomaly_summary() == []
    c = a.completeness("SIM-NA-001", T0, T0 + timedelta(minutes=5))
    assert c["expected_slots"] == c["absent_slots"] == 5 and c["events"] == 0


def test_inputs_are_validated(analytics):
    with pytest.raises(ValueError, match="unknown metric"):
        analytics.metric_stats(["cpu_utilization; DROP TABLE x"])
    with pytest.raises(ValueError, match="bucket"):
        analytics.event_counts(bucket="week")
    with pytest.raises(ValueError, match="timezone"):
        analytics.readings("SIM-NA-001", datetime(2026, 1, 1), datetime(2026, 1, 2))
    with pytest.raises(ValueError, match="before"):
        analytics.readings("SIM-NA-001", T0, T0)
    assert analytics.readings("x' OR '1'='1", T0, T0 + timedelta(days=1)) == []   # bound parameter


def test_runs_are_kept_apart(sqlite_store, loaded_conn, sim_config, tmp_path):
    """Asset IDs repeat across runs; run_id separates them (otherwise rows are combined)."""
    root = tmp_path / "runs"
    a = make_events(loaded_conn, sim_config, assets=2, minutes=30, missing_rate=0.0)
    b = make_events(loaded_conn, sim_config, assets=2, minutes=30, missing_rate=0.0, seed=7)
    for evs in (a, b):
        sqlite_store.persist_batch(batch_record(len(evs)), prepare(evs))
    ParquetExporter(sqlite_store, root).flush_all()
    analytics = TelemetryAnalytics(root)
    run_a, run_b = a[0]["simulation_run_id"], b[0]["simulation_run_id"]
    assert analytics.overview()["runs"] == {run_a: 60, run_b: 60}
    mixed = analytics.completeness("SIM-NA-001", T0, T0 + timedelta(minutes=30))
    assert mixed["events"] == 60 and mixed["duplicate_events"] == 30      # two runs mixed
    only_a = analytics.completeness("SIM-NA-001", T0, T0 + timedelta(minutes=30), run_id=run_a)
    assert only_a["events"] == 30 and only_a["duplicate_events"] == 0
    rows = analytics.readings("SIM-NA-001", T0, T0 + timedelta(minutes=5), run_id=run_b)
    assert {r["simulation_run_id"] for r in rows} == {run_b} and len(rows) == 5
    assert len(sqlite_store.readings_between("SIM-NA-001", T0, T0 + timedelta(minutes=5),
                                             simulation_run_id=run_a)) == 5
