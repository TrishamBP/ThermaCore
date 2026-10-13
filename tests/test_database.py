import sqlite3

import pytest

from data_engineering.db import init_db
from data_engineering.simulation.generate_telemetry import METRICS, READING_COLUMNS, run_simulation

EXPECTED_TABLES = {"source_files", "source_records", "region_filtered_records", "simulation_runs",
                   "asset_metadata", "telemetry_readings", "anomaly_events", "anomaly_labels"}


def _tables(conn):
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def test_schema_creates_all_tables_and_is_rerunnable(conn):
    assert EXPECTED_TABLES <= _tables(conn)
    init_db(conn)
    assert EXPECTED_TABLES <= _tables(conn)
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


@pytest.fixture
def simulated(loaded_conn, sim_config, monkeypatch, tmp_path):
    monkeypatch.setattr("data_engineering.simulation.generate_telemetry.SIMULATION_DIR", tmp_path / "sim")
    result = run_simulation(loaded_conn, sim_config)
    return loaded_conn, result


def _reading(result, **overrides):
    """A copy of the first observed (non-missing) reading, with overrides applied."""
    observed = next(r for r in result.readings if r[READING_COLUMNS.index("is_missing")] == 0)
    row = dict(zip(READING_COLUMNS, observed))
    row.update(overrides)
    return row


def _insert(conn, row):
    cols = list(row)
    conn.execute(f"INSERT INTO telemetry_readings ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                 [row[c] for c in cols])


def test_duplicate_asset_timestamp_is_rejected(simulated):
    conn, result = simulated
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, _reading(result, slot_index=99999))


def test_missing_slot_cannot_carry_values(simulated):
    conn, result = simulated
    row = _reading(result, ts_utc="2030-01-01T00:00:00Z", slot_index=50000, is_missing=1)
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, row)


def test_observed_slot_cannot_have_null_metric(simulated):
    conn, result = simulated
    row = _reading(result, ts_utc="2030-01-01T00:00:00Z", slot_index=50000, is_missing=0, pue=None)
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, row)


def test_telemetry_must_be_labeled_synthetic(simulated):
    conn, result = simulated
    row = _reading(result, ts_utc="2030-01-01T00:00:00Z", slot_index=50000, provenance="dataset")
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, row)


def test_timestamp_format_is_enforced(simulated):
    conn, result = simulated
    row = _reading(result, ts_utc="2030-01-01 00:00:00", slot_index=50000)
    with pytest.raises(sqlite3.IntegrityError):
        _insert(conn, row)


def test_missing_slots_are_stored_as_null_not_zero(simulated):
    conn, result = simulated
    rows = conn.execute("SELECT * FROM telemetry_readings WHERE is_missing = 1").fetchall()
    assert rows, "fixture config should produce missing slots"
    assert all(row[m] is None for row in rows for m in METRICS)


def test_rerunning_simulation_replaces_run_without_duplicates(simulated, sim_config):
    conn, result = simulated
    run_simulation(conn, sim_config, export=False)
    assert conn.execute("SELECT COUNT(*) FROM simulation_runs").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM telemetry_readings").fetchone()[0] == len(result.readings)
    assert conn.execute("SELECT COUNT(*) FROM anomaly_labels").fetchone()[0] == len(result.labels)


def test_deleting_run_cascades(simulated):
    conn, result = simulated
    with conn:
        conn.execute("DELETE FROM simulation_runs WHERE run_id = ?", (result.run["run_id"],))
    for table in ("asset_metadata", "telemetry_readings", "anomaly_events", "anomaly_labels"):
        assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0, table
