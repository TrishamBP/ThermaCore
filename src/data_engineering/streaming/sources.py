"""Event sources for the producer. Both yield events one at a time (no full materialization).

- simulated_events: generate with the Phase 1 TelemetrySimulator while publishing (Mode B).
- stored_run_events: stream a simulation run already stored in SQLite (Mode A).

The source Kaggle rows are not published directly: they have no timestamps and no asset
identifiers, so turning them into telemetry would mean inventing both. They are used as the
simulation baseline instead (see docs/dataset/README.md).
"""

import json
import sqlite3
from typing import Iterator

from data_engineering.config import region_filter_name
from data_engineering.simulation.generate_telemetry import (
    GENERATOR_NAME, GENERATOR_VERSION, METRICS, SimulationConfigError, TelemetrySimulator,
    load_seed_rows, source_checksum)
from data_engineering.streaming.events import build_event


class SourceError(RuntimeError):
    pass


def build_simulator(conn: sqlite3.Connection, config: dict) -> TelemetrySimulator:
    """TelemetrySimulator seeded from the region-filtered source rows in SQLite."""
    try:
        seed_rows = load_seed_rows(conn, region_filter_name(config))
        checksum = source_checksum(conn)
    except SimulationConfigError as exc:
        raise SourceError(f"{exc}. Run `uv run python -m data_engineering.pipeline` first.") from exc
    return TelemetrySimulator(config, seed_rows, seed_rows[0]["source_file"], checksum)


def simulated_events(simulator: TelemetrySimulator, data_center_id: str,
                     continuous: bool = False) -> Iterator[dict]:
    run_info = {"source_file": simulator.source_file, "source_sha256": simulator.source_sha256,
                "generator_name": GENERATOR_NAME, "generator_version": GENERATOR_VERSION,
                "seed": simulator.seed}
    scenario = simulator.config["scenario"]
    for reading in simulator.iter_slots(None if continuous else simulator.n_slots):
        anomalies = [{"anomaly_id": a["anomaly_id"], "anomaly_type": a["anomaly_type"],
                      "metrics": json.loads(a["metrics_json"])} for a in reading.anomalies]
        yield build_event(run_id=simulator.run_id, asset=reading.asset, slot_index=reading.slot_index,
                          ts_utc=reading.ts_utc, is_missing=reading.is_missing, values=reading.values,
                          anomalies=anomalies, scenario=scenario, data_center_id=data_center_id,
                          run_info=run_info, published_from="simulator")


def latest_run_id(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT run_id FROM simulation_runs ORDER BY created_at DESC LIMIT 1").fetchone()
    if row is None:
        raise SourceError("no simulation run in SQLite. Run `uv run python -m data_engineering.pipeline` first.")
    return row["run_id"]


def stored_run_size(conn: sqlite3.Connection, run_id: str) -> int:
    return conn.execute("SELECT COUNT(*) FROM telemetry_readings WHERE run_id = ?", (run_id,)).fetchone()[0]


def stored_run_events(conn: sqlite3.Connection, run_id: str, data_center_id: str) -> Iterator[dict]:
    """Stream telemetry_readings of one run in time order (slot, then asset)."""
    run = conn.execute("SELECT * FROM simulation_runs WHERE run_id = ?", (run_id,)).fetchone()
    if run is None:
        raise SourceError(f"simulation run {run_id!r} not found in SQLite")
    run = dict(run)
    scenario = json.loads(run["config_json"])["scenario"]
    run_info = {k: run[k] for k in ("source_file", "source_sha256", "generator_name", "generator_version", "seed")}
    assets = {r["asset_id"]: dict(r) for r in conn.execute(
        "SELECT * FROM asset_metadata WHERE run_id = ?", (run_id,))}

    # Labels are small (one row per injected cell); index them by reading.
    events = {r["anomaly_id"]: dict(r) for r in conn.execute(
        "SELECT * FROM anomaly_events WHERE run_id = ? ORDER BY anomaly_id", (run_id,))}
    active: dict[tuple[str, str], list[str]] = {}
    for r in conn.execute("SELECT DISTINCT anomaly_id, asset_id, ts_utc FROM anomaly_labels "
                          "WHERE run_id = ? ORDER BY anomaly_id", (run_id,)):
        active.setdefault((r["asset_id"], r["ts_utc"]), []).append(r["anomaly_id"])

    cursor = conn.execute(
        f"SELECT asset_id, ts_utc, slot_index, is_missing, {', '.join(METRICS)} FROM telemetry_readings "
        "WHERE run_id = ? ORDER BY slot_index, asset_id", (run_id,))
    for row in cursor:  # sqlite3 cursors fetch incrementally
        anomalies = [{"anomaly_id": a, "anomaly_type": events[a]["anomaly_type"],
                      "metrics": json.loads(events[a]["metrics_json"])}
                     for a in active.get((row["asset_id"], row["ts_utc"]), [])]
        yield build_event(run_id=run_id, asset=assets[row["asset_id"]], slot_index=row["slot_index"],
                          ts_utc=row["ts_utc"], is_missing=bool(row["is_missing"]),
                          values={m: row[m] for m in METRICS}, anomalies=anomalies, scenario=scenario,
                          data_center_id=data_center_id, run_info=run_info, published_from="stored_run")
