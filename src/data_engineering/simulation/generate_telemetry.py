"""Generate a clearly labeled SYNTHETIC 24-hour, one-minute telemetry run.

    uv run python -m data_engineering.simulation.generate_telemetry [--config config/simulation.yaml]

Why synthetic: the source dataset has no timestamps and no equipment identifiers, so its
rows are independent records, not a time series. Each simulated asset is seeded from one
North American source row (its static attributes and baseline metric values). Temporal
behaviour, asset IDs, missing slots and anomalies are generated here and labeled as such.

Determinism: the run_id is a hash of the simulation config and source checksum, and each
random component (asset selection, workload regime, per-metric noise, missingness,
anomaly placement) draws from its own seeded stream. The same config + source always
produces identical output; changing e.g. missing_rate does not change the signal.
"""

import argparse
import csv
import hashlib
import json
import math
import random
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from data_engineering.config import load_config, region_filter_name
from data_engineering.db import connect, init_db
from data_engineering.logging_utils import get_logger
from data_engineering.paths import SIMULATION_DIR, SOURCE_CSV

GENERATOR_NAME = "dco-baseline-telemetry"
GENERATOR_VERSION = "1.0.0"
DEFAULT_DRIVER = "workload_intensity"

# Metrics simulated per slot; order matches telemetry_readings columns.
METRICS = (
    "workload_intensity", "cpu_utilization", "gpu_utilization", "memory_utilization",
    "storage_utilization", "disk_io_iops", "network_throughput_gbps", "network_latency_ms",
    "ambient_temperature_c", "humidity_percentage", "inlet_temperature_c",
    "exhaust_temperature_c", "fan_speed_rpm", "airflow_cfm", "power_consumption_kw", "pue",
)
# Static attributes copied from the baseline source row into asset_metadata.
ASSET_ATTRIBUTES = (
    "server_type", "cooling_type", "datacenter_region", "energy_source_primary",
    "maintenance_status", "server_age_years", "rack_u_position", "energy_efficiency_class",
    "failure_risk_level",
)

log = get_logger("generate_telemetry")


class SimulationConfigError(ValueError):
    pass


@dataclass(frozen=True)
class MetricModel:
    driver: str | None
    slope: float
    resid_std: float
    std: float
    lo: float
    hi: float


@dataclass
class SimulationResult:
    run: dict
    assets: list[dict]
    readings: list[tuple]          # columns: READING_COLUMNS
    events: list[dict]
    labels: list[tuple]            # columns: LABEL_COLUMNS
    content_sha256: str
    models: dict = field(repr=False, default_factory=dict)


READING_COLUMNS = ("run_id", "asset_id", "ts_utc", "slot_index", "provenance",
                   "is_missing", "is_anomalous", *METRICS)
LABEL_COLUMNS = ("anomaly_id", "run_id", "asset_id", "ts_utc", "metric",
                 "clean_value", "injected_value")


def parse_utc(ts: str) -> datetime:
    if not ts.endswith("Z"):
        raise SimulationConfigError(f"start_ts_utc must be UTC with trailing 'Z': {ts!r}")
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def format_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generation_order(drivers: dict[str, str]) -> list[str]:
    for metric, driver in drivers.items():
        if metric not in METRICS or driver not in METRICS:
            raise SimulationConfigError(f"unknown metric in drivers: {metric} <- {driver}")
        if metric == DEFAULT_DRIVER:
            raise SimulationConfigError(f"{DEFAULT_DRIVER} is the root driver and cannot have one")
    order = [DEFAULT_DRIVER]
    pending = [m for m in METRICS if m != DEFAULT_DRIVER]
    while pending:
        ready = [m for m in pending if drivers.get(m, DEFAULT_DRIVER) in order]
        if not ready:
            raise SimulationConfigError(f"cyclic drivers among {pending}")
        order.extend(ready)
        pending = [m for m in pending if m not in ready]
    return order


def fit_metric_models(rows: list[dict], drivers: dict[str, str]) -> dict[str, dict[str, MetricModel]]:
    """Per server_type, fit each metric as a linear function of its driver over the seed rows."""
    by_type: dict[str, list[dict]] = {}
    for row in rows:
        by_type.setdefault(row["server_type"], []).append(row)
    models: dict[str, dict[str, MetricModel]] = {}
    for server_type, group in sorted(by_type.items()):
        n = len(group)
        models[server_type] = {}
        for metric in METRICS:
            y = [float(r[metric]) for r in group]
            my = sum(y) / n
            std = math.sqrt(sum((v - my) ** 2 for v in y) / n)
            driver = None if metric == DEFAULT_DRIVER else drivers.get(metric, DEFAULT_DRIVER)
            slope, resid_std = 0.0, std
            if driver is not None:
                x = [float(r[driver]) for r in group]
                mx = sum(x) / n
                var = sum((v - mx) ** 2 for v in x) / n
                if var > 0:
                    slope = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y)) / n / var
                resid_std = math.sqrt(
                    sum((yi - my - slope * (xi - mx)) ** 2 for xi, yi in zip(x, y)) / n)
            models[server_type][metric] = MetricModel(driver, slope, resid_std, std, min(y), max(y))
    return models


def compute_run_id(sim_config: dict, source_sha256: str) -> str:
    payload = json.dumps({"config": sim_config, "source_sha256": source_sha256,
                          "generator": GENERATOR_VERSION}, sort_keys=True)
    return "sim-" + hashlib.sha256(payload.encode()).hexdigest()[:12]


def _clip(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v


class AssetSimulator:
    """Clean (anomaly-free) metric values for one asset, produced one slot at a time.

    Workload = baseline + diurnal curve + smoothed regime offset + AR(1) noise; every other
    metric follows its driver via the fitted slope plus its own AR(1) noise.
    """

    def __init__(self, baseline: dict, models: dict[str, MetricModel], order: list[str],
                 sim: dict, seed: int, asset_id: str, freq_s: int) -> None:
        wl, noise_cfg = sim["workload"], sim["noise"]
        self.baseline, self.models, self.order, self.wl = baseline, models, order, wl
        self.phi = float(noise_cfg["ar_phi"])
        self.innov = math.sqrt(1 - self.phi * self.phi)
        self.regime_rng = random.Random(f"{seed}|{asset_id}|regime")
        self.noise_rng = {m: random.Random(f"{seed}|{asset_id}|noise|{m}") for m in METRICS}
        self.sigma = {m: float(noise_cfg["noise_fraction"]) * models[m].resid_std for m in METRICS}
        self.state = {m: self.noise_rng[m].gauss(0.0, 1.0) * self.sigma[m] for m in METRICS}
        self.p_change = float(wl["regime_changes_per_hour"]) * freq_s / 3600.0
        self.alpha = 1.0 - math.exp(-freq_s / (float(wl["regime_smoothing_minutes"]) * 60.0))
        self.offset = self.target = 0.0

    def step(self, ts: datetime) -> dict[str, float]:
        wl = self.wl
        if self.regime_rng.random() < self.p_change:
            self.target = self.regime_rng.uniform(-wl["regime_max_shift"], wl["regime_max_shift"])
        self.offset += (self.target - self.offset) * self.alpha
        hour = ts.hour + ts.minute / 60.0 + ts.second / 3600.0
        diurnal = wl["diurnal_amplitude"] * math.cos(2 * math.pi * (hour - wl["diurnal_peak_hour_utc"]) / 24)
        for m in METRICS:
            self.state[m] = self.phi * self.state[m] + self.innov * self.sigma[m] * self.noise_rng[m].gauss(0.0, 1.0)

        values: dict[str, float] = {}
        for m in self.order:
            model = self.models[m]
            base = float(self.baseline[m])
            if model.driver is None:
                raw = base + diurnal + self.offset + self.state[m]
            else:
                raw = base + model.slope * (values[model.driver] - float(self.baseline[model.driver])) + self.state[m]
            values[m] = round(_clip(raw, model.lo, model.hi), 3)
        return values


def _place_anomalies(spec_list: list[dict], asset_ids: list[str], n_slots: int,
                     seed: int, window: int = 0) -> list[dict]:
    """Place anomalies inside one window of n_slots. Window 0 uses the original stream."""
    rng = random.Random(f"{seed}|anomalies" if window == 0 else f"{seed}|anomalies|window{window}")
    placed: list[dict] = []
    for spec in spec_list:
        metrics = list(spec["metrics"])
        unknown = [m for m in metrics if m not in METRICS]
        if unknown:
            raise SimulationConfigError(f"anomaly {spec['type']} uses non-simulated metrics {unknown}")
        if spec["shape"] not in ("step", "ramp"):
            raise SimulationConfigError(f"unknown anomaly shape {spec['shape']!r}")
        # Durations are clamped to the window so short runs still work.
        hi_d = min(int(spec["duration_minutes"][1]), n_slots)
        lo_d = min(int(spec["duration_minutes"][0]), hi_d)
        for _ in range(int(spec["count"])):
            for _attempt in range(200):
                asset_id = rng.choice(asset_ids)
                duration = rng.randint(int(lo_d), int(hi_d))
                start = rng.randint(0, n_slots - duration)
                end = start + duration - 1
                clash = any(p["asset_id"] == asset_id and set(p["metrics"]) & set(metrics)
                            and not (end < p["start_slot"] or start > p["end_slot"]) for p in placed)
                if not clash:
                    break
            else:
                # No room left in this window (only happens for very short runs): place fewer
                # anomalies rather than overlap them. The caller reports requested vs placed.
                continue
            placed.append({"anomaly_type": spec["type"], "asset_id": asset_id, "metrics": metrics,
                           "shape": spec["shape"], "magnitude_std": float(spec["magnitude_std"]),
                           "start_slot": start, "end_slot": end})
    return placed


@dataclass(frozen=True)
class SlotReading:
    """One asset at one time slot, as produced by TelemetrySimulator.iter_slots()."""
    asset: dict                       # asset_metadata row (static, copied from the baseline source row)
    slot_index: int                   # 0-based, counted from the simulation start
    ts_utc: str
    is_missing: bool
    values: dict[str, float] | None   # observed values incl. injected anomalies; None if missing
    clean: dict[str, float]           # values before anomaly injection
    anomalies: tuple[dict, ...]       # anomaly_events rows active at this slot (empty if missing)


class TelemetrySimulator:
    """Streaming, deterministic telemetry generator.

    Slots are produced in time order (all assets for slot 0, then slot 1, ...) and nothing
    is materialized beyond the current anomaly window, so runs can be finite or unbounded.
    The configured duration defines one window: anomalies are placed per window, and the
    first window is identical to the stored Phase 1 run for the same config.
    """

    def __init__(self, config: dict, seed_rows: list[dict], source_file: str, source_sha256: str) -> None:
        sim = config["simulation"]
        self.config, self.sim = config, sim
        self.seed = int(sim["seed"])
        self.freq_s = int(sim["frequency_seconds"])
        self.duration = int(sim["duration_minutes"])
        if self.freq_s <= 0 or self.duration <= 0:
            raise SimulationConfigError("duration_minutes and frequency_seconds must be positive")
        if (self.duration * 60) % self.freq_s:
            raise SimulationConfigError("duration must be a whole number of frequency intervals")
        self.n_slots = self.duration * 60 // self.freq_s
        asset_count = int(sim["asset_count"])
        self.missing_rate = float(sim["missing_rate"])
        if not 0 <= self.missing_rate < 1:
            raise SimulationConfigError("missing_rate must be in [0, 1)")
        if asset_count < 1:
            raise SimulationConfigError("asset_count must be at least 1")
        if asset_count > len(seed_rows):
            raise SimulationConfigError(f"asset_count {asset_count} > {len(seed_rows)} seed rows")

        self.start = parse_utc(sim["start_ts_utc"])
        order = generation_order(sim.get("drivers") or {})
        self.models = fit_metric_models(seed_rows, sim.get("drivers") or {})
        self.config_json = json.dumps({"scenario": config["scenario"], "simulation": sim,
                                       "anomalies": config.get("anomalies", {})}, sort_keys=True)
        self.run_id = compute_run_id(json.loads(self.config_json), source_sha256)
        self.source_file, self.source_sha256 = source_file, source_sha256

        ordered = sorted(seed_rows, key=lambda r: r["source_row_number"])
        baselines = random.Random(f"{self.seed}|assets").sample(ordered, asset_count)
        self.assets: list[dict] = []
        self._asset_sims: dict[str, AssetSimulator] = {}
        self._missing_rng: dict[str, random.Random] = {}
        for i, row in enumerate(baselines, start=1):
            asset_id = f"SIM-{row['datacenter_region']}-{i:03d}"
            self.assets.append({"run_id": self.run_id, "asset_id": asset_id, "identifier_origin": "synthetic",
                                "baseline_source_file": row["source_file"],
                                "baseline_source_row_number": row["source_row_number"],
                                **{a: row[a] for a in ASSET_ATTRIBUTES}})
            self._asset_sims[asset_id] = AssetSimulator(row, self.models[row["server_type"]], order,
                                                        sim, self.seed, asset_id, self.freq_s)
            self._missing_rng[asset_id] = random.Random(f"{self.seed}|{asset_id}|missing")
        self._asset_type = {a["asset_id"]: a["server_type"] for a in self.assets}
        self._window_cache: tuple[int, list[dict], dict] | None = None
        self.window_anomalies(0)  # validate anomaly config up front

    def timestamp(self, slot: int) -> datetime:
        return self.start + timedelta(seconds=slot * self.freq_s)

    def window_anomalies(self, window: int) -> list[dict]:
        """anomaly_events rows for one window (slots are absolute, counted from the start)."""
        return self._window(window)[1]

    def _window(self, window: int) -> tuple[int, list[dict], dict]:
        if self._window_cache is None or self._window_cache[0] != window:
            offset = window * self.n_slots
            placed = _place_anomalies(self.config.get("anomalies", {}).get("events", []),
                                      [a["asset_id"] for a in self.assets], self.n_slots, self.seed, window)
            requested = sum(int(e["count"]) for e in self.config.get("anomalies", {}).get("events", []))
            if len(placed) < requested:
                log.warning("window too short for all configured anomalies; placed fewer",
                            extra={"context": {"window": window, "requested": requested, "placed": len(placed),
                                               "window_slots": self.n_slots}})
            events, schedule = [], {}
            for idx, ev in enumerate(placed, start=1):
                anomaly_id = (f"{self.run_id}-A{idx:03d}" if window == 0
                              else f"{self.run_id}-W{window:03d}-A{idx:03d}")
                start, end = ev["start_slot"] + offset, ev["end_slot"] + offset
                row = {"anomaly_id": anomaly_id, "run_id": self.run_id, "asset_id": ev["asset_id"],
                       "anomaly_type": ev["anomaly_type"], "metrics_json": json.dumps(ev["metrics"]),
                       "shape": ev["shape"], "magnitude_std": ev["magnitude_std"],
                       "start_slot": start, "end_slot": end,
                       "start_ts_utc": format_utc(self.timestamp(start)),
                       "end_ts_utc": format_utc(self.timestamp(end))}
                events.append(row)
                ramp_len = max(1, (end - start + 1) // 3)
                for k, slot in enumerate(range(start, end + 1)):
                    factor = 1.0 if ev["shape"] == "step" else min(1.0, (k + 1) / ramp_len)
                    schedule.setdefault((ev["asset_id"], slot), []).append((row, ev["metrics"], factor))
            self._window_cache = (window, events, schedule)
        return self._window_cache

    def iter_slots(self, max_slots: int | None = None):
        """Yield SlotReading objects in time order. max_slots=None runs until the caller stops."""
        slot = 0
        while max_slots is None or slot < max_slots:
            _, _, schedule = self._window(slot // self.n_slots)
            ts = self.timestamp(slot)
            ts_text = format_utc(ts)
            for asset in self.assets:
                aid = asset["asset_id"]
                clean = self._asset_sims[aid].step(ts)
                is_missing = self._missing_rng[aid].random() < self.missing_rate
                values, active = dict(clean), []
                for row, metrics, factor in schedule.get((aid, slot), ()):
                    for m in metrics:
                        delta = row["magnitude_std"] * self.models[self._asset_type[aid]][m].std * factor
                        values[m] = round(clean[m] + delta, 3)
                    active.append(row)
                yield SlotReading(asset, slot, ts_text, is_missing, None if is_missing else values,
                                  clean, () if is_missing else tuple(active))
            slot += 1

    def run_row(self, content_sha256: str) -> dict:
        return {"run_id": self.run_id, "generator_name": GENERATOR_NAME, "generator_version": GENERATOR_VERSION,
                "seed": self.seed, "start_ts_utc": format_utc(self.start), "duration_minutes": self.duration,
                "frequency_seconds": self.freq_s, "expected_slots": self.n_slots,
                "asset_count": len(self.assets), "missing_rate": self.missing_rate,
                "region_filter": region_filter_name(self.config),
                "source_file": self.source_file, "source_sha256": self.source_sha256,
                "config_json": self.config_json, "content_sha256": content_sha256}


def generate(config: dict, seed_rows: list[dict], source_file: str, source_sha256: str) -> SimulationResult:
    """Materialize one window (the configured duration) for SQLite storage. No I/O."""
    simulator = TelemetrySimulator(config, seed_rows, source_file, source_sha256)
    run_id = simulator.run_id
    events = simulator.window_anomalies(0)
    event_index = {e["anomaly_id"]: i for i, e in enumerate(events)}
    per_asset: dict[str, list[tuple]] = {a["asset_id"]: [] for a in simulator.assets}
    labels_keyed = []
    for r in simulator.iter_slots(simulator.n_slots):
        aid = r.asset["asset_id"]
        values = [None] * len(METRICS) if r.is_missing else [r.values[m] for m in METRICS]
        per_asset[aid].append((run_id, aid, r.ts_utc, r.slot_index, "synthetic",
                               int(r.is_missing), int(bool(r.anomalies)), *values))
        for ev in r.anomalies:
            for pos, m in enumerate(json.loads(ev["metrics_json"])):
                labels_keyed.append(((event_index[ev["anomaly_id"]], r.slot_index, pos),
                                     (ev["anomaly_id"], run_id, aid, r.ts_utc, m, r.clean[m], r.values[m])))
    readings = [row for a in simulator.assets for row in per_asset[a["asset_id"]]]
    labels = [label for _, label in sorted(labels_keyed)]

    digest = hashlib.sha256()
    for part in (simulator.assets, readings, events, labels):
        digest.update(json.dumps(part, sort_keys=True, default=str).encode())
    run = simulator.run_row(digest.hexdigest())
    return SimulationResult(run, simulator.assets, readings, events, labels, digest.hexdigest(), simulator.models)


def load_seed_rows(conn: sqlite3.Connection, filter_name: str, source_file: str = SOURCE_CSV) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM filtered_source_records WHERE filter_name = ? AND source_file = ? "
        "ORDER BY source_row_number", (filter_name, source_file)).fetchall()
    if not rows:
        raise SimulationConfigError(
            f"no source rows for filter {filter_name!r}; run the region filter first")
    return [dict(r) for r in rows]


def source_checksum(conn: sqlite3.Connection, source_file: str = SOURCE_CSV) -> str:
    row = conn.execute("SELECT sha256 FROM source_files WHERE source_file = ?", (source_file,)).fetchone()
    if row is None:
        raise SimulationConfigError(f"{source_file} has not been loaded; run load_source first")
    return row["sha256"]


def write_run(conn: sqlite3.Connection, result: SimulationResult) -> None:
    """Replace any previous copy of this run_id in one transaction (idempotent rerun)."""
    run = dict(result.run, created_at=datetime.now(timezone.utc).isoformat())
    asset_cols = list(result.assets[0])
    event_cols = list(result.events[0]) if result.events else []
    with conn:
        conn.execute("DELETE FROM simulation_runs WHERE run_id = ?", (run["run_id"],))
        conn.execute(f"INSERT INTO simulation_runs ({', '.join(run)}) VALUES ({', '.join('?' * len(run))})",
                     list(run.values()))
        conn.executemany(
            f"INSERT INTO asset_metadata ({', '.join(asset_cols)}) VALUES ({', '.join('?' * len(asset_cols))})",
            [[a[c] for c in asset_cols] for a in result.assets])
        conn.executemany(
            f"INSERT INTO telemetry_readings ({', '.join(READING_COLUMNS)}) "
            f"VALUES ({', '.join('?' * len(READING_COLUMNS))})", result.readings)
        if result.events:
            conn.executemany(
                f"INSERT INTO anomaly_events ({', '.join(event_cols)}) VALUES ({', '.join('?' * len(event_cols))})",
                [[e[c] for c in event_cols] for e in result.events])
        conn.executemany(
            f"INSERT INTO anomaly_labels ({', '.join(LABEL_COLUMNS)}) VALUES ({', '.join('?' * len(LABEL_COLUMNS))})",
            result.labels)


def export_run(result: SimulationResult, out_root: Path | None = None) -> Path:
    out = (out_root or SIMULATION_DIR) / result.run["run_id"]
    out.mkdir(parents=True, exist_ok=True)

    def dump(name: str, header, rows) -> None:
        with (out / name).open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerows(rows)

    dump("telemetry_readings.csv", READING_COLUMNS, result.readings)
    dump("anomaly_labels.csv", LABEL_COLUMNS, result.labels)
    if result.assets:
        dump("asset_metadata.csv", list(result.assets[0]), [list(a.values()) for a in result.assets])
    if result.events:
        dump("anomaly_events.csv", list(result.events[0]), [list(e.values()) for e in result.events])
    (out / "run.json").write_text(json.dumps(
        {**result.run, "config": json.loads(result.run["config_json"]),
         "provenance": "synthetic",
         "note": "Generated by this project. Not real historical telemetry."}, indent=2), encoding="utf-8")
    return out


def run_simulation(conn: sqlite3.Connection, config: dict, export: bool = True) -> SimulationResult:
    filter_name = region_filter_name(config)
    seed_rows = load_seed_rows(conn, filter_name)
    result = generate(config, seed_rows, SOURCE_CSV, source_checksum(conn))
    write_run(conn, result)
    out = export_run(result) if export else None
    log.info("simulation run written", extra={"context": {
        "run_id": result.run["run_id"], "assets": len(result.assets),
        "slots": len(result.readings), "missing": sum(r[5] for r in result.readings),
        "anomaly_events": len(result.events), "anomaly_labels": len(result.labels),
        "content_sha256": result.content_sha256, "export_dir": str(out) if out else None}})
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", type=Path, default=None)
    args = ap.parse_args()
    conn = connect()
    try:
        init_db(conn)
        run_simulation(conn, load_config(args.config))
    except SimulationConfigError:
        log.exception("simulation failed")
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
