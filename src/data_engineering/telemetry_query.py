"""Read-only telemetry query functions shared by the dashboard, telemetry API and copilot tools.

Every function takes an already-open store, explicit (run, asset, start, end) bounds and a row cap,
uses only parameterized store queries, and never mixes assets: aggregation is always per asset.

Aggregation semantics (documented in the API):
  - readings with the same (asset, event_timestamp) collapse to the most recently ingested one;
  - only reading_status = 'observed' contributes values; 'missing' readings are counted, never zero;
  - buckets are [bucket_start, bucket_start + interval) aligned to the epoch in UTC; each metric reports
    mean/min/max/count of its observed values (null when count = 0);
  - non-GPU assets report GPU utilization as not applicable (null), not 0.
"""

from datetime import datetime, timedelta, timezone

import numpy as np

from data_engineering.simulation.generate_telemetry import METRICS

UNITS = {
    "workload_intensity": "ratio_0_1", "cpu_utilization": "percent", "gpu_utilization": "percent",
    "memory_utilization": "percent", "storage_utilization": "percent", "disk_io_iops": "iops",
    "network_throughput_gbps": "Gbps", "network_latency_ms": "ms", "ambient_temperature_c": "degC",
    "humidity_percentage": "percent", "inlet_temperature_c": "degC", "exhaust_temperature_c": "degC",
    "fan_speed_rpm": "rpm", "airflow_cfm": "cfm", "power_consumption_kw": "kW", "pue": "ratio",
    "temperature_delta_c": "degC",
}
DERIVED = {"temperature_delta_c": "exhaust_temperature_c - inlet_temperature_c (same reading)"}
SUPPORTED_METRICS = (*METRICS, "temperature_delta_c")
METRIC_NOTES = {
    "power_consumption_kw": "per-server IT power; not facility power and not cooling power",
    "pue": "PUE value carried in each asset's simulated telemetry; not a measured facility ratio",
    "gpu_utilization": "not applicable (null) for non-GPU assets",
}
# Not in the telemetry at all.
UNAVAILABLE_MEASUREMENTS = {
    "cooling_power_kw": "no measured cooling-system power (fans, CRAH/CRAC, chiller) in the telemetry",
    "facility_power_kw": "no facility-level power measurement in the telemetry",
    "cooling_efficiency": "not in the telemetry schema",
    "cooling_setpoints": "no setpoint or actuator-command field exists",
}
INTERVALS = {"raw": None, "1m": 60, "5m": 300, "15m": 900, "1h": 3600}
GPU_ASSET_TYPES = frozenset({"GPU"})


def iso(ts: datetime | None) -> str | None:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if ts else None


def dedupe(rows: list[dict]) -> tuple[list[dict], int]:
    """Collapse same (asset, timestamp) rows to the latest ingested; keeps chronological order per asset."""
    rows = sorted(rows, key=lambda r: (r["asset_id"], r["event_timestamp"], r.get("ingested_at") or 0))
    out: list[dict] = []
    for r in rows:
        if out and out[-1]["asset_id"] == r["asset_id"] and out[-1]["event_timestamp"] == r["event_timestamp"]:
            out[-1] = r
        else:
            out.append(r)
    return out, len(rows) - len(out)


def value(row: dict, metric: str, asset_type: str | None):
    if row.get("reading_status", "observed") != "observed":
        return None
    if metric == "gpu_utilization" and asset_type not in GPU_ASSET_TYPES:
        return None
    if metric == "temperature_delta_c":
        a, b = row.get("exhaust_temperature_c"), row.get("inlet_temperature_c")
        return None if a is None or b is None else a - b
    return row.get(metric)


def _stats(values: list[float]) -> dict:
    if not values:
        return {"mean": None, "min": None, "max": None, "count": 0}
    arr = np.asarray(values, float)
    return {"mean": round(float(arr.mean()), 4), "min": round(float(arr.min()), 4), "max": round(float(arr.max()), 4),
            "count": int(len(arr))}


def series(rows: list[dict], metrics: list[str], asset_type: str | None, interval_s: int | None) -> dict:
    """Rows of ONE asset (deduped, chronological) -> points. raw: one point per reading."""
    if interval_s is None:
        points = [{"timestamp": iso(r["event_timestamp"]), "reading_status": r["reading_status"],
                   **{m: value(r, m, asset_type) for m in metrics}} for r in rows]
        return {"points": points, "missing_readings": sum(r["reading_status"] != "observed" for r in rows)}
    buckets: dict[int, list[dict]] = {}
    for r in rows:
        key = int(r["event_timestamp"].timestamp()) // interval_s * interval_s
        buckets.setdefault(key, []).append(r)
    points = []
    for key in sorted(buckets):
        group = buckets[key]
        point = {"bucket_start": iso(datetime.fromtimestamp(key, timezone.utc)), "readings": len(group),
                 "missing_readings": sum(r["reading_status"] != "observed" for r in group)}
        for m in metrics:
            point[m] = _stats([v for r in group if (v := value(r, m, asset_type)) is not None])
        points.append(point)
    return {"points": points, "missing_readings": sum(r["reading_status"] != "observed" for r in rows)}


def irregular_intervals(rows: list[dict], expected_s: int = 60, tolerance: float = 1.5) -> dict:
    ts = [r["event_timestamp"] for r in rows]
    gaps = [(b - a).total_seconds() for a, b in zip(ts, ts[1:])]
    return {"gaps_over_tolerance": sum(g > expected_s * tolerance for g in gaps),
            "largest_gap_seconds": max(gaps) if gaps else None}


def trend_per_minute(points: list[tuple[datetime, float]]) -> float | None:
    """Least-squares slope (units per minute) against elapsed time; None with < 3 points."""
    if len(points) < 3:
        return None
    t = np.array([(p[0] - points[0][0]).total_seconds() / 60 for p in points])
    y = np.array([p[1] for p in points], float)
    if np.ptp(t) == 0:
        return None
    return round(float(np.polyfit(t, y, 1)[0]), 6)


def asset_window(store, run_id: str, asset: dict, start: datetime, end: datetime, metrics: list[str],
                 interval: str, limit: int) -> dict:
    """One asset's telemetry in (start, end]; limit caps returned points (truncated flag set)."""
    rows, duplicates = dedupe([{**r, "asset_id": asset["asset_id"]}
                               for r in store.readings_window(run_id, asset["asset_id"], start, end)])
    result = series(rows, metrics, asset.get("asset_type"), INTERVALS[interval])
    truncated = len(result["points"]) > limit
    return {"asset_id": asset["asset_id"], "asset_type": asset.get("asset_type"), "interval": interval,
            "points": result["points"][:limit], "truncated": truncated, "readings": len(rows),
            "duplicates_collapsed": duplicates, "missing_readings": result["missing_readings"],
            **irregular_intervals(rows)}


def latest_per_asset(store, run_id: str, as_of: datetime | None, assets: dict[str, dict],
                     asset_id: str | None = None) -> dict[str, dict]:
    rows, _ = dedupe(store.latest_observed(run_id, as_of, asset_id))
    return {r["asset_id"]: r for r in rows if r["asset_id"] in assets}


def window_bounds(start: datetime | None, end: datetime | None, default_end: datetime | None,
                  default_minutes: int, max_hours: int) -> tuple[datetime, datetime]:
    """Resolve and validate a (start, end] range; raises ValueError with an explanation."""
    end = end or default_end
    if end is None:
        raise ValueError("no telemetry: the time range cannot be resolved")
    start = start or end - timedelta(minutes=default_minutes)
    if start >= end:
        raise ValueError("start_time must be before end_time")
    if end - start > timedelta(hours=max_hours):
        raise ValueError(f"time range exceeds the maximum of {max_hours} hours")
    return start, end
