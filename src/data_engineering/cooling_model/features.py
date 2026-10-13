"""Feature and target construction for one asset's telemetry series (pure numpy, no I/O).

Leakage rules:
- every feature at time t uses only readings with timestamp <= t;
- lags are looked up by exact timestamp (t - k minutes), never by row offset, so gaps and
  irregular spacing produce NaN instead of a silently wrong value;
- targets are the inlet temperature exactly h minutes after t (NaN if that reading does not exist);
- missing readings are NaN, never 0 (XGBoost treats NaN as missing);
- GPU utilization of non-GPU servers is a structural 0 in the telemetry and is set to NaN;
- categorical one-hot columns use categories learned from the training split only.
"""

import math
import warnings
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

from data_engineering.cooling_model.config import FeatureConfig
from data_engineering.simulation.generate_telemetry import METRICS

US_PER_MINUTE = 60_000_000
GPU_ASSET_TYPES = frozenset({"GPU"})


@dataclass
class Series:
    """One asset's readings: sorted, one row per timestamp."""
    t_us: np.ndarray                  # int64 microseconds since epoch (event time)
    values: dict[str, np.ndarray]     # field -> float64, NaN = missing
    asset: dict                       # asset_metadata/assets row (asset_type, cooling_type, ...)
    duplicates_dropped: int = 0

    def __len__(self) -> int:
        return len(self.t_us)


def _us(ts) -> int:
    return int(round(ts.timestamp() * 1_000_000))


def series_from_rows(rows: Sequence[dict], asset: dict) -> Series:
    """rows: dicts with event_timestamp, ingested_at, reading_status and metric columns.

    Same-timestamp rows collapse to the most recently ingested one (ties: last in input order),
    the same rule as the cooling state estimator.
    """
    ordered = sorted(enumerate(rows), key=lambda ir: (ir[1]["event_timestamp"], ir[1].get("ingested_at") or 0, ir[0]))
    kept: list[dict] = []
    for _, row in ordered:
        if kept and kept[-1]["event_timestamp"] == row["event_timestamp"]:
            kept[-1] = row
        else:
            kept.append(row)
    is_gpu = asset.get("asset_type") in GPU_ASSET_TYPES
    values = {}
    for m in METRICS:
        col = [row.get(m) if row.get("reading_status", "observed") == "observed" else None for row in kept]
        arr = np.array([np.nan if v is None else float(v) for v in col], dtype=np.float64)
        arr[~np.isfinite(arr)] = np.nan
        values[m] = arr
    if not is_gpu:
        values["gpu_utilization"] = np.full(len(kept), np.nan)
    values["temperature_delta_c"] = values["exhaust_temperature_c"] - values["inlet_temperature_c"]
    t_us = np.array([_us(r["event_timestamp"]) for r in kept], dtype=np.int64)
    return Series(t_us=t_us, values=values, asset=asset, duplicates_dropped=len(rows) - len(kept))


def _at_offset(series: Series, field: str, offset_minutes: int) -> np.ndarray:
    """Value of `field` exactly offset minutes away from each row's timestamp (negative = past)."""
    keys = series.t_us + offset_minutes * US_PER_MINUTE
    idx = np.searchsorted(series.t_us, keys)
    idx_c = np.clip(idx, 0, max(len(series) - 1, 0))
    ok = (idx < len(series)) & (series.t_us[idx_c] == keys) if len(series) else np.zeros(0, bool)
    out = np.full(len(series), np.nan)
    out[ok] = series.values[field][idx_c[ok]]
    return out


def feature_names(fc: FeatureConfig, categories: dict[str, list[str]]) -> list[str]:
    names = [f"{f}" for f in fc.current]
    names += [f"{f}_lag_{k}m" for f, ks in fc.lags_minutes.items() for k in ks]
    names += [f"{f}_rate_{k}m" for f, ks in fc.rates_minutes.items() for k in ks]
    names += [f"{f}_{stat}_{w}m" for f, ws in fc.rolling_minutes.items() for w in ws
              for stat in ("mean", "std", "min", "max")]
    if fc.time_of_day:
        names += ["hour_sin", "hour_cos"]
    names += [f"{c}={v}" for c in fc.categorical for v in categories.get(c, [])]
    return names


def build_features(series: Series, fc: FeatureConfig, categories: dict[str, list[str]]) -> np.ndarray:
    """Feature matrix (rows x feature_names) for every row of the series."""
    n = len(series)
    cols: list[np.ndarray] = [series.values[f] for f in fc.current]
    for f, ks in fc.lags_minutes.items():
        cols += [_at_offset(series, f, -k) for k in ks]
    for f, ks in fc.rates_minutes.items():
        cols += [(series.values[f] - _at_offset(series, f, -k)) / k for k in ks]
    for f, ws in fc.rolling_minutes.items():
        v = series.values[f]
        for w in ws:
            lo = np.searchsorted(series.t_us, series.t_us - w * US_PER_MINUTE, side="right")
            stats = np.full((n, 4), np.nan)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)       # all-NaN windows -> NaN
                for i in range(n):
                    window = v[lo[i]:i + 1]
                    if np.isfinite(window).any():
                        stats[i] = (np.nanmean(window), np.nanstd(window), np.nanmin(window), np.nanmax(window))
            cols += [stats[:, j] for j in range(4)]
    if fc.time_of_day:
        hours = (series.t_us / 1_000_000 % 86_400) / 3_600
        cols += [np.sin(2 * math.pi * hours / 24), np.cos(2 * math.pi * hours / 24)]
    for c in fc.categorical:
        value = series.asset.get(c)
        cols += [np.full(n, 1.0 if value == v else 0.0) for v in categories.get(c, [])]
    return np.column_stack(cols) if cols else np.zeros((n, 0))


def build_targets(series: Series, horizons: Sequence[int]) -> dict[int, np.ndarray]:
    """Target per horizon: inlet(t + h) - inlet(t); NaN when either reading is missing."""
    inlet = series.values["inlet_temperature_c"]
    return {h: _at_offset(series, "inlet_temperature_c", h) - inlet for h in horizons}
