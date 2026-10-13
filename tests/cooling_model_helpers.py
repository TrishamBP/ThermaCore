"""Synthetic fixtures for the cooling response model tests (controlled physics, fixed seeds).

Two worlds, both generated here (not real telemetry):
- "observational": inlet follows workload; fan speed varies independently but has NO effect
  on inlet (like the project simulator) -> the model must stay a forecasting_baseline.
- "causal": inlet relaxes toward a target that DEPENDS on the fan speed (more fan, cooler) and
  the fan steps independently of workload -> the validity check must accept the fan.
"""

import dataclasses
import math
from pathlib import Path

import numpy as np

from cooling_helpers import event, seed
from data_engineering.cooling_model.config import load_settings
from data_engineering.cooling_model.features import US_PER_MINUTE, Series
from data_engineering.simulation.generate_telemetry import METRICS


def settings(artifact_dir: Path, **optimizer):
    """Shipped config with a test artifact dir, small row minimum and optional optimizer overrides."""
    base = load_settings()
    model = dataclasses.replace(base.model, artifact_dir=artifact_dir, min_training_rows=300)
    return dataclasses.replace(base, model=model, optimizer=dataclasses.replace(base.optimizer, **optimizer))


def world(kind: str, minutes: int, asset_index: int, rng: np.random.Generator) -> list[dict]:
    """Per-minute metric dicts for one asset."""
    w = 0.5 + 0.3 * np.sin(2 * math.pi * (np.arange(minutes) / 240 + asset_index / 3))
    fan = np.repeat(rng.uniform(2000, 4000, minutes // 10 + 1), 10)[:minutes]
    inlet = np.empty(minutes)
    inlet[0] = 21.0
    for t in range(1, minutes):
        target = 18.0 + 6.0 * w[t - 1]
        if kind == "causal":
            target -= 0.002 * (fan[t - 1] - 3000.0)
        inlet[t] = inlet[t - 1] + 0.3 * (target - inlet[t - 1]) + rng.normal(0, 0.02)
    rows = []
    for t in range(minutes):
        rows.append({"workload_intensity": float(w[t]), "cpu_utilization": float(90 * w[t]),
                     "inlet_temperature_c": float(inlet[t]), "exhaust_temperature_c": float(inlet[t] + 9 + 4 * w[t]),
                     "fan_speed_rpm": float(fan[t]), "airflow_cfm": float(fan[t] * 0.06),
                     "power_consumption_kw": float(60 + 80 * w[t])})
    return rows


def seed_world(store, kind: str, assets: int = 3, minutes: int = 600, seed_value: int = 7, run: str = "sim-test-run"):
    rng = np.random.default_rng(seed_value)
    for a in range(assets):
        asset_id = f"T-{a:02d}"
        events = [event(asset_id, m, run=run, **row) for m, row in enumerate(world(kind, minutes, a, rng))]
        seed(store, events)


def make_series(inlet, *, t_minutes=None, asset_type="Compute", **fields) -> Series:
    """Series with explicit values (unspecified metrics constant)."""
    inlet = np.asarray(inlet, float)
    n = len(inlet)
    t = np.asarray(t_minutes if t_minutes is not None else np.arange(n), float)
    values = {m: np.full(n, 1.0) for m in METRICS}
    values["inlet_temperature_c"] = inlet
    for k, v in fields.items():
        values[k] = np.asarray(v, float)
    values["temperature_delta_c"] = values["exhaust_temperature_c"] - values["inlet_temperature_c"]
    return Series((1_767_268_800_000_000 + t * US_PER_MINUTE).astype(np.int64), values,
                  {"asset_id": "X", "asset_type": asset_type, "cooling_type": "Air"})
