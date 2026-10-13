"""Load and validate config/cooling_state.yaml (override with COOLING_STATE_CONFIG)."""

import os
from dataclasses import dataclass, field, fields, replace
from pathlib import Path

import yaml

from data_engineering.paths import REPO_ROOT

DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "cooling_state.yaml"
TREND_METRICS = ("inlet_temperature_c", "exhaust_temperature_c", "temperature_delta_c", "cpu_utilization",
                 "gpu_utilization", "workload_intensity", "fan_speed_rpm", "airflow_cfm",
                 "power_consumption_kw", "pue")


class CoolingConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Limits:
    """Site/equipment limits. None = not configured -> dependent rules are not evaluated."""
    inlet_temperature_high_c: float | None = None
    inlet_temperature_critical_c: float | None = None
    exhaust_temperature_high_c: float | None = None
    temperature_delta_high_c: float | None = None
    sustained_inlet_rise_c_per_minute: float | None = None
    rapid_inlet_rise_c_per_minute: float | None = None
    airflow_drop_fraction: float | None = None
    fan_airflow_divergence_fraction: float | None = None
    inlet_approach_margin_c: float | None = None          # within this many deg C below the inlet limit
    inlet_approach_horizon_minutes: float | None = None   # projected to reach the inlet limit this soon
    light_load_cpu_utilization_pct: float | None = None   # window-mean CPU below this = light load
    light_load_gpu_utilization_pct: float | None = None   # GPU servers: window-mean GPU must also be below


@dataclass(frozen=True)
class CoolingConfig:
    window_minutes: int = 15
    recent_minutes: int = 5
    expected_interval_seconds: int = 60
    gap_tolerance_factor: float = 1.5
    min_points: int = 5
    min_coverage: float = 0.8
    max_missing_fraction: float = 0.2
    stale_after_seconds: int = 300
    max_ingest_age_seconds: int = 300
    trend_min_change: dict = field(default_factory=dict)
    limits: Limits = field(default_factory=Limits)

    def validate(self) -> "CoolingConfig":
        problems = []
        ints = ("window_minutes", "recent_minutes", "expected_interval_seconds", "min_points",
                "stale_after_seconds", "max_ingest_age_seconds")
        for name in ints:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                problems.append(f"{name} must be a positive integer (got {value!r})")
        if not problems:
            if self.window_minutes > 1440:
                problems.append("window_minutes must be <= 1440")
            if self.recent_minutes > self.window_minutes:
                problems.append("recent_minutes must be <= window_minutes")
            if self.min_points < 2:
                problems.append("min_points must be >= 2 (a trend needs two points)")
        if not _is_number(self.gap_tolerance_factor) or self.gap_tolerance_factor < 1:
            problems.append("gap_tolerance_factor must be a number >= 1")
        for name in ("min_coverage", "max_missing_fraction"):
            value = getattr(self, name)
            if not _is_number(value) or not 0 <= value <= 1:
                problems.append(f"{name} must be between 0 and 1")
        unknown = set(self.trend_min_change) - set(TREND_METRICS)
        if unknown:
            problems.append(f"trend_min_change has unknown metrics {sorted(unknown)}")
        for name, value in self.trend_min_change.items():
            if not _is_number(value) or value <= 0:
                problems.append(f"trend_min_change.{name} must be a positive number")
        missing = set(TREND_METRICS) - set(self.trend_min_change)
        if missing:
            problems.append(f"trend_min_change is missing {sorted(missing)}")
        for f in fields(Limits):
            value = getattr(self.limits, f.name)
            if value is not None and not _is_number(value):
                problems.append(f"limits.{f.name} must be a number or null")
        lim = self.limits
        for name in ("airflow_drop_fraction", "fan_airflow_divergence_fraction"):
            value = getattr(lim, name)
            if _is_number(value) and not 0 < value < 1:
                problems.append(f"limits.{name} must be between 0 and 1 (exclusive)")
        for name in ("sustained_inlet_rise_c_per_minute", "rapid_inlet_rise_c_per_minute", "temperature_delta_high_c",
                     "inlet_approach_margin_c", "inlet_approach_horizon_minutes"):
            value = getattr(lim, name)
            if _is_number(value) and value <= 0:
                problems.append(f"limits.{name} must be > 0")
        for name in ("light_load_cpu_utilization_pct", "light_load_gpu_utilization_pct"):
            value = getattr(lim, name)
            if _is_number(value) and not 0 < value <= 100:
                problems.append(f"limits.{name} must be > 0 and <= 100 (percent)")
        if (_is_number(lim.inlet_temperature_high_c) and _is_number(lim.inlet_temperature_critical_c)
                and lim.inlet_temperature_critical_c < lim.inlet_temperature_high_c):
            problems.append("limits.inlet_temperature_critical_c must be >= limits.inlet_temperature_high_c")
        if problems:
            raise CoolingConfigError("; ".join(problems))
        return self

    def with_overrides(self, window_minutes: int | None = None, **limits) -> "CoolingConfig":
        """Copy with a different window and/or limits (used by the API and tests); re-validated."""
        updated = self if window_minutes is None else replace(self, window_minutes=window_minutes)
        if window_minutes is not None and updated.recent_minutes > window_minutes:
            updated = replace(updated, recent_minutes=window_minutes)
        if limits:
            updated = replace(updated, limits=replace(updated.limits, **limits))
        return updated.validate()


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def load_cooling_config(path: Path | str | None = None) -> CoolingConfig:
    path = Path(path or os.environ.get("COOLING_STATE_CONFIG") or DEFAULT_CONFIG_PATH)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise CoolingConfigError(f"cooling state config not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise CoolingConfigError(f"invalid YAML in {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise CoolingConfigError(f"{path} must contain a mapping")
    known = {f.name for f in fields(CoolingConfig)}
    unknown = set(raw) - known
    if unknown:
        raise CoolingConfigError(f"unknown settings in {path}: {sorted(unknown)}")
    limits_raw = raw.pop("limits", None) or {}
    if not isinstance(limits_raw, dict):
        raise CoolingConfigError("limits must be a mapping")
    unknown_limits = set(limits_raw) - {f.name for f in fields(Limits)}
    if unknown_limits:
        raise CoolingConfigError(f"unknown limits in {path}: {sorted(unknown_limits)}")
    return CoolingConfig(**raw, limits=Limits(**limits_raw)).validate()
