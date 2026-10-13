"""Load and validate config/cooling_model.yaml (override with COOLING_MODEL_CONFIG)."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from data_engineering.paths import REPO_ROOT
from data_engineering.simulation.generate_telemetry import METRICS

DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "cooling_model.yaml"
DERIVED = ("temperature_delta_c",)
SERIES_FIELDS = (*METRICS, *DERIVED)
CATEGORICAL_FIELDS = ("asset_type", "cooling_type")


class CoolingModelConfigError(ValueError):
    pass


@dataclass(frozen=True)
class FeatureConfig:
    current: tuple[str, ...]
    lags_minutes: dict[str, tuple[int, ...]]
    rates_minutes: dict[str, tuple[int, ...]]
    rolling_minutes: dict[str, tuple[int, ...]]
    time_of_day: bool
    categorical: tuple[str, ...]

    @property
    def lookback_minutes(self) -> int:
        spans = [k for group in (self.lags_minutes, self.rates_minutes, self.rolling_minutes)
                 for ks in group.values() for k in ks]
        return max(spans, default=0)

    def to_dict(self) -> dict:
        return {"current": list(self.current),
                "lags_minutes": {k: list(v) for k, v in self.lags_minutes.items()},
                "rates_minutes": {k: list(v) for k, v in self.rates_minutes.items()},
                "rolling_minutes": {k: list(v) for k, v in self.rolling_minutes.items()},
                "time_of_day": self.time_of_day, "categorical": list(self.categorical)}

    @classmethod
    def from_dict(cls, d: dict) -> "FeatureConfig":
        return cls(current=tuple(d.get("current", ())),
                   lags_minutes={k: tuple(v) for k, v in (d.get("lags_minutes") or {}).items()},
                   rates_minutes={k: tuple(v) for k, v in (d.get("rates_minutes") or {}).items()},
                   rolling_minutes={k: tuple(v) for k, v in (d.get("rolling_minutes") or {}).items()},
                   time_of_day=bool(d.get("time_of_day", False)),
                   categorical=tuple(d.get("categorical", ())))


@dataclass(frozen=True)
class ModelConfig:
    artifact_dir: Path
    horizons_minutes: tuple[int, ...]
    min_training_rows: int
    min_series_minutes: int
    train_fraction: float
    validation_fraction: float
    features: FeatureConfig
    xgboost: dict
    validity: dict


@dataclass(frozen=True)
class OptimizerConfig:
    controllable_setting: str
    t_safe_c: float | None
    setting_min: float | None
    setting_max: float | None
    search_step: float | None
    supported_range_quantiles: tuple[float, float]
    measured_cooling_power_column: str | None
    recommendation_only: bool = True


@dataclass(frozen=True)
class CoolingModelSettings:
    model: ModelConfig
    optimizer: OptimizerConfig
    source: Path = field(default=DEFAULT_CONFIG_PATH)


def _num(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_features(f: FeatureConfig) -> list[str]:
    problems = []
    for name in f.current:
        if name not in SERIES_FIELDS:
            problems.append(f"features.current: unknown field {name!r}")
    for group_name in ("lags_minutes", "rates_minutes", "rolling_minutes"):
        for name, ks in getattr(f, group_name).items():
            if name not in SERIES_FIELDS:
                problems.append(f"features.{group_name}: unknown field {name!r}")
            if not ks or any(not isinstance(k, int) or isinstance(k, bool) or k <= 0 for k in ks):
                problems.append(f"features.{group_name}.{name}: minutes must be positive integers")
    for name in f.categorical:
        if name not in CATEGORICAL_FIELDS:
            problems.append(f"features.categorical: unknown field {name!r} (allowed: {CATEGORICAL_FIELDS})")
    if "inlet_temperature_c" not in f.current:
        problems.append("features.current must include inlet_temperature_c (the forecast is relative to it)")
    return problems


def load_settings(path: Path | str | None = None) -> CoolingModelSettings:
    path = Path(path or os.environ.get("COOLING_MODEL_CONFIG") or DEFAULT_CONFIG_PATH)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise CoolingModelConfigError(f"cooling model config not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise CoolingModelConfigError(f"invalid YAML in {path}: {exc}") from exc
    if not isinstance(raw, dict) or set(raw) - {"model", "optimizer"}:
        raise CoolingModelConfigError(f"{path} must contain only 'model' and 'optimizer' sections")
    m, o = raw.get("model") or {}, raw.get("optimizer") or {}
    problems = []
    try:
        features = FeatureConfig.from_dict(m.get("features") or {})
    except (TypeError, AttributeError) as exc:
        raise CoolingModelConfigError(f"model.features is malformed: {exc}") from exc
    problems += validate_features(features)

    horizons = tuple(m.get("horizons_minutes") or ())
    if not horizons or any(not isinstance(h, int) or isinstance(h, bool) or h <= 0 for h in horizons):
        problems.append("model.horizons_minutes must be a non-empty list of positive integers")
    if len(set(horizons)) != len(horizons):
        problems.append("model.horizons_minutes contains duplicates")
    for name in ("min_training_rows", "min_series_minutes"):
        if not isinstance(m.get(name), int) or m.get(name) <= 0:
            problems.append(f"model.{name} must be a positive integer")
    tf, vf = m.get("train_fraction"), m.get("validation_fraction")
    if not (_num(tf) and _num(vf) and 0 < tf < 1 and 0 < vf < 1 and tf + vf < 1):
        problems.append("model.train_fraction and validation_fraction must be in (0, 1) with a sum < 1")
    xgb = dict(m.get("xgboost") or {})
    if not isinstance(xgb.get("num_boost_round"), int) or xgb.get("num_boost_round", 0) <= 0:
        problems.append("model.xgboost.num_boost_round must be a positive integer")
    if "seed" not in xgb:
        problems.append("model.xgboost.seed is required (reproducibility)")
    validity = dict(m.get("validity") or {})
    for name, spec in (validity.get("controllable_settings") or {}).items():
        if name not in SERIES_FIELDS:
            problems.append(f"model.validity.controllable_settings: unknown field {name!r}")
        if (spec or {}).get("expected_effect_sign") not in (-1, 1):
            problems.append(f"model.validity.controllable_settings.{name}.expected_effect_sign must be -1 or 1")

    q = tuple(o.get("supported_range_quantiles") or (0.01, 0.99))
    if len(q) != 2 or not all(_num(x) for x in q) or not 0 <= q[0] < q[1] <= 1:
        problems.append("optimizer.supported_range_quantiles must be [low, high] with 0 <= low < high <= 1")
    for name in ("t_safe_c", "setting_min", "setting_max", "search_step"):
        if o.get(name) is not None and not _num(o.get(name)):
            problems.append(f"optimizer.{name} must be a number or null")
    if _num(o.get("search_step")) and o["search_step"] <= 0:
        problems.append("optimizer.search_step must be > 0")
    if _num(o.get("setting_min")) and _num(o.get("setting_max")) and o["setting_min"] >= o["setting_max"]:
        problems.append("optimizer.setting_min must be < setting_max")
    if o.get("recommendation_only", True) is not True:
        problems.append("optimizer.recommendation_only cannot be disabled: this system never actuates equipment")
    setting = o.get("controllable_setting", "fan_speed_rpm")
    if setting not in SERIES_FIELDS:
        problems.append(f"optimizer.controllable_setting: unknown field {setting!r}")
    if problems:
        raise CoolingModelConfigError("; ".join(problems))

    artifact_dir = Path(m.get("artifact_dir", "models/cooling_response"))
    model = ModelConfig(artifact_dir=artifact_dir if artifact_dir.is_absolute() else REPO_ROOT / artifact_dir,
                        horizons_minutes=horizons, min_training_rows=m["min_training_rows"],
                        min_series_minutes=m["min_series_minutes"], train_fraction=float(tf),
                        validation_fraction=float(vf), features=features, xgboost=xgb, validity=validity)
    optimizer = OptimizerConfig(
        controllable_setting=setting, t_safe_c=o.get("t_safe_c"), setting_min=o.get("setting_min"),
        setting_max=o.get("setting_max"), search_step=o.get("search_step"), supported_range_quantiles=q,
        measured_cooling_power_column=(o.get("energy_objective") or {}).get("measured_cooling_power_column"))
    return CoolingModelSettings(model=model, optimizer=optimizer, source=path)
