"""Constrained cooling optimizer (recommendation-only; it never actuates equipment).

    minimize   E_cooling(u)
    subject to predicted_inlet(u, t+h) <= T_safe  for every horizon h
               setting_min <= u <= setting_max,  u inside the training data's supported range

Evaluating a candidate u requires an intervention-aware response model: feeding a different fan
speed into a forecaster trained on observational data is NOT a prediction of what that fan speed
would do. Candidates are therefore only evaluated when the active model is labelled
"intervention_validated" (see validity.py). Otherwise the result is UNAVAILABLE with reasons.

The energy objective needs a MEASURED cooling-power source plus an energy-response model. Neither
exists in this telemetry (power_consumption_kw is per-server IT power), so with a validated
temperature model the best possible result is CONSTRAINTS_ONLY (feasible candidates, no
energy-optimal pick). Selection logic itself (select_setting) is generic and unit-tested.

Statuses: RECOMMENDATION_AVAILABLE | NO_FEASIBLE_SETTING | CONSTRAINTS_ONLY | UNAVAILABLE
"""

from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime

import numpy as np

from data_engineering.cooling_model.config import OptimizerConfig
from data_engineering.cooling_model.features import US_PER_MINUTE
from data_engineering.cooling_model.predictor import CoolingResponseModel
from data_engineering.simulation.generate_telemetry import METRICS


@dataclass
class CandidateEvaluation:
    setting: float
    is_current: bool
    within_bounds: bool
    within_supported_range: bool
    predicted_inlet_c: dict[str, float] = field(default_factory=dict)   # horizon -> deg C
    satisfies_t_safe: bool | None = None
    estimated_cooling_energy: float | None = None
    feasible: bool = False
    notes: list[str] = field(default_factory=list)


def generate_candidates(current: float | None, setting_min: float, setting_max: float, step: float) -> list[float]:
    """Grid over [min, max] at `step`, plus the current setting when it is within bounds."""
    if not (setting_min < setting_max) or step <= 0:
        raise ValueError("need setting_min < setting_max and step > 0")
    n = int(np.floor((setting_max - setting_min) / step + 1e-9))
    grid = [round(setting_min + i * step, 6) for i in range(n + 1)]
    if grid[-1] < setting_max:
        grid.append(float(setting_max))
    if current is not None and setting_min <= current <= setting_max:
        grid.append(round(float(current), 6))
    return sorted(set(grid))


def evaluate_candidates(candidates: list[float], current: float | None, predict_fn: Callable[[float], dict[str, float]],
                        t_safe_c: float, bounds: tuple[float, float], supported: tuple[float, float],
                        energy_fn: Callable[[float], float] | None = None) -> list[CandidateEvaluation]:
    out = []
    for u in candidates:
        ev = CandidateEvaluation(setting=u, is_current=current is not None and abs(u - current) < 1e-6,
                                 within_bounds=bounds[0] <= u <= bounds[1],
                                 within_supported_range=supported[0] <= u <= supported[1])
        if not ev.within_bounds:
            ev.notes.append("outside actuator bounds")
        if not ev.within_supported_range:
            ev.notes.append(f"outside the training data's supported range [{supported[0]:.4g}, {supported[1]:.4g}]: "
                            "unsupported counterfactual, not evaluated")
        if ev.within_bounds and ev.within_supported_range:
            ev.predicted_inlet_c = {str(k): float(v) for k, v in predict_fn(u).items()}
            ev.satisfies_t_safe = all(v <= t_safe_c for v in ev.predicted_inlet_c.values())
            if not ev.satisfies_t_safe:
                ev.notes.append(f"predicted inlet exceeds T_safe {t_safe_c} deg C")
            if energy_fn is not None:
                ev.estimated_cooling_energy = float(energy_fn(u))
            ev.feasible = bool(ev.satisfies_t_safe)
        out.append(ev)
    return out


def select_setting(evaluations: list[CandidateEvaluation], current: float | None,
                   rel_tol: float = 1e-9) -> tuple[CandidateEvaluation | None, str]:
    """Lowest-energy feasible candidate; ties -> current setting, then smallest change, then lower setting."""
    feasible = [e for e in evaluations if e.feasible and e.estimated_cooling_energy is not None]
    if not feasible:
        return None, "no candidate satisfies every constraint"
    best_energy = min(e.estimated_cooling_energy for e in feasible)
    tied = [e for e in feasible if e.estimated_cooling_energy - best_energy <= rel_tol * max(1.0, abs(best_energy))]
    ref = current if current is not None else 0.0
    choice = min(tied, key=lambda e: (not e.is_current, abs(e.setting - ref), e.setting))
    reason = "lowest estimated cooling energy among feasible candidates"
    if len(tied) > 1:
        reason += f"; {len(tied)} candidates tied, " + ("kept the current setting" if choice.is_current
                                                           else "chose the smallest change from the current setting")
    return choice, reason


def counterfactual_features(model: CoolingResponseModel, x: np.ndarray, series, setting: str, u: float) -> np.ndarray:
    """Copy of x with `setting` (and its own rate features) replaced by candidate u. Only used for
    intervention-validated models; other coupled inputs are NOT adjusted."""
    names = model.metadata["feature_names"]
    out = x.copy()
    if setting in names:
        out[names.index(setting)] = u
    for k in model.feature_config.rates_minutes.get(setting, ()):
        name = f"{setting}_rate_{k}m"
        keys = series.t_us[-1] - k * US_PER_MINUTE
        idx = np.searchsorted(series.t_us, keys)
        if name in names and idx < len(series) and series.t_us[idx] == keys:
            out[names.index(name)] = (u - series.values[setting][idx]) / k
    return out


def _unevaluated(candidates, cfg: OptimizerConfig, model, codes: list[str]) -> list[dict]:
    r = ((model.metadata.get("feature_ranges") or {}).get(cfg.controllable_setting) or {}) if model else {}
    out = []
    for u in candidates:
        bounds = None if None in (cfg.setting_min, cfg.setting_max) else cfg.setting_min <= u <= cfg.setting_max
        support = None if r.get("q_low") is None else r["q_low"] <= u <= r["q_high"]
        out.append({"setting": float(u), "evaluated": False, "within_bounds": bounds,
                    "within_supported_range": support, "predicted_inlet_c": {}, "feasible": None,
                    "estimated_cooling_energy": None, "notes": ["not evaluated: " + ", ".join(codes)]})
    return out


def recommend(model: CoolingResponseModel | None, store, run_id: str, asset_id: str, cfg: OptimizerConfig,
              as_of: datetime | None = None, candidates: list[float] | None = None) -> dict:
    """candidates: operator-supplied settings to evaluate instead of the configured grid."""
    setting = cfg.controllable_setting
    result = {"status": "UNAVAILABLE", "reasons": [], "recommendation_only": True,
              "actuation": "none: this system only recommends; it never sends commands to equipment",
              "asset_id": asset_id, "simulation_run_id": run_id, "controllable_setting": setting,
              "current_setting": None, "recommended_setting": None, "selection_reason": None,
              "forecast_under_current_conditions": [], "current_conditions_check": None, "candidates": [],
              "energy_objective": {"available": False, "source": None}, "model_version": None, "model_label": None,
              "warnings": []}
    if model is None:
        result["reasons"].append({"code": "MODEL_NOT_TRAINED", "detail": "no active cooling response model"})
        if candidates:
            result["candidates"] = _unevaluated(candidates, cfg, None, ["MODEL_NOT_TRAINED"])
        return result
    result.update(model_version=model.version, model_label=model.label)
    forecast = model.predict_for_asset(store, run_id, asset_id, as_of)
    series, x = forecast.pop("_series"), forecast.pop("_features")
    result["forecast_under_current_conditions"] = forecast["forecasts"]
    result["warnings"] = forecast["model_validity_warnings"]
    observed = series.values[setting][np.isfinite(series.values[setting])]
    current = float(observed[-1]) if len(observed) else None
    result["current_setting"] = current

    check = model.metadata["validity"]["controllable_settings"].get(setting)
    if check is None or not check.get("validated"):
        reasons = (check or {}).get("reasons") or [f"{setting} was not checked"]
        result["reasons"].append({"code": "RESPONSE_MODEL_NOT_VALIDATED",
                                  "detail": f"the model is a '{model.label}'; candidate {setting} values cannot be "
                                            "evaluated: " + " | ".join(reasons)})
    if cfg.measured_cooling_power_column is None:
        result["reasons"].append({"code": "ENERGY_OBJECTIVE_MISSING",
                                  "detail": "no measured cooling-system power exists in the telemetry "
                                            "(power_consumption_kw is per-server IT power); energy-optimal "
                                            "selection is impossible"})
    elif cfg.measured_cooling_power_column not in METRICS:
        result["reasons"].append({"code": "ENERGY_COLUMN_NOT_IN_TELEMETRY",
                                  "detail": f"{cfg.measured_cooling_power_column!r} is not a telemetry field"})
    else:
        result["reasons"].append({"code": "ENERGY_RESPONSE_MODEL_MISSING",
                                  "detail": "a measured power column exists but no validated model of its response "
                                            "to the setting has been built"})
    if cfg.t_safe_c is None:
        result["reasons"].append({"code": "T_SAFE_NOT_CONFIGURED", "detail": "optimizer.t_safe_c is not set"})
    else:
        worst = max(f["predicted_inlet_temperature_c"] for f in forecast["forecasts"])
        result["current_conditions_check"] = {
            "t_safe_c": cfg.t_safe_c, "max_predicted_inlet_c": worst, "satisfied": worst <= cfg.t_safe_c,
            "basis": "forecast under observed conditions (no counterfactual)"}
    if None in (cfg.setting_min, cfg.setting_max) or (not candidates and cfg.search_step is None):
        result["reasons"].append({"code": "ACTUATOR_BOUNDS_NOT_CONFIGURED",
                                  "detail": "optimizer.setting_min and setting_max (and search_step for the grid "
                                            "search) must be set from verified equipment limits"})
    blocking = {"RESPONSE_MODEL_NOT_VALIDATED", "T_SAFE_NOT_CONFIGURED", "ACTUATOR_BOUNDS_NOT_CONFIGURED"}
    if any(r["code"] in blocking for r in result["reasons"]):
        if candidates:
            result["candidates"] = _unevaluated(candidates, cfg, model, [r["code"] for r in result["reasons"]])
        return result

    # Only reachable with an intervention-validated model and configured constraints.
    r = model.metadata["feature_ranges"].get(setting) or {}
    supported = (r.get("q_low", -np.inf), r.get("q_high", np.inf))
    if candidates:
        candidates = sorted({float(u) for u in candidates} | ({round(current, 6)} if current is not None and
                                                             cfg.setting_min <= current <= cfg.setting_max else set()))
    else:
        candidates = generate_candidates(current, cfg.setting_min, cfg.setting_max, cfg.search_step)

    def predict(u):
        return {h: float(forecast["latest_inlet_temperature_c"] + d[0])
                for h, d in model.predict_delta(counterfactual_features(model, x, series, setting, u)[None, :]).items()}

    evaluations = evaluate_candidates(candidates, current, predict, cfg.t_safe_c, (cfg.setting_min, cfg.setting_max),
                                      supported, energy_fn=None)
    result["candidates"] = [{**asdict(e), "evaluated": bool(e.predicted_inlet_c)} for e in evaluations]
    result["warnings"].append("Candidate temperatures are model-based counterfactual estimates.")
    if not any(e.feasible for e in evaluations):
        result.update(status="NO_FEASIBLE_SETTING",
                      selection_reason="no evaluated candidate keeps the predicted inlet at or below T_safe")
    else:
        result.update(status="CONSTRAINTS_ONLY",
                      selection_reason="feasible candidates listed; no energy objective, so none is recommended")
    return result
