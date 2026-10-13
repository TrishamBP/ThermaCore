"""Deterministic evidence tools (no LLM). Every number an agent or explanation may use is produced here.

gather_context() does ALL database reads of one analysis (the graph's agent nodes never touch the
store): the cooling state snapshot of the current window, snapshots of previous windows (history),
the XGBoost forecast and the optimizer result. The *_evidence() functions turn that context into
typed Evidence items, each with a stable id that agents must cite.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import numpy as np

from data_engineering.cooling.config import CoolingConfig
from data_engineering.cooling.estimator import CoolingStateEstimator
from data_engineering.cooling.models import CoolingStateSnapshot
from data_engineering.cooling_model.config import CoolingModelSettings
from data_engineering.cooling_model.optimizer import recommend
from data_engineering.cooling_model.predictor import CoolingResponseModel, InsufficientInferenceDataError
from data_engineering.energy import fan_peer
from data_engineering.recommendations.config import WorkflowConfig
from data_engineering.recommendations.schemas import Evidence


@dataclass
class AnalysisContext:
    run_id: str
    asset_id: str
    as_of_requested: datetime | None
    snapshot: CoolingStateSnapshot
    history: list[CoolingStateSnapshot]                 # previous windows, most recent first
    forecast: dict | None
    forecast_error: str | None
    optimizer: dict | None
    model_summary: dict | None
    cooling_config: CoolingConfig
    model_settings: CoolingModelSettings
    workflow: WorkflowConfig
    notes: list[str] = field(default_factory=list)
    fan_peer: dict | None = None             # fan over-speed peer comparison for this asset (relative only)

    @property
    def window(self) -> str:
        s = self.snapshot
        start = s.window_start.isoformat() if s.window_start else "?"
        end = s.as_of.isoformat() if s.as_of else "?"
        return f"{s.window_minutes}-minute window ({start} to {end})"


def model_summary(model: CoolingResponseModel | None) -> dict | None:
    if model is None:
        return None
    ev = model.metadata["evaluation"]
    return {"version": model.version, "label": model.label,
            "test_rmse_skill_vs_persistence": {h: e["test"]["rmse_skill_vs_persistence"] for h, e in ev.items()},
            "test_rmse_c": {h: e["test"]["model"]["rmse"] for h, e in ev.items()},
            "persistence_rmse_c": {h: e["test"]["persistence"]["rmse"] for h, e in ev.items()},
            "horizons_minutes": model.metadata["horizons_minutes"],
            "source_type": model.metadata["training_data"].get("source_type")}


def gather_context(store, run_id: str, asset_id: str, as_of: datetime | None, cooling_config: CoolingConfig,
                   model: CoolingResponseModel | None, model_settings: CoolingModelSettings,
                   workflow: WorkflowConfig, clock=None) -> AnalysisContext:
    """All reads for one analysis. Raises estimator UnknownScopeError for unknown runs/assets."""
    estimator = CoolingStateEstimator(store, cooling_config, **({"clock": clock} if clock else {}))
    snapshot = estimator.evaluate(asset_id, run_id, None, as_of)
    history = []
    if snapshot.as_of is not None:
        end = snapshot.window_start
        for _ in range(workflow.comparison_windows):
            history.append(estimator.evaluate(asset_id, run_id, None, end))
            end = end - timedelta(minutes=cooling_config.window_minutes)
    forecast, forecast_error, optimizer = None, None, None
    if model is None:
        forecast_error = "no cooling response model is trained"
    elif snapshot.as_of is not None:
        kwargs = {"clock": clock} if clock else {}
        try:
            forecast = model.predict_for_asset(store, run_id, asset_id, snapshot.as_of, **kwargs)
            forecast.pop("_features", None)
            forecast.pop("_series", None)
        except (InsufficientInferenceDataError, LookupError) as exc:
            forecast_error = str(exc)
        if forecast is not None:
            optimizer = recommend(model, store, run_id, asset_id, model_settings.optimizer, snapshot.as_of)
    peer = None
    if snapshot.as_of is not None:
        result = fan_peer.analyze(store, run_id, snapshot.as_of, workflow.fan_peer, [asset_id])
        peer = {**result["results"][0], "limitations": result["limitations"], "method": result["method"]} \
            if result["results"] else None
    return AnalysisContext(run_id=run_id, asset_id=asset_id, as_of_requested=as_of, snapshot=snapshot,
                           history=history, forecast=forecast, forecast_error=forecast_error, optimizer=optimizer,
                           model_summary=model_summary(model), cooling_config=cooling_config,
                           model_settings=model_settings, workflow=workflow, fan_peer=peer)


# ------------------------------------------------------------------ helpers

class _Builder:
    def __init__(self, agent: str, prefix: str, window: str) -> None:
        self.agent, self.prefix, self.window, self.items = agent, prefix, window, []

    def add(self, kind, metric, value, unit, interpretation, source, window=None, **details) -> Evidence:
        ev = Evidence(evidence_id=f"{self.prefix}-{len(self.items) + 1}", agent=self.agent, kind=kind, metric=metric,
                      observed_value=None if value is None else float(value), unit=unit,
                      observation_window=window or self.window, interpretation=interpretation, source=source,
                      details=details)
        self.items.append(ev)
        return ev


def _metric(snapshot, name):
    return snapshot.metrics.get(name)


def _finding_evidence(b: _Builder, snapshot, prefixes: tuple[str, ...]) -> None:
    for f in snapshot.findings:
        if f.rule_id.startswith(prefixes):
            b.add("rule_finding", f.rule_id, None, "none", f.explanation, "cooling_state_estimator",
                  rule_id=f.rule_id, severity=f.severity, finding_kind=f.kind, state=f.state,
                  evidence={k: v for k, v in f.evidence.items() if isinstance(v, (int, float, str, bool, type(None)))})


# ------------------------------------------------------------------ thermal

def thermal_evidence(ctx: AnalysisContext) -> list[Evidence]:
    s, lim = ctx.snapshot, ctx.cooling_config.limits
    b = _Builder("thermal", "TH", ctx.window)
    inlet, exhaust, delta = _metric(s, "inlet_temperature_c"), _metric(s, "exhaust_temperature_c"), \
        _metric(s, "temperature_delta_c")
    workload = _metric(s, "workload_intensity")
    if inlet is not None and inlet.latest is not None:
        b.add("measured", "inlet_temperature_c.latest", inlet.latest, "°C", "latest inlet reading", "telemetry")
        b.add("measured", "inlet_temperature_c.max", inlet.maximum, "°C", "highest inlet reading in the window",
              "telemetry")
        if inlet.trend_per_minute is not None:
            b.add("derived", "inlet_temperature_c.trend_per_minute", inlet.trend_per_minute, "°C/min",
                  f"least-squares slope over the window (direction: {inlet.direction})", "cooling_state_estimator")
        if inlet.recent_rate_per_minute is not None:
            b.add("derived", "inlet_temperature_c.recent_rate_per_minute", inlet.recent_rate_per_minute, "°C/min",
                  f"rate over the last {ctx.cooling_config.recent_minutes} minutes", "cooling_state_estimator")
    else:
        b.add("unavailable", "inlet_temperature_c", None, "°C", "no observed inlet temperature in the window",
              "telemetry")
    if exhaust is not None and exhaust.latest is not None:
        b.add("measured", "exhaust_temperature_c.latest", exhaust.latest, "°C", "latest exhaust reading", "telemetry")
    if delta is not None and delta.latest is not None:
        b.add("derived", "temperature_delta_c.latest", delta.latest, "°C", "exhaust minus inlet", "derived")
    if lim.inlet_temperature_high_c is not None:
        b.add("configuration", "limits.inlet_temperature_high_c", lim.inlet_temperature_high_c, "°C",
              "configured inlet limit (config/cooling_state.yaml)", "configuration")
        if inlet is not None and inlet.maximum is not None:
            b.add("derived", "inlet_headroom_c", lim.inlet_temperature_high_c - inlet.maximum, "°C",
                  "configured inlet limit minus the highest inlet reading in the window", "derived")
    else:
        b.add("unavailable", "limits.inlet_temperature_high_c", None, "°C", "inlet limit is not configured",
              "configuration")
    if lim.inlet_approach_margin_c is not None:
        b.add("configuration", "limits.inlet_approach_margin_c", lim.inlet_approach_margin_c, "°C",
              "configured approach margin", "configuration")
    if workload is not None and workload.mean is not None:
        b.add("measured", "workload_intensity.mean", workload.mean, "fraction",
              f"window mean (direction: {s.workload_direction})", "telemetry")
        if workload.std_dev is not None:
            b.add("derived", "workload_intensity.std_dev", workload.std_dev, "fraction", "window standard deviation",
                  "cooling_state_estimator")
    _finding_evidence(b, s, ("TH_", "WL_"))
    f = ctx.forecast
    if f is None:
        b.add("unavailable", "inlet_forecast", None, "°C", f"no forecast: {ctx.forecast_error}", "cooling_response_model")
    else:
        ms = ctx.model_summary or {}
        for fc in f["forecasts"]:
            h = str(fc["horizon_minutes"])
            skill = (ms.get("test_rmse_skill_vs_persistence") or {}).get(h)
            b.add("model_prediction", f"inlet_forecast_{h}min", fc["predicted_inlet_temperature_c"], "°C",
                  f"XGBoost forecast for {fc['forecast_timestamp']} under OBSERVED conditions "
                  f"(label {f['model_label']}; offline test RMSE {fc['offline_test_rmse_c']:.3f} °C vs "
                  f"persistence {fc['offline_persistence_rmse_c']:.3f} °C)", "cooling_response_model",
                  horizon_minutes=fc["horizon_minutes"], persistence_c=fc["persistence_inlet_temperature_c"],
                  offline_test_rmse_c=fc["offline_test_rmse_c"], skill_vs_persistence=skill,
                  model_version=f["model_version"], data_quality=f["data_quality"]["status"],
                  extrapolated_features=len(f["extrapolated_features"]))
    o = ctx.optimizer
    if o is not None:
        b.add("model_prediction" if o["status"] != "UNAVAILABLE" else "unavailable", "cooling_optimizer.status", None,
              "status", f"optimizer status {o['status']}: " + "; ".join(r["code"] for r in o["reasons"]),
              "cooling_optimizer", status=o["status"], reasons=[r["code"] for r in o["reasons"]],
              current_setting=o["current_setting"], recommended_setting=o["recommended_setting"])
        if o["current_setting"] is not None:
            b.add("measured", f"{o['controllable_setting']}.latest", o["current_setting"], "rpm",
                  "current (latest observed) setting", "telemetry")
    return b.items


# ------------------------------------------------------------------ energy

def energy_evidence(ctx: AnalysisContext) -> list[Evidence]:
    s = ctx.snapshot
    b = _Builder("energy", "EN", ctx.window)
    power = _metric(s, "power_consumption_kw")
    if power is not None and power.latest is not None:
        b.add("measured", "power_consumption_kw.latest", power.latest, "kW",
              "per-server IT power; NOT cooling power and never used as cooling power", "telemetry")
    pue = _metric(s, "pue")
    if pue is not None and pue.latest is not None:
        b.add("measured", "pue.latest", pue.latest, "ratio",
              "PUE value reported with this asset's telemetry (simulated); there is no validated method here to "
              "convert it into cooling power, so it is not used for energy estimates", "telemetry")
    column = ctx.model_settings.optimizer.measured_cooling_power_column
    b.add("unavailable" if column is None else "configuration", "measured_cooling_power", None, "kW",
          "no measured cooling-system power (fans, CRAH/CRAC, chiller) exists in the telemetry" if column is None
          else f"configured cooling power column {column!r} (no validated energy-response model)", "configuration",
          configured_column=column)
    p = ctx.fan_peer
    if p is not None:
        window = f"window ending {p['window']['end']}" if p.get("window") else ctx.window
        group = "/".join(str(v) for v in p["group"].values())
        b.add("derived", "fan_peer_comparison.status", None, "status",
              f"fan-speed peer comparison ({group} peers, similar workload and inlet): {p['status']}"
              + (f" - {'; '.join(p['reasons'])}" if p["reasons"] else ""), "energy.fan_peer", window,
              status=p["status"], reference_windows=p["reference_windows"],
              reference_assets=len(p["reference_assets"]))
        if p.get("asset_fan_rpm") is not None:
            b.add("measured", "fan_speed_rpm.window_mean", p["asset_fan_rpm"], "rpm",
                  "this asset's mean fan speed in the comparison window", "telemetry", window)
        if p.get("peer_median_fan_rpm") is not None:
            b.add("derived", "fan_speed_rpm.peer_median", p["peer_median_fan_rpm"], "rpm",
                  f"median fan speed of {p['reference_windows']} comparable windows from "
                  f"{len(p['reference_assets'])} peer asset(s)", "energy.fan_peer", window)
            b.add("derived", "fan_speed_rpm.excess_fraction", p["excess_fraction"], "fraction",
                  "fan speed relative to the peer median minus 1", "energy.fan_peer", window)
            b.add("derived", "fan_power.relative_to_peers", p["relative_fan_power"], "ratio",
                  "fan affinity-law power ratio (rpm/peer median)^3: relative only, assumes the same fan model and "
                  "system curve; not kW", "energy.fan_peer", window)
        if p.get("persistence") and p["persistence"]["windows_compared"]:
            b.add("derived", "fan_peer_comparison.windows_elevated", p["persistence"]["windows_elevated"], "count",
                  f"windows with ELEVATED fan speed out of {p['persistence']['windows_compared']} compared in the "
                  "lookback", "energy.fan_peer", window)
    o = ctx.optimizer
    b.add("unavailable", "cooling_energy_impact", None, "kW",
          "energy impact of candidate settings cannot be estimated: "
          + ("; ".join(r["code"] for r in o["reasons"]) if o else "optimizer not run"), "cooling_optimizer")
    return b.items


def energy_measurement_available(ctx: AnalysisContext) -> bool:
    return ctx.model_settings.optimizer.measured_cooling_power_column is not None


def energy_analysis_available(ctx: AnalysisContext) -> bool:
    """Something for the energy specialist to interpret: measured cooling power or a peer comparison result."""
    return energy_measurement_available(ctx) or (
        ctx.fan_peer is not None and ctx.fan_peer["status"] in ("ELEVATED", "WITHIN_PEER_RANGE", "BELOW_PEERS"))


# ------------------------------------------------------------------ anomaly / maintenance / data quality

def _window_means(snapshots, name):
    out = []
    for snap in snapshots:
        m = snap.metrics.get(name)
        out.append(None if m is None else m.mean)
    return out


def anomaly_evidence(ctx: AnalysisContext) -> list[Evidence]:
    s, wf = ctx.snapshot, ctx.workflow
    b = _Builder("anomaly", "AN", ctx.window)
    _finding_evidence(b, s, ("AF_", "DQ_"))
    dq = s.data_quality
    b.add("derived", "data_quality.status", None, "status",
          f"telemetry data quality {dq.status}; coverage {dq.coverage}; missing readings {dq.missing_readings}; "
          f"gaps {len(dq.gaps)}; conflicting duplicates {dq.conflicting_duplicates}", "cooling_state_estimator",
          status=dq.status, stale=dq.stale, coverage=dq.coverage, missing_readings=dq.missing_readings,
          gaps=len(dq.gaps), conflicting_duplicates=dq.conflicting_duplicates, missing_fields=dq.missing_fields)
    if dq.coverage is not None:
        b.add("derived", "data_quality.coverage", dq.coverage, "fraction", "share of expected readings present",
              "cooling_state_estimator")
    if dq.ingest_age_seconds is not None:
        b.add("measured", "pipeline.ingest_age_seconds", dq.ingest_age_seconds, "seconds",
              "wall-clock time since the store last received data", "telemetry_store")
    delta = _metric(s, "temperature_delta_c")
    if delta is not None and delta.minimum is not None and delta.minimum < 0:
        b.add("derived", "temperature_delta_c.min", delta.minimum, "°C",
              "exhaust colder than inlet: physically implausible for air passing through a server; suggests a "
              "sensor or mapping problem", "derived", contradictory=True)
    fan, air = _metric(s, "fan_speed_rpm"), _metric(s, "airflow_cfm")
    if fan is not None and air is not None and fan.latest and air.latest is not None and fan.latest > 0 \
            and air.latest == 0:
        b.add("derived", "airflow_cfm.latest", 0.0, "CFM", "zero airflow while the fan reports rotation: "
              "contradictory signals", "derived", contradictory=True)
    if ctx.forecast is not None and ctx.forecast["data_quality"]["status"] != "OK":
        b.add("derived", "forecast_inputs.data_quality", None, "status",
              "forecast inputs degraded: " + "; ".join(ctx.forecast["data_quality"]["notes"]), "cooling_response_model",
              status=ctx.forecast["data_quality"]["status"])
    # Comparison with recent operating history (previous windows of the same asset).
    for name, unit in (("temperature_delta_c", "°C"), ("inlet_temperature_c", "°C")):
        cur = _metric(s, name)
        hist = [v for v in _window_means(ctx.history, name) if v is not None]
        if cur is None or cur.mean is None or len(hist) < 2:
            continue
        mu, sd = float(np.mean(hist)), float(np.std(hist, ddof=1))
        z = None if sd == 0 else (cur.mean - mu) / sd
        unusual = z is not None and abs(z) > wf.history_deviation_z
        b.add("derived", f"{name}.history_z", z, "none",
              f"current window mean {cur.mean:.3f} {unit} vs mean {mu:.3f} {unit} of the previous {len(hist)} windows "
              + ("(unusual)" if unusual else "(within the usual range)"), "derived",
              history_mean=mu, history_std=sd, current_mean=cur.mean, unusual=unusual)
    fan_hist, air_hist = _window_means(ctx.history, "fan_speed_rpm"), _window_means(ctx.history, "airflow_cfm")
    ratios = [a / f for f, a in zip(fan_hist, air_hist) if f and a is not None]
    if fan is not None and air is not None and fan.mean and air.mean is not None and len(ratios) >= 2:
        cur_ratio, mu, sd = air.mean / fan.mean, float(np.mean(ratios)), float(np.std(ratios, ddof=1))
        z = None if sd == 0 else (cur_ratio - mu) / sd
        b.add("derived", "airflow_per_fan_rpm.history_z", z, "none",
              f"airflow per fan rpm {cur_ratio:.5f} vs {mu:.5f} in the previous {len(ratios)} windows", "derived",
              current=cur_ratio, history_mean=mu, history_std=sd,
              unusual=z is not None and abs(z) > wf.history_deviation_z)
    # Persistence of anomaly findings across windows (current + history).
    windows = [s, *ctx.history]
    for rule_id in sorted({f.rule_id for f in s.findings if f.rule_id.startswith(("AF_", "TH_"))
                           and f.severity != "INFO"}):
        present = [any(f.rule_id == rule_id for f in w.findings) for w in windows]
        consecutive = next((i for i, p in enumerate(present) if not p), len(present))
        b.add("derived", f"{rule_id}.persistent_windows", consecutive, "count",
              f"{rule_id} present in {consecutive} consecutive window(s) up to now (of {len(windows)} examined)",
              "cooling_state_estimator", rule_id=rule_id, consecutive_windows=consecutive,
              history_quality=[w.data_quality.status for w in windows[:consecutive]])
    return b.items


# ------------------------------------------------------------------ workload

def workload_evidence(ctx: AnalysisContext) -> list[Evidence]:
    """Utilization levels/trends and how inlet temperature moved alongside them (correlation only)."""
    s = ctx.snapshot
    b = _Builder("workload", "WL", ctx.window)
    for name, unit in (("workload_intensity", "fraction"), ("cpu_utilization", "percent"), ("gpu_utilization", "percent"),
                       ("memory_utilization", "percent")):
        m = _metric(s, name)
        if m is None:
            continue
        if not m.applicable:
            b.add("unavailable", name, None, "none", m.reason or "not applicable for this asset", "telemetry")
            continue
        if m.mean is None:
            continue
        b.add("measured", f"{name}.mean", m.mean, unit, f"window mean (direction: "
              f"{m.direction})", "telemetry")
        if m.trend_per_minute is not None:
            b.add("derived", f"{name}.trend_per_minute", m.trend_per_minute, "none", "least-squares slope per minute",
                  "cooling_state_estimator")
    inlet = _metric(s, "inlet_temperature_c")
    if inlet is not None and inlet.trend_per_minute is not None:
        b.add("derived", "inlet_temperature_c.trend_per_minute", inlet.trend_per_minute, "°C/min",
              f"inlet slope in the same window (direction: {inlet.direction}); co-movement with workload is "
              "correlation, not proven causation", "cooling_state_estimator")
    b.add("derived", "workload_direction", None, "status", f"combined workload direction: {s.workload_direction}",
          "cooling_state_estimator", direction=s.workload_direction)
    _finding_evidence(b, s, ("WL_",))
    return b.items
