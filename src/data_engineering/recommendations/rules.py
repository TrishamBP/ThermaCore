"""Deterministic evidence requirements for each recommendation type.

For every type the rules decide one of:
  - not applicable (nothing indicates it)                   -> NotApplicable(reason)
  - triggered, requirements met                             -> Candidate(status REQUIRES_REVIEW)
  - triggered, requirements NOT met (missing evidence)      -> Candidate(status INCONCLUSIVE, missing_evidence)
LLM agents cannot create a candidate; they can only support or dispute one (validator.py).
"""

from dataclasses import dataclass, field

from data_engineering.recommendations.evidence import AnalysisContext, energy_measurement_available
from data_engineering.recommendations.schemas import Evidence, NotApplicable


@dataclass
class Candidate:
    type: str
    status: str                       # REQUIRES_REVIEW | INCONCLUSIVE
    severity: str
    title: str
    recommended_action: str
    evidence_ids: list[str]
    missing_evidence: list[str] = field(default_factory=list)
    invalidation: list[str] = field(default_factory=list)
    hypotheses: list[str] = field(default_factory=list)
    uses_model_prediction: bool = False
    inference_based: bool = False
    current_setting: float | None = None
    candidate_setting: float | None = None
    setting_name: str | None = None
    predicted_temperature: float | None = None
    temperature_horizon_minutes: int | None = None
    warnings: list[str] = field(default_factory=list)
    relative_energy: dict | None = None      # relative indicators only (no kW)


def _ids(evidence: list[Evidence], *metrics_or_prefixes: str) -> list[str]:
    return [e.evidence_id for e in evidence if any(e.metric == m or e.metric.startswith(m) for m in metrics_or_prefixes)]


def _findings(ctx: AnalysisContext, prefix: str):
    return [f for f in ctx.snapshot.findings if f.rule_id.startswith(prefix)]


STANDARD_INVALIDATION = [
    "Workload changes materially (direction no longer stable).",
    "Inlet temperature approaches its configured limit.",
    "Airflow, fan or temperature telemetry becomes stale, missing or contradictory.",
]


def cooling_efficiency(ctx, th: list[Evidence]) -> Candidate | NotApplicable:
    s, lim = ctx.snapshot, ctx.cooling_config.limits
    inlet = s.metrics.get("inlet_temperature_c")
    if lim.inlet_temperature_high_c is None or lim.inlet_approach_margin_c is None:
        return NotApplicable(type="COOLING_EFFICIENCY", reason="inlet limit or approach margin is not configured, so "
                                                               "thermal headroom cannot be assessed")
    if inlet is None or inlet.maximum is None:
        return NotApplicable(type="COOLING_EFFICIENCY", reason="no inlet temperature in the window")
    headroom = lim.inlet_temperature_high_c - inlet.maximum
    thermal_warnings = [f for f in s.findings if f.rule_id.startswith(("TH_", "AF_")) and f.severity != "INFO"]
    if s.workload_direction != "stable" or inlet.direction == "increasing" or headroom < lim.inlet_approach_margin_c \
            or thermal_warnings or s.data_quality.status == "INSUFFICIENT":
        return NotApplicable(type="COOLING_EFFICIENCY",
                             reason=f"no opportunity: workload {s.workload_direction}, inlet {inlet.direction}, "
                                    f"headroom {headroom:.2f} °C (needs >= margin {lim.inlet_approach_margin_c} °C), "
                                    f"{len(thermal_warnings)} thermal/airflow warning(s)")
    o = ctx.optimizer
    missing = []
    if o is None:
        missing.append("cooling optimizer result (no trained model or no forecast)")
    elif o["status"] != "RECOMMENDATION_AVAILABLE":
        missing += [f"{r['code']}: {r['detail']}" for r in o["reasons"]]
    if not energy_measurement_available(ctx):
        missing.append("measured cooling-system power (required to estimate any energy reduction)")
    c = Candidate(
        type="COOLING_EFFICIENCY", status="INCONCLUSIVE" if missing else "REQUIRES_REVIEW", severity="INFO",
        title="Potential cooling-efficiency opportunity (requires validation)",
        recommended_action="Evaluate whether a small reduction in cooling (fan speed) is possible while workload "
                           "remains stable. No setting is proposed until a validated response model and measured "
                           "cooling power exist." if missing else
                           "Review the proposed setting with an operator before any controlled test.",
        evidence_ids=_ids(th, "inlet_temperature_c", "inlet_headroom_c", "limits.", "workload_intensity",
                          "cooling_optimizer.status", "inlet_forecast"),
        missing_evidence=list(dict.fromkeys(missing)),
        invalidation=[*STANDARD_INVALIDATION, "The candidate setting is outside the model's validated range."],
        uses_model_prediction=True)
    if o is not None:
        c.current_setting, c.setting_name = o["current_setting"], o["controllable_setting"]
        if o["status"] == "RECOMMENDATION_AVAILABLE":
            c.candidate_setting = o["recommended_setting"]
    return c


def preemptive_cooling(ctx, th: list[Evidence]) -> Candidate | NotApplicable:
    s, lim = ctx.snapshot, ctx.cooling_config.limits
    triggers = [f for f in s.findings if f.rule_id in ("TH_APPROACHING_INLET_LIMIT", "TH_RAPID_INLET_RISE",
                                                       "TH_SUSTAINED_INLET_RISE")]
    f = ctx.forecast
    threshold = None
    if lim.inlet_temperature_high_c is not None:
        threshold = lim.inlet_temperature_high_c - (lim.inlet_approach_margin_c or 0.0)
    worst = max(f["forecasts"], key=lambda x: x["predicted_inlet_temperature_c"]) if f else None
    forecast_approach = worst is not None and threshold is not None and \
        worst["predicted_inlet_temperature_c"] >= threshold
    if not triggers and not forecast_approach:
        return NotApplicable(type="PREEMPTIVE_COOLING",
                             reason="no rising-inlet or approaching-limit finding and no forecast near the limit")
    missing, warnings = [], []
    if lim.inlet_temperature_high_c is None:
        missing.append("configured inlet temperature limit")
    if f is None:
        missing.append(f"inlet forecast ({ctx.forecast_error})")
    else:
        if f["extrapolated_features"]:
            missing.append("forecast within the training data range (inputs are extrapolated)")
        if threshold is not None and not forecast_approach:
            missing.append(f"forecast agreement: measured trend indicates a rise but the {worst['horizon_minutes']}-"
                           f"minute forecast {worst['predicted_inlet_temperature_c']:.2f} °C stays below "
                           f"{threshold:.2f} °C (limit minus margin)")
        skill = ((ctx.model_summary or {}).get("test_rmse_skill_vs_persistence") or {}).get(
            str(worst["horizon_minutes"])) if worst else None
        if skill is not None and skill <= 0:
            warnings.append(f"the forecaster does not beat persistence offline at {worst['horizon_minutes']} min "
                            f"(RMSE skill {skill:+.1%})")
    severity = "CRITICAL" if any(x.severity == "CRITICAL" for x in triggers) else "WARNING"
    c = Candidate(
        type="PREEMPTIVE_COOLING", status="INCONCLUSIVE" if missing else "REQUIRES_REVIEW", severity=severity,
        title="Inlet temperature approaching its limit: evaluate additional cooling",
        recommended_action="Evaluate additional cooling capacity or workload placement for this asset before the "
                           "inlet limit is reached. No specific setting is proposed: the response of inlet "
                           "temperature to cooling settings is not validated.",
        evidence_ids=_ids(th, "inlet_temperature_c", "inlet_headroom_c", "limits.", "inlet_forecast",
                          "TH_APPROACHING_INLET_LIMIT", "TH_RAPID_INLET_RISE", "TH_SUSTAINED_INLET_RISE",
                          "workload_intensity"),
        missing_evidence=missing,
        invalidation=["Inlet temperature stops rising or falls well below the limit minus margin.",
                      "Forecast inputs become stale or extrapolated.",
                      "Workload drops materially."],
        uses_model_prediction=f is not None, inference_based=True, warnings=warnings)
    if worst is not None:
        c.predicted_temperature, c.temperature_horizon_minutes = worst["predicted_inlet_temperature_c"], \
            worst["horizon_minutes"]
    return c


def airflow_investigation(ctx, an: list[Evidence]) -> Candidate | NotApplicable:
    af = _findings(ctx, "AF_")
    if not af:
        return NotApplicable(type="AIRFLOW_INVESTIGATION", reason="no airflow/fan finding in the current window")
    dq = ctx.snapshot.data_quality
    missing = [f"usable {m} readings" for m in ("fan_speed_rpm", "airflow_cfm", "inlet_temperature_c")
               if m in dq.missing_fields]
    return Candidate(
        type="AIRFLOW_INVESTIGATION", status="INCONCLUSIVE" if missing else "REQUIRES_REVIEW", severity="WARNING",
        title="Abnormal airflow or fan behaviour: investigate",
        recommended_action="Inspect the airflow path and fan telemetry of this asset (filters, obstructions, fan and "
                           "airflow sensors). The finding is an observed pattern; its cause is not established.",
        evidence_ids=_ids(an, "AF_", "airflow_per_fan_rpm", "temperature_delta_c", "data_quality.status"),
        missing_evidence=missing,
        invalidation=["Airflow and fan readings return to their usual relationship.",
                      "Fan or airflow telemetry is shown to be faulty (then treat as a sensor issue)."],
        hypotheses=["partially blocked filter or obstructed airflow path", "fan or airflow sensor error",
                    "fan control behaving differently from usual"],
        inference_based=any(f.kind == "inference" for f in af))


def energy_waste(ctx, en: list[Evidence]) -> Candidate | NotApplicable:
    """Fan over-speed vs comparable peers (relative indicator; no kW: no cooling power is measured)."""
    p = ctx.fan_peer
    no_power = "; no measured cooling power exists (power_consumption_kw is server IT power, not cooling power)"
    if p is None or p["status"] in ("INSUFFICIENT_PEERS", "INSUFFICIENT_DATA"):
        reason = "; ".join(p["reasons"]) if p and p["reasons"] else "no fan peer comparison"
        return NotApplicable(type="ENERGY_WASTE", reason=f"cannot screen for fan over-speed: {reason}{no_power}")
    if p["status"] != "ELEVATED":
        return NotApplicable(type="ENERGY_WASTE", reason=f"fan speed {p['status'].lower().replace('_', ' ')} of "
                                                         f"comparable peers{no_power}")
    s, missing, warnings = ctx.snapshot, [], []
    need = ctx.workflow.min_elevated_windows
    if p["persistence"]["windows_elevated"] < need:
        missing.append(f"persistence: elevated in {p['persistence']['windows_elevated']} compared window(s), needs "
                       f">= {need}")
    thermal = [f.rule_id for f in s.findings if f.rule_id.startswith(("TH_", "AF_")) and f.severity != "INFO"]
    if thermal:
        missing.append(f"no thermal or airflow warnings (present: {', '.join(thermal)}); a higher fan speed may be "
                       "warranted")
    if s.data_quality.status != "OK":
        warnings.append(f"telemetry data quality is {s.data_quality.status}")
    rel = {"basis": "fan speed vs comparable peers (same " + "/".join(p["group"]) + ", similar workload and inlet)",
           "asset_fan_rpm": p["asset_fan_rpm"], "peer_median_fan_rpm": p["peer_median_fan_rpm"],
           "excess_fraction": p["excess_fraction"], "relative_fan_power": p["relative_fan_power"],
           "reference_windows": p["reference_windows"], "reference_assets": p["reference_assets"],
           "window": p["window"], "persistence": p["persistence"], "kw": None,
           "limitations": p["limitations"]}
    return Candidate(
        type="ENERGY_WASTE", status="INCONCLUSIVE" if missing else "REQUIRES_REVIEW", severity="INFO",
        title="Fan running faster than comparable peers: possible energy waste to investigate",
        recommended_action="Investigate why this server's fan runs faster than comparable servers under similar "
                           "workload and inlet temperature (fan curve or firmware setting, fan or sensor fault, "
                           "airflow restriction, hardware difference). Do not lower the fan speed without a "
                           "supervised test: its effect on inlet temperature is not validated.",
        evidence_ids=_ids(en, "fan_peer_comparison", "fan_speed_rpm", "fan_power", "measured_cooling_power"),
        missing_evidence=missing,
        invalidation=["The fan speed returns to the peer range at similar workload and inlet temperature.",
                      "Workload or inlet temperature diverges from the compared peers.",
                      "The difference is explained by different hardware or a deliberate fan policy."],
        hypotheses=["fan curve / firmware setting differs from peers", "fan or tachometer sensor error",
                    "airflow restriction making the fan controller compensate", "different fan hardware"],
        inference_based=True, warnings=warnings, relative_energy=rel)


def maintenance_inspection(ctx, an: list[Evidence]) -> Candidate | NotApplicable:
    need = ctx.workflow.maintenance_min_persistent_windows
    persistent = [e for e in an if e.metric.endswith(".persistent_windows")
                  and e.details.get("consecutive_windows", 0) >= need]
    if not persistent:
        return NotApplicable(type="MAINTENANCE_INSPECTION",
                             reason=f"no anomaly persisted for {need} consecutive windows")
    poor = [e for e in persistent if any(q != "OK" for q in e.details.get("history_quality", []))]
    rules_ = ", ".join(e.details["rule_id"] for e in persistent)
    return Candidate(
        type="MAINTENANCE_INSPECTION", status="INCONCLUSIVE" if poor else "REQUIRES_REVIEW", severity="WARNING",
        title="Persistent cooling anomaly: schedule an inspection",
        recommended_action=f"Schedule an inspection of this asset's cooling path: {rules_} persisted for at least "
                           f"{need} consecutive windows. No component failure is claimed.",
        evidence_ids=[e.evidence_id for e in persistent] + _ids(an, "AF_", "TH_"),
        missing_evidence=["good data quality in every persistent window"] if poor else [],
        invalidation=["The anomaly does not recur in the next windows.",
                      "The pattern is explained by a workload change or a sensor fault."],
        hypotheses=["degrading fan or airflow component", "recurring obstruction", "sensor drift"],
        inference_based=True)


SERIOUS_DQ = ("DQ_NO_READINGS", "DQ_INSUFFICIENT_POINTS", "DQ_STALE_ASSET", "DQ_LOW_COVERAGE", "DQ_MISSING_READINGS",
              "DQ_TIMESTAMP_GAPS", "DQ_IRREGULAR_INTERVALS", "DQ_PIPELINE_STALE")


def sensor_data_quality(ctx, an: list[Evidence]) -> Candidate | NotApplicable:
    dq_findings = [f for f in ctx.snapshot.findings if f.rule_id in SERIOUS_DQ or
                   (f.rule_id == "DQ_DUPLICATE_TIMESTAMPS" and f.severity != "INFO")]
    if ctx.as_of_requested is not None:      # historical analysis: a stopped stream (wall clock) is expected
        dq_findings = [f for f in dq_findings if f.rule_id != "DQ_PIPELINE_STALE"]
    contradictory = [e for e in an if e.details.get("contradictory")]
    if not dq_findings and not contradictory:
        return NotApplicable(type="SENSOR_DATA_QUALITY", reason="no stale, missing, duplicate-conflict or "
                                                                "contradictory telemetry detected")
    only_pipeline = all(f.rule_id == "DQ_PIPELINE_STALE" for f in dq_findings) and not contradictory
    return Candidate(
        type="SENSOR_DATA_QUALITY", status="REQUIRES_REVIEW", severity="WARNING",
        title="Telemetry quality problem: check sensors and the data pipeline",
        recommended_action="Check the telemetry pipeline (producer/consumer) for this asset." if only_pipeline else
                           "Check this asset's sensors and the telemetry pipeline: readings are stale, missing, "
                           "irregular, conflicting or physically contradictory.",
        evidence_ids=_ids(an, "DQ_", "data_quality.", "pipeline.", "forecast_inputs") +
        [e.evidence_id for e in contradictory],
        invalidation=["Readings arrive on time, complete and consistent again."],
        hypotheses=["stopped producer or consumer", "sensor or network fault"] if not only_pipeline else
                   ["stopped producer or consumer"])


def build_candidates(ctx: AnalysisContext, evidence: dict[str, list[Evidence]]):
    """-> (candidates, not_applicable)."""
    th, en, an = evidence.get("thermal", []), evidence.get("energy", []), evidence.get("anomaly", [])
    out, na = [], []
    for fn, ev in ((preemptive_cooling, th), (airflow_investigation, an), (sensor_data_quality, an),
                   (maintenance_inspection, an), (energy_waste, en), (cooling_efficiency, th)):
        result = fn(ctx, ev)
        (na if isinstance(result, NotApplicable) else out).append(result)
    return out, na
