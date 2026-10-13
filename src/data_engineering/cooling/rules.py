"""Explicit, deterministic classification rules.

Every rule has an ID, a fixed resulting state and severity, and records its evidence.
`kind` separates what was measured from what was inferred:
  observation  - a measured value compared with a configured limit
  inference    - a rule over trends/relationships; suggests a condition, does not confirm it
  data_quality - the telemetry itself is stale, sparse, irregular or duplicated

Overall state (deterministic precedence):
  1. INSUFFICIENT_DATA if a DQ_NO_READINGS finding exists (asset scope) or no asset has
     usable readings (data-center scope);
  2. otherwise the state of the most severe finding (CRITICAL > WARNING); ties are broken by
     HIGH_THERMAL_LOAD > RAPID_TEMPERATURE_RISE > APPROACHING_THERMAL_LIMIT >
     POTENTIAL_AIRFLOW_ISSUE > DATA_QUALITY_ISSUE;
  3. no WARNING/CRITICAL finding: LIGHT_LOAD if the light-load rule fired (at data-center scope:
     only if every assessed asset is lightly loaded); else PARTIALLY_ASSESSED if any rule could
     not be evaluated (unset threshold or too few readings); else NORMAL.
Other INFO findings (e.g. workload direction) never change the state. LIGHT_LOAD never hides a
problem: any WARNING/CRITICAL finding outranks it, and rules_not_evaluated is still reported.
"""

from data_engineering.cooling.config import CoolingConfig
from data_engineering.cooling.metrics import PreparedSeries
from data_engineering.cooling.models import Finding, MetricSeries, RuleNotEvaluated

SEVERITY_RANK = {"INFO": 0, "WARNING": 1, "CRITICAL": 2}
STATE_TIEBREAK = ["HIGH_THERMAL_LOAD", "RAPID_TEMPERATURE_RISE", "APPROACHING_THERMAL_LIMIT",
                  "POTENTIAL_AIRFLOW_ISSUE", "DATA_QUALITY_ISSUE", "LIGHT_LOAD", "PARTIALLY_ASSESSED", "NORMAL"]
NOT_A_FAULT = "Rule-based inference from telemetry; not a confirmed hardware fault."


def _r(value, digits=4):
    return None if value is None else round(float(value), digits)


def data_quality_findings(series: PreparedSeries, metrics: dict[str, MetricSeries], config: CoolingConfig,
                          asset_id: str | None, staleness_s: float | None) -> list[Finding]:
    f: list[Finding] = []

    def add(rule_id, state, severity, explanation, **evidence):
        f.append(Finding(rule_id=rule_id, state=state, severity=severity, kind="data_quality",
                         asset_id=asset_id, explanation=explanation, evidence=evidence))

    if series.observed_points == 0:
        add("DQ_NO_READINGS", "INSUFFICIENT_DATA", "WARNING",
            "No observed readings in the window; the state cannot be assessed.",
            present_points=series.present_points, missing_readings=series.missing_readings,
            expected_points=series.expected_points)
        return f
    inlet = metrics.get("inlet_temperature_c")
    if inlet is not None and not inlet.sufficient_points:
        add("DQ_INSUFFICIENT_POINTS", "DATA_QUALITY_ISSUE", "WARNING",
            f"Only {inlet.observed_points} observed inlet temperature readings (minimum {config.min_points}); "
            "trend-based rules were not evaluated.",
            observed_points=inlet.observed_points, min_points=config.min_points)
    if staleness_s is not None and staleness_s > config.stale_after_seconds:
        add("DQ_STALE_ASSET", "DATA_QUALITY_ISSUE", "WARNING",
            f"The newest reading of this asset is {staleness_s:.0f} s older than the evaluation time.",
            staleness_seconds=_r(staleness_s, 1), stale_after_seconds=config.stale_after_seconds)
    if series.coverage < config.min_coverage:
        add("DQ_LOW_COVERAGE", "DATA_QUALITY_ISSUE", "WARNING",
            f"Observed {series.observed_points} of {series.expected_points} expected readings.",
            coverage=_r(series.coverage), min_coverage=config.min_coverage,
            observed_points=series.observed_points, expected_points=series.expected_points)
    if series.missing_fraction > config.max_missing_fraction:
        add("DQ_MISSING_READINGS", "DATA_QUALITY_ISSUE", "WARNING",
            f"{series.missing_readings} of {series.present_points} readings were reported as missing.",
            missing_fraction=_r(series.missing_fraction), max_missing_fraction=config.max_missing_fraction)
    interior = [g for g in series.gaps if g.position == "interior"]
    if interior:
        add("DQ_TIMESTAMP_GAPS", "DATA_QUALITY_ISSUE", "WARNING",
            f"{len(interior)} gap(s) longer than {config.gap_tolerance_factor} x the expected "
            f"{config.expected_interval_seconds} s interval between readings.",
            gaps=[{"start": g.start.isoformat(), "end": g.end.isoformat(), "seconds": g.seconds} for g in interior],
            longest_gap_seconds=max(g.seconds for g in interior))
    if series.irregular_intervals:
        add("DQ_IRREGULAR_INTERVALS", "DATA_QUALITY_ISSUE", "WARNING",
            f"{series.irregular_intervals} interval(s) much shorter than the expected spacing.",
            irregular_intervals=series.irregular_intervals,
            expected_interval_seconds=config.expected_interval_seconds)
    if series.duplicate_timestamps:
        conflicting = series.conflicting_duplicates > 0
        add("DQ_DUPLICATE_TIMESTAMPS", "DATA_QUALITY_ISSUE" if conflicting else "NORMAL",
            "WARNING" if conflicting else "INFO",
            f"{series.duplicate_timestamps} reading(s) shared a timestamp with another; the most recently "
            "ingested one was kept" + (" and the duplicates had different values." if conflicting else "."),
            duplicate_timestamps=series.duplicate_timestamps, conflicting=series.conflicting_duplicates)
    return f


def pipeline_staleness_finding(ingest_age_s: float | None, config: CoolingConfig) -> Finding | None:
    if ingest_age_s is None or ingest_age_s <= config.max_ingest_age_seconds:
        return None
    return Finding(rule_id="DQ_PIPELINE_STALE", state="DATA_QUALITY_ISSUE", severity="WARNING",
                   kind="data_quality",
                   explanation=f"No telemetry was stored for {ingest_age_s:.0f} s (wall clock); the stream may have "
                               "stopped, so the snapshot may not reflect the current state.",
                   evidence={"ingest_age_seconds": _r(ingest_age_s, 1),
                             "max_ingest_age_seconds": config.max_ingest_age_seconds})


def condition_findings(metrics: dict[str, MetricSeries], config: CoolingConfig,
                       asset_id: str | None) -> tuple[list[Finding], list[RuleNotEvaluated]]:
    """Thermal, rapid-rise and airflow rules. Needs metrics computed for one asset."""
    lim = config.limits
    findings: list[Finding] = []
    skipped: list[RuleNotEvaluated] = []
    inlet = metrics["inlet_temperature_c"]
    exhaust = metrics["exhaust_temperature_c"]
    delta = metrics["temperature_delta_c"]
    fan, airflow = metrics["fan_speed_rpm"], metrics["airflow_cfm"]

    def skip(rule_id, reason):
        skipped.append(RuleNotEvaluated(rule_id=rule_id, reason=reason, asset_id=asset_id))

    def add(rule_id, state, severity, kind, explanation, **evidence):
        findings.append(Finding(rule_id=rule_id, state=state, severity=severity, kind=kind, asset_id=asset_id,
                                explanation=explanation, evidence=evidence))

    # --- limits on measured values (observations) ---
    for rule_id, metric, limit_name in (("TH_INLET_ABOVE_LIMIT", inlet, "inlet_temperature_high_c"),
                                        ("TH_EXHAUST_ABOVE_LIMIT", exhaust, "exhaust_temperature_high_c"),
                                        ("TH_DELTA_ABOVE_LIMIT", delta, "temperature_delta_high_c")):
        limit = getattr(lim, limit_name)
        if limit is None:
            skip(rule_id, f"limits.{limit_name} is not configured")
        elif metric.latest is None:
            skip(rule_id, "no observed value in the window")
        elif metric.latest >= limit:
            severity = "WARNING"
            critical = lim.inlet_temperature_critical_c if rule_id == "TH_INLET_ABOVE_LIMIT" else None
            if critical is not None and metric.latest >= critical:
                severity = "CRITICAL"
            add(rule_id, "HIGH_THERMAL_LOAD", severity, "observation",
                f"Latest value {metric.latest:.3f} {metric.unit} is at or above the configured limit {limit} "
                f"{metric.unit}" + (f" (critical limit {critical})." if severity == "CRITICAL" else "."),
                latest=_r(metric.latest), latest_timestamp=metric.latest_timestamp.isoformat(), limit=limit,
                critical_limit=critical, window_maximum=_r(metric.maximum))
    if lim.inlet_temperature_critical_c is None:
        skip("TH_INLET_CRITICAL_TIER", "limits.inlet_temperature_critical_c is not configured")

    # --- temperature trends (inferences) ---
    for rule_id, field_name, limit_name, state, label in (
            ("TH_SUSTAINED_INLET_RISE", "trend_per_minute", "sustained_inlet_rise_c_per_minute",
             "HIGH_THERMAL_LOAD", "over the window"),
            ("TH_RAPID_INLET_RISE", "recent_rate_per_minute", "rapid_inlet_rise_c_per_minute",
             "RAPID_TEMPERATURE_RISE", f"over the last {config.recent_minutes} minutes")):
        limit = getattr(lim, limit_name)
        rate = getattr(inlet, field_name)
        if limit is None:
            skip(rule_id, f"limits.{limit_name} is not configured")
        elif rate is None:
            skip(rule_id, f"inlet temperature rate unavailable (needs >= {config.min_points} readings, "
                          f">= 3 in the recent sub-window)")
        elif rate >= limit:
            add(rule_id, state, "WARNING", "inference",
                f"Inlet temperature is rising {rate:.4f} deg C/min {label} (threshold {limit} deg C/min).",
                rate_c_per_minute=_r(rate), threshold_c_per_minute=limit, latest=_r(inlet.latest),
                change_over_window=_r(inlet.change), elapsed_minutes=_r(inlet.change_elapsed_minutes, 2))

    # --- approaching the inlet limit: within a margin (observation) or projected (inference) ---
    limit = lim.inlet_temperature_high_c
    margin, horizon = lim.inlet_approach_margin_c, lim.inlet_approach_horizon_minutes
    if limit is None:
        skip("TH_APPROACHING_INLET_LIMIT", "limits.inlet_temperature_high_c is not configured")
    elif margin is None and horizon is None:
        skip("TH_APPROACHING_INLET_LIMIT", "neither limits.inlet_approach_margin_c nor "
                                          "limits.inlet_approach_horizon_minutes is configured")
    elif inlet.latest is None:
        skip("TH_APPROACHING_INLET_LIMIT", "no observed inlet temperature in the window")
    elif inlet.latest < limit:            # at/above the limit is TH_INLET_ABOVE_LIMIT, not 'approaching'
        headroom = limit - inlet.latest
        # Current rate: the recent sub-window rate, else the whole-window trend.
        rate = inlet.recent_rate_per_minute if inlet.recent_rate_per_minute is not None else inlet.trend_per_minute
        minutes_to_limit = headroom / rate if rate is not None and rate > 0 else None
        by_margin = margin is not None and headroom <= margin
        by_projection = horizon is not None and minutes_to_limit is not None and minutes_to_limit <= horizon
        if horizon is not None and rate is None and not by_margin:
            skip("TH_APPROACHING_INLET_LIMIT", "projection unavailable (too few inlet readings) and the margin "
                                              "condition does not hold")
        if by_margin or by_projection:
            triggers = [t for t, on in (("margin", by_margin), ("projection", by_projection)) if on]
            parts = []
            if by_margin:
                parts.append(f"inlet {inlet.latest:.2f} deg C is within {margin} deg C of the {limit} deg C limit")
            if by_projection:
                parts.append(f"at the current rate of {rate:.3f} deg C/min it would reach the limit in about "
                             f"{minutes_to_limit:.1f} min (horizon {horizon} min)")
            add("TH_APPROACHING_INLET_LIMIT", "APPROACHING_THERMAL_LIMIT", "WARNING",
                "inference" if by_projection else "observation",
                "Approaching the inlet temperature limit: " + "; ".join(parts) + "."
                + (" The projection assumes the current rate continues; it is not a forecast." if by_projection else ""),
                triggers=triggers, latest=_r(inlet.latest), limit=limit, headroom_c=_r(headroom),
                margin_c=margin, rate_c_per_minute=_r(rate), minutes_to_limit=_r(minutes_to_limit, 1),
                horizon_minutes=horizon)

    # --- airflow (inferences; never a confirmed fault) ---
    frac = lim.airflow_drop_fraction
    if frac is None:
        skip("AF_AIRFLOW_DROP_WITH_RISING_TEMPERATURE", "limits.airflow_drop_fraction is not configured")
    elif not (airflow.sufficient_points and inlet.sufficient_points) or not airflow.first:
        skip("AF_AIRFLOW_DROP_WITH_RISING_TEMPERATURE", "airflow or inlet temperature has too few readings")
    else:
        drop = -airflow.change / airflow.first
        rising = inlet.direction == "increasing" or delta.direction == "increasing"
        if drop >= frac and rising:
            add("AF_AIRFLOW_DROP_WITH_RISING_TEMPERATURE", "POTENTIAL_AIRFLOW_ISSUE", "WARNING", "inference",
                f"Airflow fell {drop:.1%} over the window while temperature rose. " + NOT_A_FAULT,
                airflow_first=_r(airflow.first), airflow_latest=_r(airflow.latest), airflow_drop_fraction=_r(drop),
                threshold_fraction=frac, inlet_direction=inlet.direction, delta_direction=delta.direction)
    frac = lim.fan_airflow_divergence_fraction
    if frac is None:
        skip("AF_FAN_AIRFLOW_DIVERGENCE", "limits.fan_airflow_divergence_fraction is not configured")
    elif not (fan.sufficient_points and airflow.sufficient_points) or not fan.first:
        skip("AF_FAN_AIRFLOW_DIVERGENCE", "fan speed or airflow has too few readings")
    else:
        rise = fan.change / fan.first
        if rise >= frac and airflow.change is not None and airflow.change <= 0:
            add("AF_FAN_AIRFLOW_DIVERGENCE", "POTENTIAL_AIRFLOW_ISSUE", "WARNING", "inference",
                f"Fan speed rose {rise:.1%} over the window but airflow did not increase "
                "(in this dataset airflow normally tracks fan speed). " + NOT_A_FAULT,
                fan_first=_r(fan.first), fan_latest=_r(fan.latest), fan_rise_fraction=_r(rise),
                threshold_fraction=frac, airflow_change=_r(airflow.change))
    return findings, skipped


def light_load_finding(metrics: dict[str, MetricSeries], config: CoolingConfig,
                       asset_id: str | None) -> tuple[Finding | None, RuleNotEvaluated | None]:
    """WL_LIGHT_LOAD: window-mean CPU below its threshold; on GPU servers GPU must be below too."""
    lim = config.limits
    cpu, gpu = metrics["cpu_utilization"], metrics["gpu_utilization"]

    def skip(reason):
        return None, RuleNotEvaluated(rule_id="WL_LIGHT_LOAD", reason=reason, asset_id=asset_id)

    if lim.light_load_cpu_utilization_pct is None:
        return skip("limits.light_load_cpu_utilization_pct is not configured")
    if not cpu.sufficient_points or cpu.mean is None:
        return skip(f"cpu_utilization has too few readings (needs >= {config.min_points})")
    evidence = {"cpu_mean": _r(cpu.mean), "cpu_threshold": lim.light_load_cpu_utilization_pct}
    light = cpu.mean < lim.light_load_cpu_utilization_pct
    if gpu.applicable:
        if lim.light_load_gpu_utilization_pct is None:
            return skip("GPU server: limits.light_load_gpu_utilization_pct is not configured")
        if not gpu.sufficient_points or gpu.mean is None:
            return skip(f"gpu_utilization has too few readings (needs >= {config.min_points})")
        evidence.update(gpu_mean=_r(gpu.mean), gpu_threshold=lim.light_load_gpu_utilization_pct)
        light = light and gpu.mean < lim.light_load_gpu_utilization_pct
    if not light:
        return None, None
    what = "CPU" + (" and GPU" if gpu.applicable else "")
    return Finding(rule_id="WL_LIGHT_LOAD", state="LIGHT_LOAD", severity="INFO", kind="observation",
                   asset_id=asset_id, evidence=evidence,
                   explanation=f"Light load: {config.window_minutes}-minute mean {what} utilization below the "
                               "configured threshold."), None


def workload_direction(metrics: dict[str, MetricSeries]) -> str:
    """Combine available workload metrics: agreeing non-stable directions win, disagreement is 'mixed'."""
    directions = [metrics[m].direction for m in ("cpu_utilization", "gpu_utilization", "workload_intensity")
                  if m in metrics and metrics[m].applicable and metrics[m].direction is not None]
    if not directions:
        return "unknown"
    moving = {d for d in directions if d != "stable"}
    if not moving:
        return "stable"
    return moving.pop() if len(moving) == 1 else "mixed"


def workload_finding(direction: str, metrics: dict[str, MetricSeries], asset_id: str | None) -> Finding | None:
    if direction not in ("increasing", "decreasing"):
        return None
    evidence = {m: {"change": _r(metrics[m].change), "direction": metrics[m].direction}
                for m in ("cpu_utilization", "gpu_utilization", "workload_intensity")
                if m in metrics and metrics[m].applicable and metrics[m].direction is not None}
    return Finding(rule_id=f"WL_{direction.upper()}", state="NORMAL", severity="INFO", kind="observation",
                   asset_id=asset_id, explanation=f"Workload is broadly {direction} over the window.",
                   evidence=evidence)


def classify(findings: list[Finding], not_evaluated: list[RuleNotEvaluated],
             insufficient: bool = False, light_load: bool | None = None) -> tuple[str, str]:
    """(state, severity) by the precedence documented at the top of this module."""
    if insufficient:
        return "INSUFFICIENT_DATA", "WARNING"
    # Below the scope that has no data (e.g. one silent asset in a data-center snapshot), a
    # no-readings finding is a data-quality problem, not a reason to drop the whole assessment.
    states = [("DATA_QUALITY_ISSUE" if f.state == "INSUFFICIENT_DATA" else f.state, f.severity) for f in findings]
    significant = [(s, sev) for s, sev in states if SEVERITY_RANK[sev] > 0 and s != "NORMAL"]
    if significant:
        top = max(SEVERITY_RANK[sev] for _, sev in significant)
        candidates = {s for s, sev in significant if SEVERITY_RANK[sev] == top}
        state = min(candidates, key=STATE_TIEBREAK.index)
        severity = "CRITICAL" if top == 2 else "WARNING"
        return state, severity
    if light_load is None:                 # asset scope: the asset's own light-load finding
        light_load = any(f.rule_id == "WL_LIGHT_LOAD" for f in findings)
    if light_load:
        return "LIGHT_LOAD", "INFO"
    if not_evaluated:
        return "PARTIALLY_ASSESSED", "INFO"
    return "NORMAL", "INFO"
