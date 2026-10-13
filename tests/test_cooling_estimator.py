"""Cooling state estimator end to end over a real SQLite store with controlled readings."""

import pytest

from cooling_helpers import T0, all_limits_config, config, event, seed, series, wall_clock
from data_engineering.cooling.config import CoolingConfigError, load_cooling_config
from data_engineering.cooling.estimator import CoolingStateEstimator, UnknownScopeError

MINUTES = range(0, 15)          # 15 readings, 12:00..12:14 -> window (11:59, 12:14]


def run(store, cfg=None, clock=None, **kwargs):
    return CoolingStateEstimator(store, cfg or all_limits_config(), clock or wall_clock()).evaluate(**kwargs)


def rule_ids(snapshot):
    return sorted(f.rule_id for f in snapshot.findings)


# 1. normal, stable telemetry -------------------------------------------------------------------

def test_stable_telemetry_is_normal(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES))
    s = run(sqlite_store, asset_id="A1")
    assert (s.state, s.severity, s.findings, s.rules_not_evaluated) == ("NORMAL", "INFO", [], [])
    inlet = s.metrics["inlet_temperature_c"]
    assert (inlet.latest, inlet.mean, inlet.minimum, inlet.maximum, inlet.std_dev) == (21.0, 21.0, 21.0, 21.0, 0.0)
    assert inlet.trend_per_minute == 0 and inlet.direction == "stable" and inlet.unit == "degC"
    assert s.metrics["temperature_delta_c"].latest == 9.0 and s.metrics["temperature_delta_c"].source == "derived"
    assert s.metrics["power_consumption_kw"].unit == "kW" and s.metrics["pue"].latest == 1.1
    assert s.as_of == T0.replace(minute=14) and s.window_minutes == 15
    assert s.data_quality.status == "OK" and s.data_quality.coverage == 1.0
    s.model_dump_json()                                         # JSON-serializable


def test_unset_limits_are_never_reported_as_normal(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES))
    s = run(sqlite_store, cfg=config(), asset_id="A1")          # shipped config: all limits unset
    assert s.state == "PARTIALLY_ASSESSED" and s.findings == []
    reasons = {r.rule_id: r.reason for r in s.rules_not_evaluated}
    assert reasons["TH_INLET_ABOVE_LIMIT"] == "limits.inlet_temperature_high_c is not configured"
    assert {"TH_RAPID_INLET_RISE", "AF_FAN_AIRFLOW_DIVERGENCE"} <= set(reasons)


# 2./3. rising and elevated temperature -----------------------------------------------------------

def test_rapid_rise_in_recent_minutes(sqlite_store):
    # flat at 21 for 10 minutes, then +0.5 degC/min: recent rate 0.5 >= 0.3 (sustained limit raised out of the way)
    seed(sqlite_store, series("A1", MINUTES, inlet_temperature_c=lambda m: 21.0 + max(0, m - 10) * 0.5))
    s = run(sqlite_store, cfg=all_limits_config(sustained_inlet_rise_c_per_minute=1.0), asset_id="A1")
    assert rule_ids(s) == ["TH_RAPID_INLET_RISE"] and s.state == "RAPID_TEMPERATURE_RISE"
    f = s.findings[0]
    assert f.kind == "inference" and f.evidence["rate_c_per_minute"] == pytest.approx(0.5)


def test_sustained_rise_and_precedence_over_rapid_rise(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES, inlet_temperature_c=lambda m: 20.0 + 0.4 * m))
    s = run(sqlite_store, asset_id="A1")
    assert rule_ids(s) == ["TH_RAPID_INLET_RISE", "TH_SUSTAINED_INLET_RISE"]
    assert s.state == "HIGH_THERMAL_LOAD" and s.severity == "WARNING"   # tie broken by documented precedence
    assert s.metrics["inlet_temperature_c"].trend_per_minute == pytest.approx(0.4)
    assert s.metrics["inlet_temperature_c"].change == pytest.approx(5.6)


def test_inlet_above_configured_limit_and_critical_tier(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES, inlet_temperature_c=28.0, exhaust_temperature_c=37.0))
    s = run(sqlite_store, asset_id="A1")
    assert rule_ids(s) == ["TH_INLET_ABOVE_LIMIT"] and (s.state, s.severity) == ("HIGH_THERMAL_LOAD", "WARNING")
    assert s.findings[0].kind == "observation" and s.findings[0].evidence["limit"] == 27.0
    critical = run(sqlite_store, cfg=all_limits_config(inlet_temperature_critical_c=27.5), asset_id="A1")
    assert critical.severity == "CRITICAL" and critical.findings[0].severity == "CRITICAL"


def test_exhaust_and_delta_limits(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES, exhaust_temperature_c=46.0))
    s = run(sqlite_store, asset_id="A1")
    assert rule_ids(s) == ["TH_DELTA_ABOVE_LIMIT", "TH_EXHAUST_ABOVE_LIMIT"]
    assert s.metrics["temperature_delta_c"].latest == 25.0


# 4. workload ---------------------------------------------------------------------------------------

def test_increasing_and_decreasing_workload(sqlite_store):
    seed(sqlite_store, series("UP", MINUTES, cpu_utilization=lambda m: 30 + 2 * m, workload_intensity=lambda m: 0.3 + 0.02 * m))
    seed(sqlite_store, series("DOWN", MINUTES, cpu_utilization=lambda m: 80 - 2 * m))
    up, down = run(sqlite_store, asset_id="UP"), run(sqlite_store, asset_id="DOWN")
    assert up.workload_direction == "increasing" and rule_ids(up) == ["WL_INCREASING"]
    assert down.workload_direction == "decreasing" and rule_ids(down) == ["WL_DECREASING"]
    assert up.state == "NORMAL" and up.findings[0].severity == "INFO"          # context, not a problem
    assert up.metrics["cpu_utilization"].change == pytest.approx(28.0)


def test_gpu_is_not_applicable_for_non_gpu_assets(sqlite_store):
    seed(sqlite_store, series("CPU1", MINUTES, gpu_utilization=0.0))
    seed(sqlite_store, [event("GPU1", m, asset_type="GPU", gpu_utilization=40.0 + m) for m in MINUTES])
    cpu, gpu = run(sqlite_store, asset_id="CPU1"), run(sqlite_store, asset_id="GPU1")
    assert cpu.metrics["gpu_utilization"].applicable is False and cpu.metrics["gpu_utilization"].latest is None
    assert "has no GPU" in cpu.metrics["gpu_utilization"].reason
    assert gpu.metrics["gpu_utilization"].latest == 54.0 and gpu.metrics["gpu_utilization"].direction == "increasing"
    assert gpu.workload_direction == "increasing"          # GPU counts for GPU assets


# 5. airflow ----------------------------------------------------------------------------------------

def test_airflow_drop_with_rising_temperature(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES, airflow_cfm=lambda m: 200 - 3 * m,
                              inlet_temperature_c=lambda m: 21 + 0.05 * m))
    s = run(sqlite_store, asset_id="A1")
    assert rule_ids(s) == ["AF_AIRFLOW_DROP_WITH_RISING_TEMPERATURE"]
    assert s.state == "POTENTIAL_AIRFLOW_ISSUE" and s.findings[0].kind == "inference"
    assert "not a confirmed hardware fault" in s.findings[0].explanation
    assert s.findings[0].evidence["airflow_drop_fraction"] == pytest.approx(42 / 200)


def test_lower_airflow_alone_is_not_flagged(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES, airflow_cfm=lambda m: 200 - 3 * m))     # temperature stable
    assert rule_ids(run(sqlite_store, asset_id="A1")) == []


def test_fan_up_but_airflow_flat(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES, fan_speed_rpm=lambda m: 3000 + 40 * m))
    s = run(sqlite_store, asset_id="A1")
    assert rule_ids(s) == ["AF_FAN_AIRFLOW_DIVERGENCE"] and s.state == "POTENTIAL_AIRFLOW_ISSUE"


# 6. missing metrics ----------------------------------------------------------------------------------

def test_missing_cpu_and_unavailable_cooling_efficiency(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES, cpu_utilization=None))
    s = run(sqlite_store, asset_id="A1")
    cpu = s.metrics["cpu_utilization"]
    assert cpu.latest is None and cpu.mean is None and cpu.observed_points == 0     # never 0
    assert "cpu_utilization" in s.data_quality.missing_fields
    ce = s.metrics["cooling_efficiency"]
    assert ce.available is False and "not in the telemetry schema" in ce.reason
    assert s.data_quality.unavailable_fields == ["cooling_efficiency"]


def test_explicit_missing_readings_are_counted(sqlite_store):
    events = [event("A1", m, status="missing") if m % 3 == 0 else event("A1", m) for m in MINUTES]
    seed(sqlite_store, events)
    s = run(sqlite_store, asset_id="A1")
    assert s.data_quality.missing_readings == 5 and s.metrics["inlet_temperature_c"].observed_points == 10
    assert "DQ_MISSING_READINGS" in rule_ids(s) and "DQ_LOW_COVERAGE" in rule_ids(s)
    assert s.state == "DATA_QUALITY_ISSUE"


# 7./8. stale, gaps, duplicates ---------------------------------------------------------------------

def test_stale_asset_and_stopped_pipeline(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES))
    seed(sqlite_store, series("LAGGING", range(0, 8)))        # stopped 7 minutes before the others
    lag = run(sqlite_store, asset_id="LAGGING")
    assert "DQ_STALE_ASSET" in rule_ids(lag) and lag.data_quality.stale
    stopped = run(sqlite_store, asset_id="A1", clock=wall_clock(3600))
    assert rule_ids(stopped) == ["DQ_PIPELINE_STALE"] and stopped.data_quality.ingest_age_seconds >= 3600


def test_irregular_gaps_are_reported(sqlite_store):
    seed(sqlite_store, series("A1", [0, 1, 2, 3, 4, 9, 10, 11, 12, 13, 14]))
    s = run(sqlite_store, asset_id="A1")
    assert "DQ_TIMESTAMP_GAPS" in rule_ids(s)
    assert [(g.position, g.seconds) for g in s.data_quality.gaps] == [("interior", 300.0)]


def test_duplicate_timestamps(sqlite_store):
    events = series("A1", MINUTES) + [event("A1", 7, slot=9_007, inlet_temperature_c=21.0)]  # same values
    seed(sqlite_store, events)
    s = run(sqlite_store, asset_id="A1")
    dup = [f for f in s.findings if f.rule_id == "DQ_DUPLICATE_TIMESTAMPS"][0]
    assert dup.severity == "INFO" and s.state == "NORMAL" and s.data_quality.duplicate_timestamps == 1
    seed(sqlite_store, [event("A1", 8, slot=9_008, inlet_temperature_c=25.0)])            # conflicting value
    s = run(sqlite_store, asset_id="A1")
    dup = [f for f in s.findings if f.rule_id == "DQ_DUPLICATE_TIMESTAMPS"][0]
    assert dup.severity == "WARNING" and s.data_quality.conflicting_duplicates == 1


def test_missing_intervals_reduce_coverage(sqlite_store):
    seed(sqlite_store, series("A1", [0, 2, 4, 6, 8, 10, 12, 14]))      # every other minute absent
    s = run(sqlite_store, asset_id="A1")
    assert s.data_quality.coverage == pytest.approx(8 / 15, abs=1e-4)   # snapshot rounds to 4 decimals
    assert {"DQ_LOW_COVERAGE", "DQ_TIMESTAMP_GAPS"} <= set(rule_ids(s))


def test_too_few_points_skip_trend_rules(sqlite_store):
    seed(sqlite_store, series("A1", [12, 13, 14]))
    s = run(sqlite_store, asset_id="A1")
    assert "DQ_INSUFFICIENT_POINTS" in rule_ids(s)
    assert {r.rule_id for r in s.rules_not_evaluated} >= {"TH_SUSTAINED_INLET_RISE", "TH_RAPID_INLET_RISE"}
    assert s.metrics["inlet_temperature_c"].latest == 21.0          # point values still reported


# 9. empty results ----------------------------------------------------------------------------------

def test_empty_store_is_insufficient_data(sqlite_store):
    s = run(sqlite_store)
    assert (s.state, rule_ids(s), s.data_quality.status) == ("INSUFFICIENT_DATA", ["DQ_NO_TELEMETRY"], "INSUFFICIENT")


def test_window_without_readings_is_insufficient_data(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES))
    s = run(sqlite_store, asset_id="A1", as_of=T0.replace(hour=18))
    assert s.state == "INSUFFICIENT_DATA" and "DQ_NO_READINGS" in rule_ids(s)


def test_unknown_asset_or_run(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES))
    with pytest.raises(UnknownScopeError):
        run(sqlite_store, asset_id="NOPE")
    with pytest.raises(UnknownScopeError):
        run(sqlite_store, run_id="sim-other")


# 10. multiple assets and runs ------------------------------------------------------------------------

def test_data_center_scope_never_mixes_assets(sqlite_store):
    seed(sqlite_store, series("COOL", MINUTES, inlet_temperature_c=20.0, power_consumption_kw=80.0))
    seed(sqlite_store, series("HOT", MINUTES, inlet_temperature_c=28.0, power_consumption_kw=120.0))
    s = run(sqlite_store)
    inlet = s.metrics["inlet_temperature_c"]
    assert (inlet.assets_reporting, inlet.mean_of_latest, inlet.max_of_latest, inlet.max_asset_id) == (2, 24.0, 28.0, "HOT")
    assert s.metrics["power_consumption_kw"].sum_of_latest == 200.0
    assert s.metrics["inlet_temperature_c"].sum_of_latest is None        # temperatures are not additive
    assert s.state == "HIGH_THERMAL_LOAD" and [f.asset_id for f in s.findings] == ["HOT"]
    assert {a.asset_id: a.state for a in s.assets} == {"COOL": "NORMAL", "HOT": "HIGH_THERMAL_LOAD"}
    # mixing both assets into one series would have produced a 20 <-> 28 zig-zag; per-asset trends are flat
    assert inlet.max_trend_per_minute == 0


def test_one_silent_asset_degrades_but_does_not_void_the_data_center(sqlite_store):
    seed(sqlite_store, series("A1", range(0, 31)))                       # keeps reporting until 12:30
    seed(sqlite_store, series("OLD", [0]))
    s = run(sqlite_store, as_of=T0.replace(minute=30))                    # OLD has nothing in (12:15, 12:30]
    states = {a.asset_id: a.state for a in s.assets}
    assert states == {"A1": "NORMAL", "OLD": "INSUFFICIENT_DATA"} and s.state == "DATA_QUALITY_ISSUE"


def test_runs_are_kept_apart(sqlite_store):
    seed(sqlite_store, series("A1", MINUTES))
    seed(sqlite_store, [event("A1", m, run="sim-other", inlet_temperature_c=30.0) for m in MINUTES])
    a = run(sqlite_store, asset_id="A1", run_id="sim-test-run")
    b = run(sqlite_store, asset_id="A1", run_id="sim-other")
    assert a.metrics["inlet_temperature_c"].latest == 21.0 and b.metrics["inlet_temperature_c"].latest == 30.0
    assert run(sqlite_store, asset_id="A1").scope.simulation_run_id == "sim-other"   # default: newest run


def test_window_parameter_changes_the_history(sqlite_store):
    seed(sqlite_store, series("A1", range(0, 60), inlet_temperature_c=lambda m: 20 + 0.01 * m))
    assert run(sqlite_store, asset_id="A1", window_minutes=30).metrics["inlet_temperature_c"].observed_points == 30
    assert run(sqlite_store, asset_id="A1").metrics["inlet_temperature_c"].observed_points == 15


# 11. configuration -----------------------------------------------------------------------------------

def test_shipped_config_loads_with_all_limits_unset():
    cfg = load_cooling_config()
    assert cfg.window_minutes == 15 and all(v is None for v in vars(cfg.limits).values())


@pytest.mark.parametrize("text, message", [
    ("window_minutes: 0", "window_minutes"),
    ("window_minutes: fifteen", "window_minutes"),
    ("recent_minutes: 30", "recent_minutes"),
    ("unknown_key: 1", "unknown settings"),
    ("limits: {inlet_temperature_high_c: hot}", "number or null"),
    ("limits: {not_a_limit: 1}", "unknown limits"),
    ("limits: {inlet_temperature_high_c: 30, inlet_temperature_critical_c: 25}", "critical"),
    ("limits: {airflow_drop_fraction: 1.5}", "between 0 and 1"),
    ("min_coverage: 2", "min_coverage"),
    ("trend_min_change: {cpu_utilization: -1}", "trend_min_change"),
    ("[1, 2]", "mapping"),
])
def test_invalid_configuration_is_rejected(tmp_path, text, message):
    import yaml
    base = load_cooling_config()
    lines = tmp_path / "base.yaml"
    data = yaml.safe_load(open("config/cooling_state.yaml", encoding="utf-8"))
    override = yaml.safe_load(text)
    if isinstance(override, dict):
        for k, v in override.items():
            data[k] = {**data[k], **v} if isinstance(v, dict) and isinstance(data.get(k), dict) else v
    else:
        data = override
    lines.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(CoolingConfigError, match=message):
        load_cooling_config(lines)
    assert base.window_minutes == 15


def test_missing_config_file(tmp_path):
    with pytest.raises(CoolingConfigError, match="not found"):
        load_cooling_config(tmp_path / "absent.yaml")
