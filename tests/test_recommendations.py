"""Multi-agent recommendation workflow: rules, validator, LangGraph routing, LLM failures, persistence."""

import inspect
import re
from datetime import timedelta

import numpy as np
import pytest
from pydantic import ValidationError

from cooling_helpers import RUN, T0, all_limits_config, event, seed, series, wall_clock
from recommendation_helpers import ScriptedLLM, at, echo_explanation, engine, finding, plan
from data_engineering.cooling_model.features import Series
from data_engineering.recommendations import graph as graph_module
from data_engineering.recommendations import schemas, validator
from data_engineering.recommendations.llm import LLMError, langchain_base_url
from data_engineering.recommendations.rules import Candidate
from data_engineering.recommendations.schemas import AgentFinding, AnalysisPlan, Explanation, Recommendation


# ------------------------------------------------------------------ scenarios

def seed_airflow_sawtooth(store, minutes=60):
    """Each 15-minute window: airflow falls 180 -> 152 CFM while inlet rises 21.0 -> 21.84 C."""
    seed(store, series("A1", range(minutes), airflow_cfm=lambda m: 180.0 - 2 * (m % 15),
                       inlet_temperature_c=lambda m: 21.0 + 0.06 * (m % 15)))


def seed_light_load(store, minutes=60):
    seed(store, series("A1", range(minutes), cpu_utilization=20.0, workload_intensity=0.2))


def seed_near_limit(store, minutes=60):
    seed(store, series("A1", range(minutes), inlet_temperature_c=25.4))


class FakeModel:
    """Stands in for CoolingResponseModel: a fixed forecast, forecasting_baseline label."""

    def __init__(self, predicted=(25.6, 25.8), skill=0.05, extrapolated=()):
        self.version, self.label = "crm-fake", "forecasting_baseline"
        self.predicted, self.extrapolated = predicted, list(extrapolated)
        self.metadata = {
            "version": self.version, "model_label": self.label, "horizons_minutes": [5, 10],
            "training_data": {"source_type": "simulated"}, "feature_ranges": {},
            "validity": {"controllable_settings": {"fan_speed_rpm": {"validated": False, "reasons": ["no response"]}}},
            "evaluation": {h: {"test": {"model": {"rmse": 0.05}, "persistence": {"rmse": 0.05},
                                        "rmse_skill_vs_persistence": skill}} for h in ("5", "10")}}

    def predict_for_asset(self, store, run_id, asset_id, as_of=None, **kw):
        rows = store.readings_window(run_id, asset_id, as_of - timedelta(minutes=5), as_of)
        values = {"fan_speed_rpm": np.array([r["fan_speed_rpm"] for r in rows], float)}
        return {"forecasts": [{"horizon_minutes": h, "forecast_timestamp": (as_of + timedelta(minutes=h)).isoformat(),
                               "predicted_inlet_temperature_c": p, "persistence_inlet_temperature_c": 25.4,
                               "offline_test_rmse_c": 0.05, "offline_persistence_rmse_c": 0.05}
                              for h, p in zip((5, 10), self.predicted)],
                "model_version": self.version, "model_label": self.label, "latest_inlet_temperature_c": 25.4,
                "data_quality": {"status": "OK", "notes": []}, "model_validity_warnings": [],
                "extrapolated_features": self.extrapolated,
                "_series": Series(np.zeros(len(rows), np.int64), values, {}), "_features": None}


def by_type(result):
    return {r["type"]: r for r in result["recommendations"]}


def nodes(result):
    return [n["node"] for n in result["execution"]["nodes"]]


# ------------------------------------------------------------------ schemas and boundaries

def test_recommendations_always_require_approval_and_never_execute():
    fields = Recommendation.model_fields
    assert fields["human_approval_required"].annotation.__args__ == (True,)
    assert fields["automatic_control_executed"].annotation.__args__ == (False,)


def test_llm_output_schemas_have_no_numeric_fields():
    """An LLM cannot introduce a forecast, an energy figure, a limit or a setting."""
    for schema in (AnalysisPlan, AgentFinding, Explanation):
        text = str(schema.model_json_schema())
        assert "'number'" not in text and "'integer'" not in text, schema.__name__


def test_llm_schemas_reject_unknown_types_and_truncate_long_text():
    with pytest.raises(ValidationError):
        AgentFinding(summary="x", supported_types=["SHUT_DOWN_CHILLER"], conclusion="SUPPORTED")
    long = AgentFinding(summary="y" * 5000, conclusion="INCONCLUSIVE", hypotheses=["h"] * 20)
    assert len(long.summary) == 1200 and long.summary.endswith("…") and len(long.hypotheses) == 6
    with pytest.raises(ValidationError):
        AnalysisPlan(agents=["thermal", "chiller_control"], rationale="r")


def test_no_equipment_control_path_in_the_package():
    import data_engineering.recommendations as pkg
    from pathlib import Path
    for path in Path(pkg.__file__).parent.glob("*.py"):
        source = path.read_text(encoding="utf-8").lower()
        for forbidden in ("subprocess", "socket", "modbus", "bacnet", r"requests\.post", "set_fan", "setpoint_write",
                          r"producer\("):
            assert not re.search(rf"(?<![A-Za-z0-9_]){forbidden}", source), (path.name, forbidden)


def test_base_url_normalisation():
    assert langchain_base_url("https://api.fireworks.ai/inference/v1") == "https://api.fireworks.ai/inference"
    assert langchain_base_url("https://api.fireworks.ai/inference/v1/") == "https://api.fireworks.ai/inference"


# ------------------------------------------------------------------ deterministic workflow

def test_airflow_investigation_end_to_end_and_persisted(sqlite_store):
    seed_airflow_sawtooth(sqlite_store)
    result = engine(sqlite_store.path).analyze("A1", RUN, at(59))
    recs = by_type(result)
    a = recs["AIRFLOW_INVESTIGATION"]
    assert a["status"] == "REQUIRES_REVIEW" and a["human_approval_required"] and not a["automatic_control_executed"]
    assert any(e["metric"] == "AF_AIRFLOW_DROP_WITH_RISING_TEMPERATURE" for e in a["evidence"])
    assert a["hypotheses"] and "not established" in a["recommended_action"]
    assert a["explanation_source"] == "deterministic_template"
    assert result["status"] == "COMPLETED" and result["execution"]["mode"] == "deterministic"
    stored = sqlite_store.get_recommendation(a["recommendation_id"])
    assert stored["payload_json"]["type"] == "AIRFLOW_INVESTIGATION"
    assert sqlite_store.get_analysis(result["analysis_id"])["status"] == "COMPLETED"


def test_same_window_is_not_stored_twice(sqlite_store):
    seed_airflow_sawtooth(sqlite_store)
    e = engine(sqlite_store.path)
    first = e.analyze("A1", RUN, at(59))
    second = e.analyze("A1", RUN, at(59))
    assert first["duplicates_of_existing"] == []
    assert set(second["duplicates_of_existing"]) == {r["recommendation_id"] for r in second["recommendations"]}
    assert len(sqlite_store.list_recommendations(asset_id="A1")) == len(first["recommendations"])


def test_persistent_anomaly_becomes_maintenance_inspection(sqlite_store):
    seed_airflow_sawtooth(sqlite_store)
    recs = by_type(engine(sqlite_store.path).analyze("A1", RUN, at(59)))
    m = recs["MAINTENANCE_INSPECTION"]
    assert m["status"] == "REQUIRES_REVIEW" and "No component failure is claimed" in m["recommended_action"]
    assert any(e["metric"].endswith(".persistent_windows") and e["observed_value"] >= 3 for e in m["evidence"])


def test_one_window_anomaly_is_not_maintenance(sqlite_store):
    seed(sqlite_store, series("A1", range(45)))
    seed(sqlite_store, series("A1", range(45, 60), airflow_cfm=lambda m: 180.0 - 2 * (m - 45),
                              inlet_temperature_c=lambda m: 21.0 + 0.06 * (m - 45)))
    result = engine(sqlite_store.path).analyze("A1", RUN, at(59))
    assert "AIRFLOW_INVESTIGATION" in by_type(result) and "MAINTENANCE_INSPECTION" not in by_type(result)
    assert any(n["type"] == "MAINTENANCE_INSPECTION" for n in result["execution"]["not_applicable"])


def test_missing_cooling_power_blocks_energy_claims(sqlite_store):
    seed_light_load(sqlite_store)
    result = engine(sqlite_store.path).analyze("A1", RUN, at(59))
    eff = by_type(result)["COOLING_EFFICIENCY"]
    assert eff["status"] == "INCONCLUSIVE" and eff["uncertainty"]["level"] == "UNKNOWN"
    assert eff["estimated_energy_impact"] is None and eff["candidate_setting"] is None
    assert any("measured cooling-system power" in m for m in eff["missing_evidence"])
    na = {n["type"]: n["reason"] for n in result["execution"]["not_applicable"]}
    assert "not cooling power" in na["ENERGY_WASTE"]
    power = [e for e in eff["evidence"] if e["metric"].startswith("power_consumption_kw")]
    assert power == []                      # server power is never used as efficiency evidence


def test_preemptive_cooling_with_agreeing_forecast(sqlite_store):
    seed_near_limit(sqlite_store)
    result = engine(sqlite_store.path, model=FakeModel()).analyze("A1", RUN, at(59))
    p = by_type(result)["PREEMPTIVE_COOLING"]
    assert p["status"] == "REQUIRES_REVIEW" and p["predicted_temperature"] == 25.8
    assert p["temperature_horizon_minutes"] == 10 and p["candidate_setting"] is None
    assert p["uncertainty"]["level"] == "MEDIUM"
    kinds = {e["metric"]: e["kind"] for e in p["evidence"]}
    assert kinds["inlet_temperature_c.latest"] == "measured" and kinds["inlet_forecast_10min"] == "model_prediction"


def test_preemptive_cooling_when_the_forecaster_has_no_skill(sqlite_store):
    seed_near_limit(sqlite_store)
    p = by_type(engine(sqlite_store.path, model=FakeModel(skill=-0.04)).analyze("A1", RUN, at(59)))["PREEMPTIVE_COOLING"]
    assert p["uncertainty"]["level"] == "HIGH" and any("does not beat persistence" in w for w in p["validation_warnings"])


@pytest.mark.parametrize("model, missing", [
    (None, "inlet forecast"),
    (FakeModel(predicted=(23.0, 23.1)), "forecast agreement"),
    (FakeModel(extrapolated=["inlet_temperature_c=45 outside"]), "out-of-distribution"),
])
def test_preemptive_cooling_inconclusive_without_valid_forecast(sqlite_store, model, missing):
    seed_near_limit(sqlite_store)
    p = by_type(engine(sqlite_store.path, model=model).analyze("A1", RUN, at(59)))["PREEMPTIVE_COOLING"]
    assert p["status"] == "INCONCLUSIVE" and any(missing in m for m in p["missing_evidence"])


def test_missing_limits_make_thermal_types_not_applicable(sqlite_store):
    from cooling_helpers import config
    seed_near_limit(sqlite_store)
    result = engine(sqlite_store.path, model=FakeModel(), cooling_config=config()).analyze("A1", RUN, at(59))
    # shipped config: limit 27 + margin 2 exist, rise limits null -> approach by margin still fires
    assert "PREEMPTIVE_COOLING" in by_type(result)
    no_limits = engine(sqlite_store.path, model=FakeModel(),
                       cooling_config=all_limits_config(inlet_temperature_high_c=None))
    result = no_limits.analyze("A1", RUN, at(59))
    p = by_type(result).get("PREEMPTIVE_COOLING")
    assert p is None or (p["status"] == "INCONCLUSIVE" and
                         any("inlet temperature limit" in m for m in p["missing_evidence"]))


def test_insufficient_telemetry_skips_planning_and_reports_data_quality(sqlite_store):
    seed(sqlite_store, series("A1", range(60)))
    main, sub = ScriptedLLM(), ScriptedLLM(AgentFinding=finding("SENSOR_DATA_QUALITY"))
    result = engine(sqlite_store.path, main=main, sub=sub).analyze("A1", RUN, T0 - timedelta(hours=2))
    assert "plan_analysis" not in nodes(result) and "thermal_agent" not in nodes(result)
    assert result["execution"]["telemetry_status"] == "INSUFFICIENT"
    s = by_type(result)["SENSOR_DATA_QUALITY"]
    assert s["status"] == "REQUIRES_REVIEW"
    assert [c[0] for c in main.calls] == ["Explanation"]          # no planning call


def test_live_analysis_with_stopped_pipeline(sqlite_store):
    seed_near_limit(sqlite_store)
    result = engine(sqlite_store.path, model=FakeModel(), clock=wall_clock(3600)).analyze("A1", RUN)
    recs = by_type(result)
    assert recs["SENSOR_DATA_QUALITY"]["status"] == "REQUIRES_REVIEW"
    assert recs["PREEMPTIVE_COOLING"]["status"] == "INCONCLUSIVE"
    assert any("pipeline has stopped" in m for m in recs["PREEMPTIVE_COOLING"]["missing_evidence"])


def test_historical_analysis_does_not_flag_the_stopped_stream(sqlite_store):
    seed_light_load(sqlite_store)
    result = engine(sqlite_store.path, clock=wall_clock(3600)).analyze("A1", RUN, at(59))
    assert "SENSOR_DATA_QUALITY" not in by_type(result)
    assert any("historical analysis" in w for r in result["recommendations"] for w in r["validation_warnings"])


def test_unknown_asset_raises(sqlite_store):
    from data_engineering.cooling.estimator import UnknownScopeError
    seed_light_load(sqlite_store)
    with pytest.raises(UnknownScopeError):
        engine(sqlite_store.path).analyze("NOPE", RUN, at(59))


# ------------------------------------------------------------------ LLM orchestration

def llm_engine(store, main_script=None, sub_script=None, **kw):
    main = ScriptedLLM("fake/glm", **(main_script or {"AnalysisPlan": plan("anomaly"), "Explanation": echo_explanation}))
    sub = ScriptedLLM("fake/deepseek", **(sub_script or {}))
    return engine(store.path, main=main, sub=sub, **kw), main, sub


def test_planner_routes_and_required_agents_are_added(sqlite_store):
    seed_airflow_sawtooth(sqlite_store)
    e, main, sub = llm_engine(sqlite_store, sub_script={"AgentFinding": lambda s, u: finding(
        *(["AIRFLOW_INVESTIGATION", "MAINTENANCE_INSPECTION"] if "Anomaly" in s else []),
        conclusion="SUPPORTED" if "Anomaly" in s else "NOT_SUPPORTED")})
    result = e.analyze("A1", RUN, at(59))
    p = result["execution"]["plan"]
    assert p["llm_selected"] == ["anomaly"] and "thermal" in p["added_as_required"]
    assert "energy_agent" not in nodes(result)                       # not planned, not run
    assert result["execution"]["agents"]["anomaly"]["status"] == "OK"
    assert by_type(result)["AIRFLOW_INVESTIGATION"]["status"] == "REQUIRES_REVIEW"
    assert by_type(result)["AIRFLOW_INVESTIGATION"]["explanation_source"] == "llm"
    assert result["execution"]["tokens"]["total_tokens"] > 0


def test_energy_agent_makes_no_llm_call_without_cooling_power(sqlite_store):
    seed_light_load(sqlite_store)
    e, main, sub = llm_engine(sqlite_store, {"AnalysisPlan": plan("thermal", "energy"), "Explanation": echo_explanation},
                              {"AgentFinding": finding(conclusion="NOT_SUPPORTED")})
    result = e.analyze("A1", RUN, at(59))
    assert result["execution"]["agents"]["energy"]["status"] == "DETERMINISTIC"
    assert all("Energy Analysis Agent" not in user for _, user in sub.calls)


def test_planner_failure_falls_back_to_all_agents(sqlite_store):
    seed_light_load(sqlite_store)
    e, _, _ = llm_engine(sqlite_store, {"AnalysisPlan": LLMError("timeout", "slow"), "Explanation": echo_explanation},
                         {"AgentFinding": finding(conclusion="NOT_SUPPORTED")})
    result = e.analyze("A1", RUN, at(59))
    assert result["execution"]["plan"]["source"] == "fallback"
    assert {"thermal_agent", "energy_agent", "anomaly_agent"} <= set(nodes(result))
    assert result["status"] == "COMPLETED_WITH_FALLBACK"


def test_failed_subagent_never_yields_a_successful_recommendation(sqlite_store):
    seed_airflow_sawtooth(sqlite_store)
    e, _, _ = llm_engine(sqlite_store, sub_script={"AgentFinding": lambda s, u: (_ for _ in ()).throw(
        LLMError("provider", "503")) if "Anomaly" in s else finding(conclusion="NOT_SUPPORTED")})
    result = e.analyze("A1", RUN, at(59))
    a = by_type(result)["AIRFLOW_INVESTIGATION"]
    assert a["status"] == "INCONCLUSIVE" and any("agent failed" in m for m in a["missing_evidence"])
    assert result["status"] == "COMPLETED_WITH_FALLBACK"


def test_conflict_triggers_one_reanalysis_then_inconclusive(sqlite_store):
    seed_airflow_sawtooth(sqlite_store)
    e, _, sub = llm_engine(sqlite_store, sub_script={"AgentFinding": lambda s, u: finding(conclusion="NOT_SUPPORTED")})
    result = e.analyze("A1", RUN, at(59))
    anomaly_calls = [u for _, u in sub.calls if "AN-1" in u]
    assert len(anomaly_calls) == 2 and "previous_attempt_problems" in anomaly_calls[1]
    assert result["execution"]["agents"]["anomaly"]["attempts"] == 2
    a = by_type(result)["AIRFLOW_INVESTIGATION"]
    assert a["status"] == "INCONCLUSIVE" and any("agreement between" in m for m in a["missing_evidence"])


def test_conflict_resolved_by_reanalysis(sqlite_store):
    seed_airflow_sawtooth(sqlite_store)
    answers = {"n": 0}

    def anomaly(system, user):
        if "Anomaly" not in system:
            return finding(conclusion="NOT_SUPPORTED")
        answers["n"] += 1
        return finding(conclusion="NOT_SUPPORTED") if answers["n"] == 1 else \
            finding("AIRFLOW_INVESTIGATION", "MAINTENANCE_INSPECTION")
    e, _, _ = llm_engine(sqlite_store, sub_script={"AgentFinding": anomaly})
    result = e.analyze("A1", RUN, at(59))
    assert by_type(result)["AIRFLOW_INVESTIGATION"]["status"] == "REQUIRES_REVIEW"


def test_agent_cannot_add_unsupported_types_or_cite_unknown_evidence(sqlite_store):
    seed_light_load(sqlite_store)
    bad = finding("SENSOR_DATA_QUALITY", summary="See AN-99 and TH-1.",
                  interpretations=[{"evidence_id": "AN-404", "interpretation": "made up"}])
    e, _, _ = llm_engine(sqlite_store, sub_script={"AgentFinding": lambda s, u: bad if "Anomaly" in s
                                                    else finding(conclusion="NOT_SUPPORTED")})
    result = e.analyze("A1", RUN, at(59))
    a = result["execution"]["agents"]["anomaly"]
    assert any("AN-404" in i for i in a["issues"]) and any("AN-99" in i for i in a["issues"])
    assert "SENSOR_DATA_QUALITY" not in by_type(result)          # the LLM cannot create a recommendation


def test_hallucinated_numbers_in_the_explanation_are_rejected(sqlite_store):
    seed_airflow_sawtooth(sqlite_store)

    def lying(system, user):
        good = echo_explanation(system, user)
        good.items[0].why = "Airflow fell 37.5% and cooling energy can drop by 12.3 kW."
        return good
    e, main, _ = llm_engine(sqlite_store, {"AnalysisPlan": plan("anomaly"), "Explanation": lying},
                            {"AgentFinding": lambda s, u: finding("AIRFLOW_INVESTIGATION", "MAINTENANCE_INSPECTION")
                             if "Anomaly" in s else finding(conclusion="NOT_SUPPORTED")})
    result = e.analyze("A1", RUN, at(59))
    assert result["execution"]["explanation_source"] == "deterministic_template"
    assert [c[0] for c in main.calls].count("Explanation") == 2                 # one bounded re-ask
    trace = [n for n in result["execution"]["nodes"] if n["node"] == "generate_explanation"][0]
    assert any("37.5" in x for x in trace["rejected"]) and any("12.3" in x for x in trace["rejected"])


def test_execution_claims_are_rejected():
    ctx = type("C", (), {})()
    ctx.snapshot = type("S", (), {"window_minutes": 15})()
    ctx.cooling_config = all_limits_config()
    rec = {"recommendation_id": "r1", "evidence": [], "current_setting": None, "candidate_setting": None,
           "predicted_temperature": None, "temperature_horizon_minutes": None}
    item = {"recommendation_id": "r1", "what_to_evaluate": "x", "why": "y", "energy_impact": "n/a",
            "uncertainty": "u", "why_human_review": "z"}
    bad = Explanation(overall_summary="The fan speed has been reduced automatically.", items=[item])
    ok = Explanation(overall_summary="No change has been executed; nothing was applied.", items=[item])
    assert any("carried out" in i for i in validator.check_explanation(bad, [rec], ctx))
    assert validator.check_explanation(ok, [rec], ctx) == []


# ------------------------------------------------------------------ validator units

class _Ctx:
    def __init__(self, store, optimizer=None, label="forecasting_baseline"):
        from data_engineering.cooling.config import load_cooling_config  # noqa: F401
        self.optimizer, self.forecast, self.as_of_requested = optimizer, None, at(59)
        self.model_summary = {"label": label, "horizons_minutes": [5, 10], "test_rmse_skill_vs_persistence": {}}
        self.cooling_config = all_limits_config()
        self.snapshot = type("S", (), {"findings": [], "as_of": at(59),
                                       "data_quality": type("D", (), {"status": "OK"})()})()


def test_unvalidated_candidate_setting_is_rejected():
    c = Candidate(type="COOLING_EFFICIENCY", status="REQUIRES_REVIEW", severity="INFO", title="t",
                  recommended_action="a", evidence_ids=[], candidate_setting=2500.0, current_setting=3000.0)
    ctx = _Ctx(None, optimizer={"status": "CONSTRAINTS_ONLY", "candidates": [
        {"setting": 2500.0, "feasible": True, "within_bounds": True, "within_supported_range": True}]})
    out = validator.validate_candidates(ctx, [c], {}, {"thermal": {"status": "DETERMINISTIC"}})[0]
    assert out["candidate_setting"] is None and out["status"] == "INCONCLUSIVE"
    assert any("candidate setting rejected" in w for w in out["validation_warnings"])


def test_ranking_puts_actionable_and_severe_first():
    recs = [{"type": "COOLING_EFFICIENCY", "status": "REQUIRES_REVIEW", "severity": "INFO"},
            {"type": "SENSOR_DATA_QUALITY", "status": "INCONCLUSIVE", "severity": "WARNING"},
            {"type": "AIRFLOW_INVESTIGATION", "status": "REQUIRES_REVIEW", "severity": "WARNING"},
            {"type": "PREEMPTIVE_COOLING", "status": "REQUIRES_REVIEW", "severity": "CRITICAL"}]
    assert [r["type"] for r in validator.rank(recs)] == ["PREEMPTIVE_COOLING", "AIRFLOW_INVESTIGATION",
                                                         "COOLING_EFFICIENCY", "SENSOR_DATA_QUALITY"]


def test_every_type_has_an_owner_and_requirements():
    assert set(schemas.TYPE_OWNER) == set(schemas.RECOMMENDATION_TYPES) == set(graph_module.TYPE_REQUIREMENTS)


def test_engine_source_has_no_arbitrary_execution():
    source = inspect.getsource(graph_module)
    assert "exec(" not in source and "eval(" not in source


# ------------------------------------------------------------------ store (both backends)

def test_store_recommendation_tables(store):
    analysis = {"analysis_id": "ana-1", "simulation_run_id": RUN, "asset_id": "A1", "window_start": at(0),
                "window_end": at(15), "status": "COMPLETED", "trace_json": {"nodes": []}, "created_at": at(16)}
    rec = {"recommendation_id": "rec-1", "analysis_id": "ana-1", "simulation_run_id": RUN, "asset_id": "A1",
           "recommendation_type": "AIRFLOW_INVESTIGATION", "status": "REQUIRES_REVIEW", "severity": "WARNING",
           "window_start": at(0),
           "window_end": at(15), "payload_json": {"k": "v"}, "created_at": at(16)}
    assert store.save_analysis(analysis, [rec]) == []
    assert store.save_analysis({**analysis, "analysis_id": "ana-2"}, [{**rec, "analysis_id": "ana-2"}]) == ["rec-1"]
    assert [r["recommendation_id"] for r in store.list_recommendations(asset_id="A1")] == ["rec-1"]
    assert store.list_recommendations(recommendation_type="ENERGY_WASTE") == []
    assert store.get_recommendation("rec-1")["payload_json"] == {"k": "v"}
    store.add_decision({"decision_id": "d1", "recommendation_id": "rec-1", "decision": "DEFERRED",
                        "reviewer": "op", "note": None, "decided_at": at(20)})
    assert store.decisions("rec-1")[0]["decision"] == "DEFERRED"
    assert store.get_analysis("ana-1")["trace_json"] == {"nodes": []}
    with pytest.raises(Exception):
        store.add_decision({"decision_id": "d2", "recommendation_id": "rec-1", "decision": "EXECUTE",
                            "reviewer": "op", "note": None, "decided_at": at(21)})


def test_events_helper_is_used():                  # keep the import of event meaningful for readers
    assert event("A1", 0).reading["asset_id"] == "A1"
