"""The five API groups: dashboard, telemetry/assets, cooling predict/optimize, recommendations/alerts, copilot."""

import dataclasses
import json
from datetime import timedelta

import httpx
import pytest

from conftest import make_api_app
from cooling_helpers import RUN, T0, event, seed, series
from cooling_model_helpers import seed_world, settings
from data_engineering.cooling_model.training import train
from recommendation_helpers import ScriptedLLM

pytestmark = pytest.mark.anyio
V1 = "/api/v1"


def code(r):
    return r.json()["error"]["code"]


async def client(app):
    """Async context manager yielding a client with the lifespan running."""
    class _C:
        async def __aenter__(self):
            self.ctx = app.router.lifespan_context(app)
            await self.ctx.__aenter__()
            self.c = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
            return self.c

        async def __aexit__(self, *exc):
            await self.c.aclose()
            await self.ctx.__aexit__(*exc)
    return _C()


def seed_two_assets(store):
    """A1 (Compute) and A2 (GPU), 60 minutes; A1 has one missing reading and one duplicate; A2 stops at 40."""
    seed(store, series("A1", range(60), inlet_temperature_c=lambda m: 21.0 + 0.01 * m))
    seed(store, [event("A1", 30, status="missing", slot=999_999)])
    seed(store, [event("A1", 10, inlet_temperature_c=99.0, slot=10 * 60)])          # same event id -> conflict
    seed(store, [event("A2", m, asset_type="GPU", gpu_utilization=55.0) for m in range(41)])


# ------------------------------------------------------------------ API 1: dashboard

async def test_dashboard_overview(api_client, sqlite_store):
    seed_two_assets(sqlite_store)
    r = await api_client.get(f"{V1}/dashboard/overview")
    assert r.status_code == 200
    b = r.json()
    assert b["assets"]["known"] == 2 and b["assets"]["fresh"] == 1 and b["assets"]["stale"] == ["A2"]
    assert b["latest_telemetry_timestamp"] == "2026-01-01T12:59:00Z"
    gpu = b["kpis"]["gpu_utilization"]
    assert gpu["assets_reporting"] == 1 and gpu["mean"] == 55.0          # the Compute server is not counted as 0
    assert b["kpis"]["power_consumption_kw"]["note"].startswith("per-server IT power")
    assert b["kpis"]["inlet_temperature_c"]["unit"] == "degC"
    assert b["trends"]["points"] and b["trends"]["slopes"]["inlet_temperature_c"]["per_minute"] is not None
    assert "cooling_power_kw" in b["unavailable_measurements"]
    assert any("stale" in w for w in b["warnings"])


async def test_dashboard_errors(api_client, sqlite_store):
    seed_two_assets(sqlite_store)
    assert code(await api_client.get(f"{V1}/dashboard/overview", params={"asset_id": "NOPE"})) == "unknown_asset"
    r = await api_client.get(f"{V1}/dashboard/overview", params={"start_time": "2026-01-01T13:00:00Z",
                                                                 "end_time": "2026-01-01T12:00:00Z"})
    assert r.status_code == 422 and code(r) == "invalid_parameter"
    r = await api_client.get(f"{V1}/dashboard/overview", params={"start_time": "2025-12-01T00:00:00Z",
                                                                 "end_time": "2026-01-01T12:00:00Z"})
    assert "maximum" in r.json()["error"]["message"]


async def test_empty_store(api_client):
    r = await api_client.get(f"{V1}/dashboard/overview")
    assert r.status_code == 404 and code(r) == "unknown_run"


# ------------------------------------------------------------------ API 2: telemetry and assets

async def test_telemetry_raw_and_bucketed(api_client, sqlite_store):
    seed_two_assets(sqlite_store)
    raw = (await api_client.get(f"{V1}/assets/A1/telemetry", params={
        "start_time": "2026-01-01T12:00:00Z", "end_time": "2026-01-01T12:59:00Z", "metrics": "inlet_temperature_c"})).json()
    s = raw["series"][0]
    times = [p["timestamp"] for p in s["points"]]
    assert times == sorted(times) and len(times) == len(set(times))      # chronological, duplicates collapsed
    assert s["duplicates_collapsed"] >= 1
    assert raw["metrics"] == [{"name": "inlet_temperature_c", "unit": "degC", "derived_from": None, "note": None}]
    b = (await api_client.get(f"{V1}/telemetry", params={"asset_id": "A1", "interval": "15m",
                                                         "metrics": "inlet_temperature_c,temperature_delta_c",
                                                         "start_time": "2026-01-01T12:00:00Z",
                                                         "end_time": "2026-01-01T12:59:00Z"})).json()
    first = b["series"][0]["points"][0]
    assert first["bucket_start"] == "2026-01-01T12:00:00Z" and first["inlet_temperature_c"]["count"] == 14
    assert "never treated as 0" in b["aggregation"]


async def test_telemetry_never_mixes_assets(api_client, sqlite_store):
    seed_two_assets(sqlite_store)
    b = (await api_client.get(f"{V1}/telemetry", params={"metrics": "gpu_utilization", "interval": "1h"})).json()
    by = {s["asset_id"]: s for s in b["series"]}
    assert set(by) == {"A1", "A2"}
    assert by["A1"]["points"][0]["gpu_utilization"]["count"] == 0          # not applicable, not 0
    assert by["A2"]["points"][0]["gpu_utilization"]["mean"] == 55.0


async def test_telemetry_cursor_pagination(api_client, sqlite_store):
    seed_two_assets(sqlite_store)
    params = {"asset_id": "A1", "metrics": "inlet_temperature_c", "limit": 25,
              "start_time": "2026-01-01T11:59:00Z", "end_time": "2026-01-01T12:59:00Z"}
    seen, cursor = [], None
    for _ in range(5):
        page = (await api_client.get(f"{V1}/telemetry", params={**params, **({"cursor": cursor} if cursor else {})})).json()
        seen += [p["timestamp"] for p in page["series"][0]["points"]]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert len(seen) == len(set(seen)) == 60 and seen == sorted(seen)
    r = await api_client.get(f"{V1}/telemetry", params={"cursor": cursor or "abc"})
    assert r.status_code == 422


@pytest.mark.parametrize("params, expected", [
    ({"metrics": "cooling_power_kw"}, "validation_error"),
    ({"metrics": "inlet_temperature_c,inlet_temperature_c"}, "validation_error"),
    ({"interval": "7m"}, "validation_error"),
    ({"limit": 0}, "validation_error"),
    ({"start_time": "2026-01-01T12:00:00"}, "validation_error"),
    ({"cursor": "not-a-cursor", "asset_id": "A1"}, "invalid_parameter"),
    ({"asset_id": "A9"}, "unknown_asset"),
    ({"run_id": "sim-none"}, "unknown_run"),
])
async def test_telemetry_validation(api_client, sqlite_store, params, expected):
    seed_two_assets(sqlite_store)
    assert code(await api_client.get(f"{V1}/telemetry", params=params)) == expected


async def test_assets_list_and_detail(api_client, sqlite_store):
    seed_two_assets(sqlite_store)
    lst = (await api_client.get(f"{V1}/assets")).json()
    assert [a["asset_id"] for a in lst["items"]] == ["A1", "A2"] and [a["fresh"] for a in lst["items"]] == [True, False]
    d = (await api_client.get(f"{V1}/assets/A1")).json()
    assert d["latest_reading"]["values"]["gpu_utilization"]["value"] is None
    assert d["latest_reading"]["values"]["inlet_temperature_c"]["unit"] == "degC"
    assert d["cooling_state"]["state"]
    assert code(await api_client.get(f"{V1}/assets/A9")) == "unknown_asset"


async def test_point_limit_is_enforced(sqlite_store):
    seed_two_assets(sqlite_store)
    async with await client(make_api_app(sqlite_store.path, max_telemetry_points=20)) as c:
        b = (await c.get(f"{V1}/telemetry", params={"metrics": "inlet_temperature_c"})).json()
    assert sum(len(s["points"]) for s in b["series"]) <= 20 and b["series"][0]["truncated"]


# ------------------------------------------------------------------ access control

async def test_bearer_token(sqlite_store):
    seed_two_assets(sqlite_store)
    async with await client(make_api_app(sqlite_store.path, auth_token="s3cret")) as c:
        assert (await c.get(f"{V1}/health")).status_code == 200                  # health stays open
        r = await c.get(f"{V1}/assets")
        assert r.status_code == 401 and code(r) == "unauthorized"
        assert (await c.get(f"{V1}/assets", headers={"Authorization": "Bearer wrong"})).status_code == 401
        assert (await c.get(f"{V1}/assets", headers={"Authorization": "Bearer s3cret"})).status_code == 200


async def test_asset_scope(sqlite_store):
    seed_two_assets(sqlite_store)
    async with await client(make_api_app(sqlite_store.path, allowed_assets=["A1"])) as c:
        assert [a["asset_id"] for a in (await c.get(f"{V1}/assets")).json()["items"]] == ["A1"]
        for r in (await c.get(f"{V1}/assets/A2"), await c.get(f"{V1}/telemetry", params={"asset_id": "A2"}),
                  await c.post(f"{V1}/cooling/predict", json={"asset_id": "A2"}),
                  await c.post(f"{V1}/recommendations/generate", json={"asset_id": "A2"}),
                  await c.post(f"{V1}/copilot/chat", json={"message": "status", "asset_id": "A2"})):
            assert r.status_code == 403 and code(r) == "asset_not_authorized"
        telemetry = (await c.get(f"{V1}/telemetry")).json()
        assert [s["asset_id"] for s in telemetry["series"]] == ["A1"]
        overview = (await c.get(f"{V1}/dashboard/overview")).json()
        assert overview["assets"]["known"] == 1


# ------------------------------------------------------------------ API 3: cooling predict / optimize

async def test_predict_unavailable_without_model(api_client, sqlite_store):
    seed_two_assets(sqlite_store)
    b = (await api_client.post(f"{V1}/cooling/predict", json={"asset_id": "A1"})).json()
    assert b["status"] == "UNAVAILABLE" and b["reasons"][0]["code"] == "MODEL_NOT_TRAINED" and b["forecasts"] == []
    r = await api_client.post(f"{V1}/cooling/predict", json={"asset_id": "A9"})
    assert r.status_code == 404


@pytest.fixture
def trained_app(sqlite_store, tmp_path):
    seed_world(sqlite_store, "observational")
    cfg = settings(tmp_path / "models")
    train(sqlite_store, cfg)
    return make_api_app(sqlite_store.path, cooling_model_settings=cfg), cfg


async def test_predict_with_model(trained_app):
    app, _ = trained_app
    async with await client(app) as c:
        b = (await c.post(f"{V1}/cooling/predict", json={"asset_id": "T-00", "horizons_minutes": [10, 30],
                                                          "as_of": "2026-01-01T20:00:00Z"})).json()
    assert b["status"] == "AVAILABLE" and [f["horizon_minutes"] for f in b["forecasts"]] == [10]
    assert b["unsupported_horizons"] == [30] and b["forecasts"][0]["kind"] == "model_prediction"
    assert b["forecasts"][0]["evaluation"]["offline_test_rmse_c"] > 0 and b["model"]["label"] == "forecasting_baseline"
    assert "not a calibrated" in b["uncertainty"]["method"]


async def test_predict_rejects_incompatible_or_stale_model(trained_app):
    app, cfg = trained_app
    fc = dataclasses.replace(cfg.model.features, lags_minutes={"inlet_temperature_c": (1,)})
    changed = dataclasses.replace(cfg, model=dataclasses.replace(cfg.model, features=fc))
    async with await client(app) as c:
        app.state.cooling_model_registry.settings = changed
        b = (await c.post(f"{V1}/cooling/predict", json={"asset_id": "T-00"})).json()
        assert b["status"] == "UNAVAILABLE" and b["reasons"][0]["code"] == "FEATURE_SCHEMA_MISMATCH"
        app.state.cooling_model_registry.settings = cfg
        app.state.cooling_model_registry.model.metadata["created_at"] = "2020-01-01T00:00:00+00:00"
        app.state.settings = app.state.settings.model_copy(update={"model_max_age_days": 30})
        b = (await c.post(f"{V1}/cooling/predict", json={"asset_id": "T-00"})).json()
        assert b["status"] == "UNAVAILABLE" and b["reasons"][0]["code"] == "MODEL_STALE"


async def test_optimize_inconclusive_and_validation(trained_app):
    app, _ = trained_app
    async with await client(app) as c:
        b = (await c.post(f"{V1}/cooling/optimize", json={
            "asset_id": "T-00", "candidate_settings": {"fan_speed_rpm": [3000, 3200, 3400]}})).json()
        assert b["status"] == "UNAVAILABLE" and b["conclusion"] == "INCONCLUSIVE"
        assert {r["code"] for r in b["reasons"]} >= {"RESPONSE_MODEL_NOT_VALIDATED", "ENERGY_OBJECTIVE_MISSING",
                                                    "T_SAFE_NOT_CONFIGURED", "ACTUATOR_BOUNDS_NOT_CONFIGURED"}
        assert [x["setting"] for x in b["candidates"]] == [3000, 3200, 3400]
        assert all(not x["evaluated"] and x["within_bounds"] is None for x in b["candidates"])
        assert b["recommended_setting"] is None and b["recommendation_only"] is True
        r = await c.post(f"{V1}/cooling/optimize", json={"asset_id": "T-00", "candidate_settings": {"chiller_c": [1]}})
        assert r.status_code == 422 and code(r) == "invalid_parameter"
        r = await c.post(f"{V1}/cooling/optimize", json={"asset_id": "T-00", "candidate_settings": {"fan_speed_rpm": []}})
        assert r.status_code == 422


async def test_optimize_with_validated_model_and_bounds(sqlite_store, tmp_path):
    seed_world(sqlite_store, "causal")
    cfg = settings(tmp_path / "models", t_safe_c=40.0, setting_min=2000.0, setting_max=4000.0)
    train(sqlite_store, cfg)
    async with await client(make_api_app(sqlite_store.path, cooling_model_settings=cfg)) as c:
        b = (await c.post(f"{V1}/cooling/optimize", json={
            "asset_id": "T-00", "as_of": "2026-01-01T20:00:00Z",
            "candidate_settings": {"fan_speed_rpm": [2500, 3500, 9000]}})).json()
    assert b["status"] == "CONSTRAINTS_ONLY" and b["conclusion"] == "INCONCLUSIVE"     # no energy objective
    by = {x["setting"]: x for x in b["candidates"]}
    assert by[9000.0]["within_bounds"] is False and not by[9000.0]["feasible"]
    assert by[2500.0]["evaluated"] and by[2500.0]["predicted_inlet_c"]
    assert b["recommended_setting"] is None


# ------------------------------------------------------------------ API 4: recommendations and alerts

def seed_airflow(store, asset="A1"):
    seed(store, series(asset, range(60), airflow_cfm=lambda m: 180.0 - 2 * (m % 15),
                       inlet_temperature_c=lambda m: 21.0 + 0.06 * (m % 15)))


async def test_generate_list_detail_decide_and_alerts(api_client, sqlite_store):
    seed_airflow(sqlite_store)
    g = await api_client.post(f"{V1}/recommendations/generate", json={"asset_id": "A1",
                                                                       "as_of": "2026-01-01T12:59:00Z"})
    assert g.status_code == 200
    body = g.json()
    assert body["human_approval_required"] is True and body["automatic_control_executed"] is False
    types = {r["type"] for r in body["recommendations"]}
    assert "AIRFLOW_INVESTIGATION" in types
    again = (await api_client.post(f"{V1}/recommendations/generate", json={"asset_id": "A1",
                                                                           "as_of": "2026-01-01T12:59:00Z"})).json()
    assert len(again["duplicates_of_existing"]) == len(again["recommendations"])          # deduplicated
    lst = (await api_client.get(f"{V1}/recommendations", params={"type": "AIRFLOW_INVESTIGATION"})).json()
    assert lst["count"] == 1 and lst["items"][0]["review_status"] == "PENDING_REVIEW"
    rid = lst["items"][0]["recommendation_id"]
    assert (await api_client.get(f"{V1}/recommendations", params={"severity": "CRITICAL"})).json()["count"] == 0
    assert (await api_client.get(f"{V1}/recommendations", params={"end_time": "2026-01-01T12:00:00Z"})).json()["count"] == 0
    d = (await api_client.get(f"{V1}/recommendations/{rid}")).json()
    assert d["recommendation"]["evidence"] and d["recommendation"]["uncertainty"]["basis"]
    dec = await api_client.post(f"{V1}/recommendations/{rid}/decisions",
                                json={"decision": "APPROVED_FOR_EVALUATION", "reviewer": "op1"})
    assert dec.status_code == 200 and dec.json()["automatic_control_executed"] is False
    assert (await api_client.get(f"{V1}/recommendations/{rid}")).json()["review_status"] == "APPROVED_FOR_EVALUATION"
    bad = await api_client.post(f"{V1}/recommendations/{rid}/decisions", json={"decision": "EXECUTE", "reviewer": "x"})
    assert bad.status_code == 422
    alerts = (await api_client.get(f"{V1}/alerts")).json()
    assert alerts["count"] >= 1 and alerts["items"][0]["rule_id"].startswith("AF_")
    aid = alerts["items"][0]["alert_id"]
    one = (await api_client.get(f"{V1}/alerts/{aid}")).json()
    assert one["related_recommendations"] and "not a confirmed equipment fault" in one["note"]
    analysis = (await api_client.get(f"{V1}/recommendations/analyses/{body['analysis_id']}")).json()
    assert analysis["execution"]["mode"] == "deterministic"
    assert code(await api_client.get(f"{V1}/recommendations/rec-{'0' * 20}")) == "unknown_recommendation"
    assert code(await api_client.get(f"{V1}/alerts/alr-{'0' * 20}")) == "unknown_alert"


async def test_recommendation_cursor_pagination(api_client, sqlite_store):
    for a in ("A1", "A2", "A3"):
        seed_airflow(sqlite_store, a)
        await api_client.post(f"{V1}/recommendations/generate", json={"asset_id": a, "as_of": "2026-01-01T12:59:00Z"})
    seen, cursor = [], None
    while True:
        page = (await api_client.get(f"{V1}/recommendations", params={"limit": 2, **({"cursor": cursor} if cursor
                                                                                   else {})})).json()
        seen += [i["recommendation_id"] for i in page["items"]]
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert len(seen) == len(set(seen)) == (await api_client.get(f"{V1}/recommendations",
                                                                params={"limit": 200})).json()["count"]


async def test_agent_status_exposes_no_secret(api_client):
    b = (await api_client.get(f"{V1}/recommendations/agents/status")).json()
    assert b["mode"] == "deterministic" and "api_key" not in json.dumps(b).replace("api_key_configured", "")


async def test_no_equipment_control_endpoints(api_client):
    spec = (await api_client.get("/openapi.json")).json()
    for path, ops in spec["paths"].items():
        assert not any(w in path for w in ("control", "command", "actuate", "setpoint", "execute")), path
        assert not ({"put", "delete", "patch"} & set(ops)), path


# ------------------------------------------------------------------ API 5: copilot

async def chat(c, message, **kw):
    r = await c.post(f"{V1}/copilot/chat", json={"message": message, **kw})
    assert r.status_code == 200, r.text
    return r.json()


async def test_copilot_status_question_and_follow_up(api_client, sqlite_store):
    seed_two_assets(sqlite_store)
    first = await chat(api_client, "What is the current inlet temperature of A1?")
    assert first["interpretation"]["asset_ids"] == ["A1"] and not first["clarification_needed"]
    assert set(first["agent_execution_summary"]["tools"]) == {"latest", "window_summary"}
    kinds = {e["kind"] for e in first["evidence"]}
    assert {"measured", "calculated"} <= kinds and "prediction" not in kinds
    assert all(e["unit"] for e in first["evidence"] if e["kind"] == "measured")
    follow = await chat(api_client, "What about the previous 30 minutes?", conversation_id=first["conversation_id"])
    assert follow["conversation_id"] == first["conversation_id"]
    assert follow["interpretation"]["asset_ids"] == ["A1"] and "assets" in follow["interpretation"]["resolved_from_context"]
    assert "30 minutes" in follow["interpretation"]["time_basis"]


async def test_copilot_routes_minimum_tools(api_client, sqlite_store):
    seed_two_assets(sqlite_store)
    rec = await chat(api_client, "Show existing recommendations for A1")
    assert set(rec["agent_execution_summary"]["tools"]) == {"recommendations"}
    energy = await chat(api_client, "How much cooling energy could A1 save?")
    assert "energy" in energy["agent_execution_summary"]["tools"]
    assert any(e["metric"] == "cooling_power_kw" and e["kind"] == "unavailable" for e in energy["evidence"])
    fc = await chat(api_client, "Forecast inlet temperature of A1")
    assert fc["agent_execution_summary"]["tools"]["forecast"] == "UNAVAILABLE"            # no model trained
    assert fc["predictions"] == []


@pytest.mark.parametrize("message, needle", [
    ("Compare it with the other cooling zone", "no room, zone"),
    ("Forecast the temperature", "Which asset"),
    ("Compare A1", "Which assets should I compare"),
    ("How is asset 99 doing?", "couldn't match"),
])
async def test_copilot_asks_for_clarification(api_client, sqlite_store, message, needle):
    seed_two_assets(sqlite_store)
    b = await chat(api_client, message)
    assert b["clarification_needed"] and needle in b["answer"] and b["evidence"] == []


async def test_copilot_refuses_control_and_stays_read_only(api_client, sqlite_store):
    seed_two_assets(sqlite_store)
    b = await chat(api_client, "Set the fan speed to 3000 rpm on A1 now please")
    assert b["answer"].startswith("This copilot is read-only") and b["automatic_control_executed"] is False
    assert b["optimizer"]["status"] == "UNAVAILABLE" and b["interpretation"]["candidate_settings"] == [3000.0]


async def test_copilot_scope_hides_other_assets(sqlite_store):
    seed_two_assets(sqlite_store)
    async with await client(make_api_app(sqlite_store.path, allowed_assets=["A1"])) as c:
        b = await chat(c, "Compare A1 and A2 inlet temperature")
        assert b["clarification_needed"]                       # A2 is not a known asset for this operator
        b = await chat(c, "status of all assets")
        assert {e["asset_id"] for e in b["evidence"] if e["asset_id"]} == {"A1"}


async def test_copilot_llm_answer_is_number_checked(sqlite_store):
    seed_two_assets(sqlite_store)

    def answer(system, user):
        from data_engineering.copilot.schemas import CopilotAnswer
        return CopilotAnswer(answer="A1 inlet is 77.7 degC [E1] and we have adjusted the fan.")
    main = ScriptedLLM("fake/glm", QueryPlan=LLMFailure(), CopilotAnswer=answer)
    async with await client(make_api_app(sqlite_store.path, llms=(main, ScriptedLLM("fake/ds")))) as c:
        b = await chat(c, "What is the inlet temperature of A1?")
    assert b["agent_execution_summary"]["answer_source"] == "deterministic"
    assert "77.7" not in b["answer"] and any("failed validation" in w for w in b["warnings"])
    assert any("planner unavailable" in w for w in b["warnings"])
    assert [n for n, _ in main.calls].count("CopilotAnswer") == 2                    # one bounded re-ask


async def test_copilot_llm_answer_accepted_and_plan_validated(sqlite_store):
    seed_two_assets(sqlite_store)
    from data_engineering.copilot.schemas import CopilotAnswer, QueryPlan
    plan = QueryPlan(intents=["thermal"], asset_ids=["A1", "SECRET-ASSET"], window_minutes=99999)
    main = ScriptedLLM("fake/glm", QueryPlan=plan,
                       CopilotAnswer=CopilotAnswer(answer="The latest inlet reading is shown in [E1]; nothing was "
                                                          "changed.", measured=["see [E1]"]))
    async with await client(make_api_app(sqlite_store.path, llms=(main, ScriptedLLM("fake/ds")))) as c:
        b = await chat(c, "How hot is it running?")
    assert b["interpretation"]["asset_ids"] == ["A1"]                          # unknown LLM asset dropped
    assert "60 minutes" in b["interpretation"]["time_basis"]                    # out-of-bounds window ignored
    assert b["agent_execution_summary"]["answer_source"] == "llm"


async def test_copilot_tool_failure_is_reported(sqlite_store, monkeypatch):
    seed_two_assets(sqlite_store)
    from data_engineering.copilot import tools as tools_module

    def boom(*a, **k):
        raise RuntimeError("db down")
    monkeypatch.setattr(tools_module.CopilotTools, "window_summary", boom)
    async with await client(make_api_app(sqlite_store.path)) as c:
        b = await chat(c, "What is the inlet temperature of A1?")
    assert b["agent_execution_summary"]["tools"]["window_summary"] == "FAILED"
    assert any("window_summary failed" in w for w in b["warnings"])
    assert b["agent_execution_summary"]["tools"]["latest"] == "OK"


class LLMFailure(Exception):
    def __new__(cls):
        from data_engineering.recommendations.llm import LLMError
        return LLMError("timeout", "slow provider")


def test_copilot_tools_cannot_write(sqlite_store):
    from data_engineering.storage.sqlite_store import SqliteTelemetryStore
    seed_two_assets(sqlite_store)
    ro = SqliteTelemetryStore(sqlite_store.path, read_only=True)
    try:
        assert ro.run_assets(RUN)
        with pytest.raises(Exception):
            ro.conn.execute("DELETE FROM assets")
        with pytest.raises(Exception):
            ro.add_decision({"decision_id": "d", "recommendation_id": "r", "decision": "DEFERRED", "reviewer": "x",
                             "note": None, "decided_at": T0})
    finally:
        ro.close()


def test_query_timeout_interrupts_long_statements(sqlite_store):
    from data_engineering.storage.sqlite_store import SqliteTelemetryStore
    s = SqliteTelemetryStore(sqlite_store.path, read_only=True, query_timeout_s=0.05)
    try:
        with pytest.raises(Exception, match="interrupt"):
            s._fetchall("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT COUNT(*) FROM c")
    finally:
        s.close()


def test_conversation_memory_is_bounded():
    from data_engineering.copilot.memory import ConversationStore, Turn
    now = [0.0]
    mem = ConversationStore(max_conversations=2, max_turns=3, ttl_s=10, clock=lambda: now[0])
    turn = Turn("q", [], False, None, None, [], "a")
    a = mem.append(None, turn)
    for _ in range(5):
        mem.append(a, turn)
    assert len(mem.get(a).turns) == 3
    mem.append(None, turn)
    mem.append(None, turn)
    assert len(mem) == 2 and mem.get(a) is None                       # least recently used evicted
    b = mem.append(None, turn)
    now[0] = 100.0
    assert mem.get(b) is None                                         # expired


def test_understanding_time_windows_and_assets():
    from datetime import datetime, timezone

    from data_engineering.copilot.understanding import resolve_window, understand
    known = ["SIM-NA-001", "SIM-NA-013"]
    end = datetime(2026, 1, 2, tzinfo=timezone.utc)
    u = understand("inlet trend of asset 13 over the last 2 hours", known)
    assert u.asset_ids == ["SIM-NA-013"] and u.window_minutes == 120 and "trend" in u.intents
    start, stop, basis = resolve_window(u, None, end, 60, 1440)
    assert stop == end and start == end - timedelta(hours=2) and "simulated event time" in basis
    u = understand("last 30 days please for SIM-NA-001", known, max_window_minutes=1440)
    assert u.window_minutes == 1440 and u.notes
    u = understand("between 2026-01-01T10:00:00Z and 2026-01-01T11:00:00Z for SIM-NA-001", known)
    s2, e2, basis = resolve_window(u, None, end, 60, 1440)
    assert (e2 - s2) == timedelta(hours=1) and basis == "explicit timestamps in the question"
