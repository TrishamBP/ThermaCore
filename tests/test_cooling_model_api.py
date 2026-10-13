"""/api/v1/cooling-model/* and /api/v1/cooling-optimizer/recommend through the full app."""

import httpx
import pytest

from conftest import make_api_app
from cooling_model_helpers import seed_world, settings

pytestmark = pytest.mark.anyio
AS_OF = "2026-01-01T20:20:00Z"


def error_code(response) -> str:
    return response.json()["error"]["code"]



@pytest.fixture
async def trained_client(sqlite_store, tmp_path):
    seed_world(sqlite_store, "observational")
    app = make_api_app(sqlite_store.path, cooling_model_settings=settings(tmp_path / "models"),
                       model_training_enabled=True)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            trained = await client.post("/api/v1/cooling-model/train", json={})
            assert trained.status_code == 200, trained.text
            yield client


async def test_no_model_returns_503(api_client, sqlite_store):
    seed_world(sqlite_store, "observational", assets=1, minutes=30)
    for r in (await api_client.get("/api/v1/cooling-model/evaluate"),
              await api_client.get("/api/v1/cooling-model/predict", params={"asset_id": "T-00"}),
              await api_client.post("/api/v1/cooling-optimizer/recommend", json={"asset_id": "T-00"})):
        assert r.status_code == 503 and error_code(r) == "model_not_trained"


async def test_training_is_disabled_by_default(api_client):
    r = await api_client.post("/api/v1/cooling-model/train", json={})
    assert r.status_code == 403 and error_code(r) == "training_disabled"


async def test_train_evaluate_predict_recommend(trained_client):
    ev = (await trained_client.get("/api/v1/cooling-model/evaluate")).json()
    assert ev["model_label"] == "forecasting_baseline" and ev["horizons_minutes"] == [5, 10]
    assert set(ev["evaluation"]["5"]["test"]) == {"model", "persistence", "rmse_skill_vs_persistence"}
    assert any("SIMULATED" in x for x in ev["limitations"])

    p = await trained_client.get("/api/v1/cooling-model/predict", params={"asset_id": "T-00", "as_of": AS_OF})
    assert p.status_code == 200
    body = p.json()
    assert "_features" not in body and "_series" not in body
    assert [f["horizon_minutes"] for f in body["forecasts"]] == [5, 10]
    assert body["forecasts"][0]["forecast_timestamp"] == "2026-01-01T20:25:00+00:00"

    r = await trained_client.post("/api/v1/cooling-optimizer/recommend", json={"asset_id": "T-00", "as_of": AS_OF})
    assert r.status_code == 200
    rec = r.json()
    assert rec["status"] == "UNAVAILABLE" and rec["recommended_setting"] is None
    assert rec["recommendation_only"] is True
    assert {x["code"] for x in rec["reasons"]} >= {"RESPONSE_MODEL_NOT_VALIDATED", "ENERGY_OBJECTIVE_MISSING"}


async def test_errors(trained_client):
    r = await trained_client.get("/api/v1/cooling-model/predict", params={"asset_id": "NOPE"})
    assert r.status_code == 404 and error_code(r) == "unknown_asset"
    r = await trained_client.get("/api/v1/cooling-model/predict", params={"asset_id": "T-00", "run_id": "sim-x"})
    assert r.status_code == 404 and error_code(r) == "unknown_run"
    r = await trained_client.get("/api/v1/cooling-model/predict", params={"asset_id": "T-00", "as_of": "2025-01-01T00:00:00Z"})
    assert r.status_code == 422 and error_code(r) == "insufficient_data"
    r = await trained_client.get("/api/v1/cooling-model/predict", params={"asset_id": "T-00", "as_of": "2026-01-01T20:20:00"})
    assert r.status_code == 422 and error_code(r) == "validation_error"           # naive datetime
    r = await trained_client.post("/api/v1/cooling-optimizer/recommend", json={"asset_id": "T-00", "fan": 1})
    assert r.status_code == 422 and error_code(r) == "validation_error"           # unknown fields rejected
    r = await trained_client.post("/api/v1/cooling-model/train", json={"run_ids": ["sim-unknown"]})
    assert r.status_code == 404 and error_code(r) == "unknown_run"


async def test_invalid_artifact_returns_503(sqlite_store, tmp_path):
    models = tmp_path / "models"
    (models / "crm-broken").mkdir(parents=True)
    (models / "ACTIVE").write_text("crm-broken")
    app = make_api_app(sqlite_store.path, cooling_model_settings=settings(models))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            r = await client.get("/api/v1/cooling-model/evaluate")
    assert r.status_code == 503 and error_code(r) == "model_artifact_invalid"


async def test_training_with_insufficient_data(sqlite_store, tmp_path):
    seed_world(sqlite_store, "observational", assets=1, minutes=90)
    app = make_api_app(sqlite_store.path, cooling_model_settings=settings(tmp_path / "models"),
                       model_training_enabled=True)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            r = await client.post("/api/v1/cooling-model/train", json={})
    assert r.status_code == 422 and error_code(r) == "insufficient_data"


async def test_openapi_lists_the_routes(api_client):
    paths = (await api_client.get("/openapi.json")).json()["paths"]
    assert {"/api/v1/cooling-model/train", "/api/v1/cooling-model/evaluate", "/api/v1/cooling-model/predict",
            "/api/v1/cooling-optimizer/recommend"} <= set(paths)
