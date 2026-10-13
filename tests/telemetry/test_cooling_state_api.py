"""GET /api/v1/telemetry/cooling-state through the full app (lifespan, middleware, handlers)."""

import asyncio
import sqlite3

import httpx
import pytest

from conftest import make_api_app
from cooling_helpers import MINUTES_15, seed, series
from data_engineering.streaming.settings import load_consumer_settings

pytestmark = pytest.mark.anyio
URL = "/api/v1/telemetry/cooling-state"


EXPECTED_ROUTES = {"/api/v1/health", "/api/v1/telemetry/cooling-state", "/api/v1/dashboard/overview", "/api/v1/telemetry",
     "/api/v1/assets", "/api/v1/assets/{asset_id}", "/api/v1/assets/{asset_id}/telemetry",
     "/api/v1/cooling/predict", "/api/v1/cooling/optimize", "/api/v1/cooling-model/train",
     "/api/v1/cooling-model/evaluate", "/api/v1/cooling-model/predict", "/api/v1/cooling-optimizer/recommend",
     "/api/v1/recommendations", "/api/v1/recommendations/generate", "/api/v1/recommendations/{recommendation_id}",
     "/api/v1/recommendations/{recommendation_id}/decisions", "/api/v1/recommendations/analyses/{analysis_id}",
     "/api/v1/recommendations/agents/status", "/api/v1/alerts", "/api/v1/alerts/{alert_id}", "/api/v1/copilot/chat",
     "/api/v1/digital-twin/layout", "/api/v1/digital-twin/layout/{object_id}", "/api/v1/digital-twin/state",
     "/api/v1/digital-twin/overlays", "/api/v1/digital-twin/assets/{object_id}", "/api/v1/digital-twin/analyze",
     "/api/v1/digital-twin/stream", "/api/v1/energy/fan-peer-comparison"}


def error_code(response) -> str:
    return response.json()["error"]["code"]


async def test_data_center_and_asset_snapshots(api_client, sqlite_store):
    seed(sqlite_store, series("A1", MINUTES_15, inlet_temperature_c=28.0))
    seed(sqlite_store, series("A2", MINUTES_15))
    dc = await api_client.get(URL)
    assert dc.status_code == 200
    body = dc.json()
    assert body["scope"]["level"] == "data_center" and body["state"] == "HIGH_THERMAL_LOAD"
    assert {a["asset_id"] for a in body["assets"]} == {"A1", "A2"}
    one = (await api_client.get(URL, params={"asset_id": "A2", "window_minutes": 10})).json()
    assert one["scope"]["asset_id"] == "A2" and one["window_minutes"] == 10 and one["state"] == "NORMAL"
    assert one["metrics"]["inlet_temperature_c"]["observed_points"] == 10


async def test_as_of_is_normalized_to_utc(api_client, sqlite_store):
    seed(sqlite_store, series("A1", MINUTES_15))
    body = (await api_client.get(URL, params={"asset_id": "A1", "as_of": "2026-01-01T14:07:00+02:00"})).json()
    assert body["as_of"] == "2026-01-01T12:07:00Z"
    assert body["metrics"]["inlet_temperature_c"]["observed_points"] == 8


async def test_empty_store_is_200_insufficient_data(api_client):
    r = await api_client.get(URL)
    assert r.status_code == 200 and r.json()["state"] == "INSUFFICIENT_DATA"


@pytest.mark.parametrize("params, status, code", [
    ({"asset_id": "NOPE"}, 404, "unknown_asset"),
    ({"run_id": "sim-nope"}, 404, "unknown_run"),
    ({"window_minutes": 0}, 422, "validation_error"),
    ({"window_minutes": 1441}, 422, "validation_error"),
    ({"window_minutes": "abc"}, 422, "validation_error"),
    ({"asset_id": "bad id; drop"}, 422, "validation_error"),
    ({"as_of": "2026-01-01T12:00:00"}, 422, "validation_error"),     # no timezone
    ({"as_of": "not-a-date"}, 422, "validation_error"),
    ({"unexpected": "x"}, 422, "validation_error"),                  # unknown parameters are rejected
])
async def test_invalid_requests(api_client, sqlite_store, params, status, code):
    seed(sqlite_store, series("A1", MINUTES_15))
    r = await api_client.get(URL, params=params)
    assert r.status_code == status and error_code(r) == code
    assert r.json()["request_id"]


async def test_validation_errors_do_not_echo_input(api_client):
    r = await api_client.get(URL, params={"asset_id": "<script>alert(1)</script>"})
    assert r.status_code == 422 and "<script>" not in r.text
    assert r.json()["error"]["details"][0]["loc"] == ["query", "asset_id"]


async def test_uninitialized_store_is_503(tmp_path):
    app = make_api_app(tmp_path / "empty.db")
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(URL)
    assert r.status_code == 503 and error_code(r) == "store_not_initialized"


async def test_unreachable_database_is_503_without_leaking_credentials(caplog):
    pytest.importorskip("psycopg")
    settings = load_consumer_settings(env={"TELEMETRY_DB_BACKEND": "postgres",
                                           "TELEMETRY_POSTGRES_URL": "postgresql://dco:s3cret-pw@127.0.0.1:1/dco"})
    app = make_api_app(store_settings=settings, db_connect_timeout_s=1)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            r, health = await c.get(URL), await c.get("/api/v1/health")
    assert r.status_code == 503 and error_code(r) == "database_unavailable"
    assert health.status_code == 503 and error_code(health) == "database_unavailable"
    assert "s3cret-pw" not in r.text and "s3cret-pw" not in caplog.text


async def test_query_failure_mid_request_is_503(api_app, api_client, sqlite_store):
    from app.database import get_db_session
    seed(sqlite_store, series("A1", MINUTES_15))

    class BrokenStore:
        def __getattr__(self, name):
            return getattr(sqlite_store, name)

        def readings_window(self, *args):
            raise sqlite3.OperationalError("disk I/O error at /secret/path")
    api_app.dependency_overrides[get_db_session] = lambda: BrokenStore()
    r = await api_client.get(URL)
    assert r.status_code == 503 and error_code(r) == "database_unavailable"
    assert "/secret/path" not in r.text and r.json()["error"]["message"] == "Telemetry store query failed"


async def test_concurrent_requests_do_not_block_each_other(api_client, sqlite_store):
    seed(sqlite_store, series("A1", MINUTES_15))
    responses = await asyncio.gather(*(api_client.get(URL, params={"asset_id": "A1"}) for _ in range(20)))
    assert {r.status_code for r in responses} == {200}
    assert len({r.headers["x-request-id"] for r in responses}) == 20


async def test_legacy_path_still_works_but_is_not_documented(api_client, sqlite_store):
    seed(sqlite_store, series("A1", MINUTES_15))
    legacy = await api_client.get("/api/telemetry/cooling-state", params={"asset_id": "A1"})
    assert legacy.status_code == 200 and legacy.json()["scope"]["asset_id"] == "A1"
    paths = (await api_client.get("/openapi.json")).json()["paths"]
    assert set(paths) == EXPECTED_ROUTES and all(p.startswith("/api/v1/") for p in paths)


async def test_health(api_client):
    r = await api_client.get("/api/v1/health")
    assert r.json() == {"status": "ok", "database": "sqlite", "schema_version": 2}
