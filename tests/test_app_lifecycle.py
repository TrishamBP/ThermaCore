"""App factory and lifespan: startup validation, shared objects, settings."""

import httpx
import pytest

from conftest import make_api_app
from data_engineering.cooling.config import CoolingConfigError

pytestmark = pytest.mark.anyio


async def test_lifespan_creates_shared_objects(sqlite_store):
    import anyio.to_thread
    app = make_api_app(sqlite_store.path, threadpool_tokens=7)
    async with app.router.lifespan_context(app):
        assert app.state.store_factory.describe().startswith("sqlite:")
        assert app.state.cooling_config.window_minutes == 15
        assert anyio.to_thread.current_default_thread_limiter().total_tokens == 7
    anyio.to_thread.current_default_thread_limiter().total_tokens = 40


async def test_invalid_cooling_config_stops_startup(sqlite_store, tmp_path, monkeypatch):
    bad = tmp_path / "cooling.yaml"
    bad.write_text("window_minutes: 0\n", encoding="utf-8")
    monkeypatch.setenv("COOLING_STATE_CONFIG", str(bad))
    from app.config import AppSettings
    from app.main import create_app
    from data_engineering.streaming.settings import load_consumer_settings
    app = create_app(AppSettings(environment="test"),
                     store_settings=load_consumer_settings(env={"TELEMETRY_SQLITE_PATH": str(sqlite_store.path)}))
    with pytest.raises(CoolingConfigError, match="window_minutes"):
        async with app.router.lifespan_context(app):
            pass


async def test_database_down_at_startup_does_not_stop_the_app(tmp_path):
    app = make_api_app(tmp_path / "missing" / "dir" / "never.db")
    async with app.router.lifespan_context(app):                 # starts fine
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get("/api/v1/health")
    assert r.status_code in (200, 503)
    assert r.json().get("status") == "degraded" or r.json()["error"]["code"] == "database_unavailable"


async def test_docs_can_be_disabled(sqlite_store):
    app = make_api_app(sqlite_store.path, docs_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            assert (await c.get("/docs")).status_code == 404
            assert (await c.get("/openapi.json")).status_code == 404


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


def test_settings_validation(monkeypatch):
    from pydantic import ValidationError

    from app.config import AppSettings
    monkeypatch.setenv("API_DB_CONNECT_TIMEOUT_S", "0")
    with pytest.raises(ValidationError):
        AppSettings()
    monkeypatch.setenv("API_DB_CONNECT_TIMEOUT_S", "5")
    monkeypatch.setenv("API_CORS_ORIGINS", '["http://localhost:5173"]')
    s = AppSettings()
    assert s.db_connect_timeout_s == 5 and s.cors_origins == ["http://localhost:5173"]


def test_module_level_app_is_importable():
    from app.main import app
    assert set(app.openapi()["paths"]) == EXPECTED_ROUTES and all(p.startswith("/api/v1/") for p in app.openapi()["paths"])
