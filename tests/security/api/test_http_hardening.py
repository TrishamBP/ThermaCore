"""Security headers, request IDs, CORS, error bodies, unsupported methods."""

import httpx
import pytest

from conftest import make_api_app
from cooling_helpers import MINUTES_15, seed, series

pytestmark = pytest.mark.anyio


async def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_security_headers_on_api_responses(api_client):
    r = await api_client.get("/api/v1/health")
    assert r.headers["x-content-type-options"] == "nosniff"
    assert r.headers["x-frame-options"] == "DENY"
    assert r.headers["referrer-policy"] == "no-referrer"
    assert r.headers["content-security-policy"] == "default-src 'none'; frame-ancestors 'none'"
    assert r.headers["cache-control"] == "no-store"


async def test_headers_also_on_errors(api_client):
    r = await api_client.get("/api/v1/telemetry/cooling-state", params={"window_minutes": 0})
    assert r.status_code == 422 and r.headers["x-content-type-options"] == "nosniff"


async def test_docs_keep_base_headers_without_strict_csp(api_client):
    r = await api_client.get("/docs")
    assert r.status_code == 200 and "content-security-policy" not in r.headers
    assert r.headers["x-frame-options"] == "DENY"


@pytest.mark.parametrize("incoming, echoed", [
    ("client-abc_123.4", True),
    ("bad id with spaces", False),
    ("x" * 65, False),
    ("<script>", False),
])
async def test_request_id_is_echoed_only_when_well_formed(api_client, incoming, echoed):
    r = await api_client.get("/api/v1/health", headers={"X-Request-ID": incoming})
    assert (r.headers["x-request-id"] == incoming) is echoed
    if not echoed:
        assert len(r.headers["x-request-id"]) == 32


async def test_error_body_carries_the_request_id(api_client, sqlite_store):
    seed(sqlite_store, series("A1", MINUTES_15))            # so that an unknown asset is a 404
    r = await api_client.get("/api/v1/telemetry/cooling-state", params={"asset_id": "NOPE"},
                             headers={"X-Request-ID": "trace-1"})
    assert r.json()["request_id"] == "trace-1" == r.headers["x-request-id"]


async def test_unknown_route_and_method_use_error_response(api_client):
    r = await api_client.get("/api/v1/nope")
    assert r.status_code == 404 and r.json()["error"]["code"] == "not_found"
    r = await api_client.post("/api/v1/telemetry/cooling-state")
    assert r.status_code == 405 and r.json()["error"]["code"] == "method_not_allowed"


async def test_unexpected_exception_is_a_generic_500(api_app):
    from app.telemetry.dependencies import get_telemetry_service

    class Exploding:
        def cooling_state(self, query):
            raise RuntimeError("internal detail: password=hunter2")
    api_app.dependency_overrides[get_telemetry_service] = lambda: Exploding()
    # Starlette sends the 500 and then re-raises for the server to log; a real client only sees the 500.
    transport = httpx.ASGITransport(app=api_app, raise_app_exceptions=False)
    async with api_app.router.lifespan_context(api_app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.get("/api/v1/telemetry/cooling-state")
    assert r.status_code == 500 and r.json()["error"] == {"code": "internal_error",
                                                          "message": "Internal server error", "details": None}
    assert "hunter2" not in r.text


async def test_cors_disabled_by_default(api_client):
    r = await api_client.get("/api/v1/health", headers={"Origin": "http://evil.example"})
    assert "access-control-allow-origin" not in r.headers


async def test_cors_allows_only_configured_origins(sqlite_store):
    app = make_api_app(sqlite_store.path, cors_origins=["http://localhost:5173"])
    async with app.router.lifespan_context(app):
        async with await _client(app) as c:
            ok = await c.get("/api/v1/health", headers={"Origin": "http://localhost:5173"})
            other = await c.get("/api/v1/health", headers={"Origin": "http://evil.example"})
    assert ok.headers["access-control-allow-origin"] == "http://localhost:5173"
    assert "access-control-allow-origin" not in other.headers
