"""Digital twin: layout model/validation, live state, overlays, inspection, analysis, SSE stream."""

import asyncio
import json
import time

import httpx
import pytest

from conftest import make_api_app
from cooling_helpers import event, seed, series
from data_engineering.digital_twin.layout import LayoutError, LayoutProvider, parse_layout
from app.common.security.auth import AssetScope
from app.digital_twin.stream import TwinBroadcaster, diff_snapshots

V1 = "/api/v1/digital-twin"

LAYOUT = """
layout: {revision: t-1, demonstration: true, units: m}
overlay_bands:
  temperature: [{max: 22.0, status: cool}, {max: null, status: hot}]
objects:
  - {id: dc, type: facility, name: DC, position: [0, 0, 0], dimensions: [10, 3, 10]}
  - {id: room-a, parent_id: dc, type: room, name: Room A, position: [0, 1, 0], dimensions: [8, 3, 8]}
  - {id: aisle-a, parent_id: room-a, type: aisle, name: Aisle A, position: [0, 0, 0], dimensions: [6, 0.1, 1]}
  - {id: rack-a, parent_id: aisle-a, type: rack, name: Rack A, position: [0, 1, 1], dimensions: [0.6, 2, 1.2]}
  - {id: srv-1, parent_id: rack-a, type: server, asset_id: A1, name: S1, position: [0, 0.5, 1], dimensions: [0.5, 0.1, 1]}
  - {id: srv-2, parent_id: rack-a, type: server, asset_id: A2, name: S2, position: [0, 0.8, 1], dimensions: [0.5, 0.1, 1]}
  - {id: srv-3, parent_id: rack-a, type: server, asset_id: A3, name: S3 (no telemetry), position: [0, 1.1, 1], dimensions: [0.5, 0.1, 1]}
  - {id: crah, parent_id: room-a, type: cooling_unit, name: CRAH, position: [3, 1, 0], dimensions: [1, 2, 2]}
"""


@pytest.fixture
def layout_file(tmp_path):
    p = tmp_path / "layout.yaml"
    p.write_text(LAYOUT, encoding="utf-8")
    return p


def seed_assets(store):
    seed(store, series("A1", range(30), inlet_temperature_c=21.0))
    seed(store, series("A2", range(20), inlet_temperature_c=25.5))          # stops early -> stale


class _Client:
    def __init__(self, app):
        self.app = app

    async def __aenter__(self):
        self.ctx = self.app.router.lifespan_context(self.app)
        await self.ctx.__aenter__()
        self.c = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test")
        return self.c

    async def __aexit__(self, *exc):
        await self.c.aclose()
        await self.ctx.__aexit__(*exc)


def twin_app(store, layout_path, **kw):
    return make_api_app(store.path, layout_provider=LayoutProvider(layout_path), **kw)


# ------------------------------------------------------------------ layout model

def test_layout_parses_hierarchy_and_version(layout_file):
    lay = LayoutProvider(layout_file).get()
    assert lay.demonstration and lay.version.startswith("t-1-")
    assert [a.id for a in lay.ancestors("srv-1")] == ["rack-a", "aisle-a", "room-a", "dc"]
    assert {o.id for o in lay.descendants("rack-a")} == {"srv-1", "srv-2", "srv-3"}
    assert lay.by_asset()["A2"].id == "srv-2" and lay.room_of("srv-1") == "room-a"


@pytest.mark.parametrize("change, message", [
    (("id: srv-2,", "id: srv-1,"), "duplicate object id"),
    (("asset_id: A2", "asset_id: A1"), "already mapped"),
    (("parent_id: rack-a, type: server, asset_id: A1", "parent_id: nope, type: server, asset_id: A1"), "does not exist"),
    (("type: rack, name: Rack A", "type: shelf, name: Rack A"), "invalid type"),
    (("parent_id: aisle-a, type: rack", "parent_id: srv-1, type: rack"), "cannot be placed under"),
    (("dimensions: [0.6, 2, 1.2]", "dimensions: [0.6, -2, 1.2]"), "must be positive"),
    (("position: [0, 1, 1]", "position: [0, .nan, 1]"), "finite"),
    (("{max: null, status: hot}", "{max: 30, status: hot}"), "final open band"),
])
def test_layout_validation(change, message):
    with pytest.raises(LayoutError, match=message):
        parse_layout(LAYOUT.replace(*change))


def test_layout_cycle_is_rejected():
    text = LAYOUT.replace("{id: dc, type: facility, name: DC,", "{id: dc, parent_id: room-a, type: facility, name: DC,")
    with pytest.raises(LayoutError, match="cannot be placed under|cyclic"):
        parse_layout(text)


def test_layout_reloads_when_the_file_changes(layout_file):
    provider = LayoutProvider(layout_file)
    v1 = provider.get().version
    assert provider.get() is provider.get()                                   # cached
    time.sleep(0.01)
    layout_file.write_text(LAYOUT.replace("Room A", "Room A (renamed)") + "\n", encoding="utf-8")
    assert provider.get().version != v1


def test_shipped_demo_layout_is_valid_and_labelled():
    lay = LayoutProvider().get()
    assert lay.demonstration is True
    assert sorted(lay.by_asset()) == [f"SIM-NA-{i:03d}" for i in range(1, 21)]


# ------------------------------------------------------------------ API



@pytest.mark.anyio
async def test_layout_api(sqlite_store, layout_file):
    seed_assets(sqlite_store)
    async with _Client(twin_app(sqlite_store, layout_file)) as c:
        b = (await c.get(f"{V1}/layout")).json()
        assert b["demonstration"] and "DEMONSTRATION" in b["notice"] and b["count"] == 8
        objs = {o["id"]: o for o in b["objects"]}
        assert objs["srv-1"]["telemetry_available"] and not objs["srv-3"]["telemetry_available"]
        assert objs["srv-1"]["dimensions"] == {"width": 0.5, "height": 0.1, "depth": 1.0}
        assert (await c.get(f"{V1}/layout", params={"layout_version": b["layout_version"]})).json()["unchanged"]
        no_servers = (await c.get(f"{V1}/layout", params={"include_servers": False})).json()
        assert all(o["type"] != "server" for o in no_servers["objects"])
        one = (await c.get(f"{V1}/layout/srv-2")).json()
        assert [p["id"] for p in one["parents"]] == ["rack-a", "aisle-a", "room-a", "dc"]
        assert (await c.get(f"{V1}/layout/nope")).json()["error"]["code"] == "unknown_layout_object"
        assert (await c.get(f"{V1}/layout", params={"room_id": "nope"})).status_code == 404


@pytest.mark.anyio
async def test_missing_layout_is_a_clear_error(sqlite_store, tmp_path):
    seed_assets(sqlite_store)
    async with _Client(twin_app(sqlite_store, tmp_path / "missing.yaml")) as c:
        r = await c.get(f"{V1}/layout")
    assert r.status_code == 503 and r.json()["error"]["code"] == "layout_not_configured"


@pytest.mark.anyio
async def test_state_api(sqlite_store, layout_file):
    seed_assets(sqlite_store)
    async with _Client(twin_app(sqlite_store, layout_file)) as c:
        b = (await c.get(f"{V1}/state")).json()
        by = {s["asset_id"]: s for s in b["assets"]}
        assert by["A1"]["freshness"] == "FRESH" and by["A1"]["thermal_status"] == "WITHIN_LIMIT"
        assert by["A1"]["values"]["inlet_temperature_c"] == {"value": 21.0, "unit": "degC", "kind": "measured"}
        assert by["A1"]["values"]["temperature_delta_c"]["kind"] == "calculated"
        assert by["A1"]["values"]["cooling_efficiency"]["kind"] == "unavailable"
        assert by["A2"]["freshness"] == "STALE" and by["A2"]["thermal_status"] == "UNKNOWN"    # stale -> no status
        assert by["A3"]["status"] == "NO_TELEMETRY" and by["A3"]["values"] == {}               # nothing invented
        rack = next(a for a in b["aggregates"] if a["object_id"] == "rack-a")
        assert rack["servers_reporting"] == 1 and rack["max_inlet_temperature_c"] == 21.0
        newer = (await c.get(f"{V1}/state", params={"since": b["state_cursor"]})).json()
        assert newer["assets"] == []                                                           # incremental
        only = (await c.get(f"{V1}/state", params={"asset_ids": "A2"})).json()
        assert [s["asset_id"] for s in only["assets"]] == ["A2"]


@pytest.mark.anyio
async def test_state_respects_asset_scope(sqlite_store, layout_file):
    seed_assets(sqlite_store)
    async with _Client(twin_app(sqlite_store, layout_file, allowed_assets=["A1"])) as c:
        by = {s["asset_id"]: s for s in (await c.get(f"{V1}/state")).json()["assets"]}
        assert by["A2"]["status"] == "RESTRICTED" and by["A2"]["values"] == {}
        assert (await c.get(f"{V1}/assets/srv-2")).status_code == 403


@pytest.mark.anyio
async def test_overlays(sqlite_store, layout_file):
    seed_assets(sqlite_store)
    async with _Client(twin_app(sqlite_store, layout_file)) as c:
        t = (await c.get(f"{V1}/overlays", params={"type": "temperature"})).json()
        by = {i["asset_id"]: i for i in t["items"]}
        assert by["A1"]["status"] == "cool" and by["A1"]["is_predicted"] is False and by["A1"]["unit"] == "degC"
        assert by["A2"]["status"] == "STALE" and by["A3"]["status"] == "NO_TELEMETRY" and by["A3"]["value"] is None
        assert "not equipment safety limits" in t["bands_note"]
        p = (await c.get(f"{V1}/overlays", params={"type": "predicted_temperature"})).json()
        assert {i["asset_id"]: i["status"] for i in p["items"]}["A1"] == "UNAVAILABLE"          # no model trained
        assert all(i["is_predicted"] for i in p["items"])
        e = (await c.get(f"{V1}/overlays", params={"type": "energy"})).json()
        assert any("not cooling power" in w for w in e["warnings"])
        ce = (await c.get(f"{V1}/overlays", params={"type": "cooling_efficiency"})).json()
        assert all(i["status"] in ("UNAVAILABLE", "NO_TELEMETRY") and i["value"] is None for i in ce["items"])
        air = (await c.get(f"{V1}/overlays", params={"type": "airflow"})).json()
        assert any("direction" in w for w in air["warnings"])
        hist = (await c.get(f"{V1}/overlays", params={"type": "data_quality",
                                                      "timestamp": "2026-01-01T12:10:00Z"})).json()
        assert {i["asset_id"]: i["status"] for i in hist["items"]}["A2"] == "FRESH"           # as of 12:10
        r = await c.get(f"{V1}/overlays", params={"type": "cfd"})
        assert r.status_code == 422


@pytest.mark.anyio
async def test_inspect_server_and_rack(sqlite_store, layout_file):
    seed_assets(sqlite_store)
    async with _Client(twin_app(sqlite_store, layout_file)) as c:
        s = (await c.get(f"{V1}/assets/srv-1")).json()
        assert s["kind"] == "asset" and s["asset_id"] == "A1"
        assert [p["id"] for p in s["parents"]] == ["rack-a", "aisle-a", "room-a", "dc"]
        assert s["forecast"]["status"] == "UNAVAILABLE" and s["history"]["series"][0]["points"]
        assert s["recommendations"] == [] and s["alerts"] == []
        by_asset = (await c.get(f"{V1}/assets/A1")).json()                     # asset id also accepted
        assert by_asset["object"]["id"] == "srv-1"
        rack = (await c.get(f"{V1}/assets/rack-a")).json()
        assert rack["kind"] == "aggregated" and rack["aggregate"]["method"]
        r = await c.get(f"{V1}/assets/srv-1", params={"start_time": "2026-01-01T13:00:00Z",
                                                      "end_time": "2026-01-01T12:00:00Z"})
        assert r.status_code == 422


@pytest.mark.anyio
async def test_analyze_uses_the_copilot(sqlite_store, layout_file):
    seed_assets(sqlite_store)
    async with _Client(twin_app(sqlite_store, layout_file)) as c:
        b = (await c.post(f"{V1}/analyze", json={"object_id": "rack-a", "question": "Which server is hottest?",
                                                 "analysis_type": "thermal"})).json()
        assert b["asset_ids"] == ["A1", "A2", "A3"] and b["recommendation_only"] is True
        assert b["execution"]["tools"] and b["evidence"]
        one = (await c.post(f"{V1}/analyze", json={"asset_id": "A1", "question": "Forecast it",
                                                   "analysis_type": "forecast"})).json()
        assert one["execution"]["tools"]["forecast"] == "UNAVAILABLE"
        r = await c.post(f"{V1}/analyze", json={"question": "x"})
        assert r.status_code == 422


@pytest.mark.anyio
async def test_stream_endpoint_returns_sse(sqlite_store, layout_file):
    seed_assets(sqlite_store)
    async with _Client(twin_app(sqlite_store, layout_file)) as c:
        r = await c.get(f"{V1}/stream", params={"max_events": 1})
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/event-stream")
    first = r.text.split("\n\n")[0]
    assert first.startswith("id: ") and "event: heartbeat" in first
    assert json.loads(first.split("data: ", 1)[1])["event"] == "heartbeat"


# ------------------------------------------------------------------ stream mechanics

def snap(ts="t1", status="OK", thermal="WITHIN_LIMIT", alerts=(), recs=(), version="v1"):
    return {"layout_version": version,
            "assets": {"A1": {"object_id": "srv-1", "asset_id": "A1", "status": status, "thermal_status": thermal,
                              "last_telemetry_timestamp": ts},
                       "A2": {"object_id": "srv-2", "asset_id": "A2", "status": "OK", "thermal_status": "WITHIN_LIMIT",
                              "last_telemetry_timestamp": "t0"}},
            "alerts": {a: {"alert_id": a, "asset_id": "A2"} for a in alerts},
            "recommendations": {r: {"recommendation_id": r, "asset_id": "A1"} for r in recs}}


def test_diff_snapshots():
    assert diff_snapshots(None, snap()) == []
    events = diff_snapshots(snap(), snap(ts="t2", thermal="APPROACHING_LIMIT", alerts=["al1"], recs=["r1"],
                                         version="v2"))
    kinds = [(e[0], e[1]) for e in events]
    assert ("layout_changed", None) in kinds and ("telemetry_update", "A1") in kinds
    assert ("asset_status_change", "A1") in kinds and ("alert_created", "A2") in kinds
    assert ("recommendation_created", "A1") in kinds and ("telemetry_update", "A2") not in kinds


@pytest.mark.anyio
async def test_broadcaster_scope_queue_bounds_and_replay():
    snaps = iter([snap(), snap(ts="t2", alerts=["al1"])])
    b = TwinBroadcaster(lambda: next(snaps), poll_interval_s=3600, queue_size=2, buffer_size=3)
    only_a1 = b.subscribe(AssetScope(["A1"]))
    everyone = b.subscribe(AssetScope([]))
    await b.close()                               # stop the background poller; drive polls by hand
    await b.poll_once()
    await b.poll_once()
    seen_a1 = [only_a1.queue.get_nowait()["event"] for _ in range(only_a1.queue.qsize())]
    assert seen_a1 == ["telemetry_update"]                                  # A2's alert is filtered out
    assert {"telemetry_update", "alert_created"} == {everyone.queue.get_nowait()["event"]
                                                     for _ in range(everyone.queue.qsize())}
    b.publish([("telemetry_update", "A1", {}) for _ in range(5)])           # overflow a 2-slot queue
    assert everyone.dropped > 0
    events = [everyone.queue.get_nowait() for _ in range(everyone.queue.qsize())]
    assert events[-1]["event"] == "resync_required"
    late = b.subscribe(AssetScope([]), last_event_id=1)                     # too old for the 3-event buffer
    assert late.queue.get_nowait()["event"] == "resync_required"
    recent_id = b.buffer[-2]["id"]
    resume = b.subscribe(AssetScope([]), last_event_id=recent_id)
    assert resume.queue.get_nowait()["id"] == b.buffer[-1]["id"]           # only buffered events are replayed
    b.unsubscribe(only_a1)
    assert only_a1.id not in b.subscribers
    await b.close()


@pytest.mark.anyio
async def test_broadcaster_survives_store_errors_and_limits_subscribers():
    def broken():
        raise RuntimeError("db down")
    b = TwinBroadcaster(broken, poll_interval_s=3600, max_subscribers=1)
    s = b.subscribe(AssetScope([]))
    await b.close()
    await b.poll_once()
    assert s.queue.get_nowait()["event"] == "resync_required"
    with pytest.raises(OverflowError):
        b.subscribe(AssetScope([]))
    await asyncio.sleep(0)
    await b.close()


def test_twin_has_no_control_routes():
    from app.digital_twin.router import router
    for route in router.routes:
        assert not ({"PUT", "DELETE", "PATCH"} & route.methods)
        assert route.path.split("/")[-1] not in ("control", "command", "setpoint")
    assert event("A1", 0).reading["asset_id"] == "A1"
