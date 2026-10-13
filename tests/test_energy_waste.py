"""Energy-waste screening: fan over-speed vs comparable peers (relative only, no kW)."""

import dataclasses
from datetime import timedelta

import httpx
import pytest

from conftest import make_api_app
from cooling_helpers import RUN, T0, event, seed, series
from data_engineering.energy.fan_peer import FanPeerConfig, analyze
from data_engineering.recommendations.config import RecommendationConfigError, WorkflowConfig, load_workflow_config
from recommendation_helpers import engine

END = T0 + timedelta(minutes=119)


def seed_fleet(store, high_fan=3600.0, high_type="Compute", inlet=21.0, peers_workload=0.5, minutes=120):
    """A1/A2 normal fans; A3 runs faster at the same workload and inlet."""
    seed(store, series("A1", range(minutes), fan_speed_rpm=3000.0, workload_intensity=peers_workload,
                       inlet_temperature_c=inlet))
    seed(store, series("A2", range(minutes), fan_speed_rpm=3050.0, workload_intensity=peers_workload,
                       inlet_temperature_c=inlet))
    seed(store, [event("A3", m, asset_type=high_type, fan_speed_rpm=high_fan, workload_intensity=0.5,
                       inlet_temperature_c=inlet) for m in range(minutes)])


def result_for(store, asset, cfg=FanPeerConfig(lookback_minutes=120), end=END):
    return {r["asset_id"]: r for r in analyze(store, RUN, end, cfg)["results"]}[asset]


# ------------------------------------------------------------------ method

def test_faster_fan_than_comparable_peers_is_elevated(sqlite_store):
    seed_fleet(sqlite_store)
    r = result_for(sqlite_store, "A3")
    assert r["status"] == "ELEVATED" and r["peer_median_fan_rpm"] == pytest.approx(3025.0)
    assert r["excess_fraction"] == pytest.approx(3600 / 3025 - 1, abs=1e-3)
    assert r["relative_fan_power"] == pytest.approx((3600 / 3025) ** 3, abs=1e-3)
    assert set(r["reference_assets"]) == {"A1", "A2"} and r["persistence"]["windows_elevated"] >= 2
    assert result_for(sqlite_store, "A1")["status"] in ("WITHIN_PEER_RANGE", "BELOW_PEERS")


def test_small_differences_are_not_flagged(sqlite_store):
    seed_fleet(sqlite_store, high_fan=3200.0)                           # +5.8 %: below the 15 % threshold
    assert result_for(sqlite_store, "A3")["status"] == "WITHIN_PEER_RANGE"


def test_different_cooling_or_asset_type_is_not_compared(sqlite_store):
    seed_fleet(sqlite_store, high_type="GPU")
    r = result_for(sqlite_store, "A3")
    assert r["status"] == "INSUFFICIENT_PEERS" and r["peer_median_fan_rpm"] is None
    assert r["excess_fraction"] is None and "group" in r["reasons"][0]


def test_peers_at_different_workload_are_not_comparable(sqlite_store):
    seed_fleet(sqlite_store, peers_workload=0.9)
    assert result_for(sqlite_store, "A3")["status"] == "INSUFFICIENT_PEERS"


def test_windows_with_missing_readings_are_skipped(sqlite_store):
    seed_fleet(sqlite_store)
    seed(sqlite_store, [event("A4", m, status="missing") for m in range(120)])
    assert result_for(sqlite_store, "A4")["status"] == "INSUFFICIENT_DATA"


def test_config_validation(tmp_path):
    good = load_workflow_config()
    assert good.fan_peer.group_by == ("cooling_type", "asset_type") and good.min_elevated_windows == 2
    p = tmp_path / "r.yaml"
    p.write_text("energy_waste: {peer_group_by: [rack_id]}", encoding="utf-8")
    with pytest.raises(RecommendationConfigError, match="peer_group_by"):
        load_workflow_config(p)
    p.write_text("energy_waste: {kw_per_rpm: 3}", encoding="utf-8")
    with pytest.raises(RecommendationConfigError, match="energy_waste"):
        load_workflow_config(p)


# ------------------------------------------------------------------ recommendation rule

def wf(**kw):
    return dataclasses.replace(WorkflowConfig(), fan_peer=FanPeerConfig(lookback_minutes=120), **kw)


def by_type(result):
    return {r["type"]: r for r in result["recommendations"]}


def test_energy_waste_recommendation_is_relative_only(sqlite_store):
    seed_fleet(sqlite_store)
    rec = by_type(engine(sqlite_store.path, workflow=wf()).analyze("A3", RUN, END))["ENERGY_WASTE"]
    assert rec["status"] == "REQUIRES_REVIEW" and rec["severity"] == "INFO"
    assert rec["estimated_energy_impact"] is None and rec["energy_unit"] is None          # never kW
    assert rec["energy_impact_status"].startswith("RELATIVE ONLY: fan +19.0% vs peer median")
    rel = rec["relative_energy_indicator"]
    assert rel["kw"] is None and rel["relative_fan_power"] == pytest.approx(1.6855, abs=1e-3)
    assert "Do not lower the fan speed" in rec["recommended_action"] and rec["candidate_setting"] is None
    assert any("affinity-law" in x for x in rec["uncertainty"]["limitations"])
    metrics = {e["metric"]: e["kind"] for e in rec["evidence"]}
    assert metrics["fan_speed_rpm.window_mean"] == "measured" and metrics["fan_speed_rpm.peer_median"] == "derived"
    assert "power_consumption_kw.latest" not in metrics                     # server power is not used


def test_energy_waste_inconclusive_with_thermal_warning(sqlite_store):
    seed_fleet(sqlite_store, inlet=28.0)                                    # above the configured 27 C limit
    rec = by_type(engine(sqlite_store.path, workflow=wf()).analyze("A3", RUN, END))["ENERGY_WASTE"]
    assert rec["status"] == "INCONCLUSIVE" and any("higher fan speed may be warranted" in m
                                                   for m in rec["missing_evidence"])


def test_energy_waste_needs_persistence(sqlite_store):
    seed_fleet(sqlite_store)
    rec = by_type(engine(sqlite_store.path, workflow=wf(min_elevated_windows=50)).analyze("A3", RUN, END))
    assert rec["ENERGY_WASTE"]["status"] == "INCONCLUSIVE"


def test_no_peers_means_not_applicable(sqlite_store):
    seed_fleet(sqlite_store, high_type="GPU")
    result = engine(sqlite_store.path, workflow=wf()).analyze("A3", RUN, END)
    assert "ENERGY_WASTE" not in by_type(result)
    reason = {n["type"]: n["reason"] for n in result["execution"]["not_applicable"]}["ENERGY_WASTE"]
    assert "cannot screen" in reason and "not cooling power" in reason


# ------------------------------------------------------------------ API, copilot, twin

@pytest.fixture
def two_hour_lookback(monkeypatch):
    """The fixture data spans 2 hours; the shipped lookback is 24 h."""
    import app.main as main
    monkeypatch.setattr(main, "load_workflow_config", lambda *a, **k: wf())


@pytest.fixture
def app(sqlite_store, two_hour_lookback):
    return make_api_app(sqlite_store.path)


async def run(app, method, url, **kw):
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            return await c.request(method, url, **kw)


@pytest.mark.anyio
async def test_fleet_endpoint(app, sqlite_store):
    seed_fleet(sqlite_store)
    b = (await run(app, "GET", "/api/v1/energy/fan-peer-comparison")).json()
    assert b["summary"]["ELEVATED"] == 1 and b["energy_units"].startswith("none")
    elevated = [r for r in b["results"] if r["status"] == "ELEVATED"]
    assert elevated[0]["asset_id"] == "A3" and elevated[0]["window"]["end"] == "2026-01-01T13:59:00Z"


@pytest.mark.anyio
async def test_fleet_endpoint_scope(sqlite_store, two_hour_lookback):
    seed_fleet(sqlite_store)
    app = make_api_app(sqlite_store.path, allowed_assets=["A3"])
    b = (await run(app, "GET", "/api/v1/energy/fan-peer-comparison")).json()
    assert [r["asset_id"] for r in b["results"]] == ["A3"]
    assert all(set(r["reference_assets"]) <= {"A3"} for r in b["results"])      # no ids outside the scope
    r = await run(app, "GET", "/api/v1/energy/fan-peer-comparison", params={"asset_id": "A1"})
    assert r.status_code == 403


@pytest.mark.anyio
async def test_copilot_reports_fan_energy_waste(app, sqlite_store):
    seed_fleet(sqlite_store)
    b = (await run(app, "POST", "/api/v1/copilot/chat", json={"message": "Which servers waste fan energy?"})).json()
    assert "A3 fan +19.0% vs comparable peers" in b["answer"] and "cannot be quantified in kW" in b["answer"]
    assert any(e["metric"] == "fan_power.relative_to_peers" and e["kind"] == "calculated" for e in b["evidence"])


@pytest.mark.anyio
async def test_twin_fan_excess_overlay(sqlite_store, tmp_path, two_hour_lookback):
    seed_fleet(sqlite_store)
    from data_engineering.digital_twin.layout import LayoutProvider
    layout = tmp_path / "l.yaml"
    layout.write_text("""
layout: {revision: e, demonstration: true}
objects:
  - {id: dc, type: facility, name: DC, position: [0, 0, 0], dimensions: [9, 3, 9]}
  - {id: room, parent_id: dc, type: room, name: R, position: [0, 0, 0], dimensions: [8, 3, 8]}
  - {id: rack, parent_id: room, type: rack, name: K, position: [0, 1, 0], dimensions: [1, 2, 1]}
  - {id: s1, parent_id: rack, type: server, asset_id: A1, name: S1, position: [0, 0.2, 0], dimensions: [1, 0.1, 1]}
  - {id: s3, parent_id: rack, type: server, asset_id: A3, name: S3, position: [0, 0.6, 0], dimensions: [1, 0.1, 1]}
""", encoding="utf-8")
    t = make_api_app(sqlite_store.path, layout_provider=LayoutProvider(layout))
    b = (await run(t, "GET", "/api/v1/digital-twin/overlays", params={"type": "fan_excess"})).json()
    by = {i["asset_id"]: i for i in b["items"]}
    assert by["A3"]["unit"] == "fraction" and by["A3"]["data_source"] == "energy.fan_peer"
    assert by["A3"]["is_predicted"] is False and "no kW" in b["warnings"][0]
