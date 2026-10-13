import copy
import types
from itertools import islice

import pytest

from data_engineering.simulation.generate_telemetry import run_simulation
from data_engineering.streaming.sources import (
    SourceError, build_simulator, simulated_events, stored_run_events)


def _config(sim_config, **sim):
    config = copy.deepcopy(sim_config)
    config["simulation"].update(sim)
    return config


def test_finite_run_yields_assets_times_slots(loaded_conn, sim_config):
    simulator = build_simulator(loaded_conn, _config(sim_config, asset_count=3, duration_minutes=1440))
    stream = simulated_events(simulator, "dc-test")
    assert isinstance(stream, types.GeneratorType)          # incremental, not a list
    events = list(stream)
    assert len(events) == 3 * 1440
    per_asset = {}
    for e in events:
        per_asset.setdefault(e["asset_id"], []).append(e["slot_index"])
    assert all(slots == list(range(1440)) for slots in per_asset.values())
    assert events[-1]["event_timestamp"] == "2026-01-01T23:59:00Z"


def test_interval_seconds_changes_slot_count(loaded_conn, sim_config):
    simulator = build_simulator(loaded_conn, _config(sim_config, asset_count=1, duration_minutes=10,
                                                     frequency_seconds=30))
    events = list(simulated_events(simulator, "dc-test"))
    assert len(events) == 20
    assert events[1]["event_timestamp"] == "2026-01-01T00:00:30Z"


def test_asset_ids_are_stable(loaded_conn, sim_config):
    a = build_simulator(loaded_conn, _config(sim_config, asset_count=4))
    b = build_simulator(loaded_conn, _config(sim_config, asset_count=4))
    assert [x["asset_id"] for x in a.assets] == ["SIM-NA-001", "SIM-NA-002", "SIM-NA-003", "SIM-NA-004"]
    assert a.assets == b.assets
    first_slot = list(islice(simulated_events(a, "dc-test"), 4))
    assert [e["asset_id"] for e in first_slot] == [x["asset_id"] for x in a.assets]


def test_same_seed_reproduces_events_and_other_seed_differs(loaded_conn, sim_config):
    def first(config, n=200):
        return list(islice(simulated_events(build_simulator(loaded_conn, config), "dc-test"), n))
    base = _config(sim_config, asset_count=2, duration_minutes=120)
    assert first(base) == first(copy.deepcopy(base))
    other = first(_config(sim_config, asset_count=2, duration_minutes=120, seed=7))
    assert [e["metrics"] for e in other] != [e["metrics"] for e in first(base)]


def test_continuous_mode_runs_past_the_window(loaded_conn, sim_config):
    simulator = build_simulator(loaded_conn, _config(sim_config, asset_count=1, duration_minutes=60))
    events = list(islice(simulated_events(simulator, "dc-test", continuous=True), 150))
    assert len(events) == 150
    assert events[-1]["slot_index"] == 149
    assert events[-1]["event_timestamp"] == "2026-01-01T02:29:00Z"
    later = {a["anomaly_id"] for e in events[60:] for a in e["labels"]["injected_anomalies"]}
    assert all("-W" in anomaly_id for anomaly_id in later)  # later windows get their own anomalies


def test_stored_run_replays_identically_to_the_simulator(loaded_conn, sim_config, monkeypatch, tmp_path):
    monkeypatch.setattr("data_engineering.simulation.generate_telemetry.SIMULATION_DIR", tmp_path / "sim")
    config = _config(sim_config, asset_count=3, duration_minutes=60)
    result = run_simulation(loaded_conn, config)
    stored = list(stored_run_events(loaded_conn, result.run["run_id"], "dc-test"))
    simulated = list(simulated_events(build_simulator(loaded_conn, config), "dc-test"))
    assert len(stored) == len(simulated) == 3 * 60
    for s, g in zip(stored, simulated):
        assert s["provenance"].pop("published_from") == "stored_run"
        assert g["provenance"].pop("published_from") == "simulator"
        assert s == g


def test_unknown_run_id_is_reported(loaded_conn):
    with pytest.raises(SourceError):
        next(stored_run_events(loaded_conn, "sim-does-not-exist", "dc-test"))


def test_missing_seed_rows_are_reported(conn, sim_config):
    with pytest.raises(SourceError, match="pipeline"):
        build_simulator(conn, sim_config)
