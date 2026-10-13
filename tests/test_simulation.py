import copy
from collections import Counter

import pytest

from data_engineering.config import load_config, region_filter_name
from data_engineering.db import connect
from data_engineering.paths import RAW_DATASET_DIR, SOURCE_CSV, db_path
from data_engineering.simulation.generate_telemetry import (
    METRICS, READING_COLUMNS, SimulationConfigError, fit_metric_models, generate, load_seed_rows,
    run_simulation)
from data_engineering.validation.validate_dataset import check_simulation, validate

IDX = {c: i for i, c in enumerate(READING_COLUMNS)}


@pytest.fixture
def seed_rows(loaded_conn, sim_config):
    return load_seed_rows(loaded_conn, region_filter_name(sim_config))


def _generate(config, rows):
    return generate(config, rows, SOURCE_CSV, "0" * 64)


def test_each_asset_has_exactly_1440_minute_slots(seed_rows, sim_config):
    result = _generate(sim_config, seed_rows)
    per_asset = Counter(r[IDX["asset_id"]] for r in result.readings)
    assert len(per_asset) == sim_config["simulation"]["asset_count"]
    assert set(per_asset.values()) == {1440}
    for asset_id in per_asset:
        slots = sorted(r[IDX["slot_index"]] for r in result.readings if r[IDX["asset_id"]] == asset_id)
        assert slots == list(range(1440))


def test_time_grid_spans_24_hours_at_one_minute(seed_rows, sim_config):
    result = _generate(sim_config, seed_rows)
    ts = sorted({r[IDX["ts_utc"]] for r in result.readings})
    assert ts[0] == "2026-01-01T00:00:00Z"
    assert ts[-1] == "2026-01-01T23:59:00Z"
    assert len(ts) == 1440


def test_same_seed_is_reproducible_and_different_seed_differs(seed_rows, sim_config):
    a = _generate(sim_config, seed_rows)
    b = _generate(copy.deepcopy(sim_config), seed_rows)
    assert a.content_sha256 == b.content_sha256
    assert a.readings == b.readings and a.labels == b.labels
    other = copy.deepcopy(sim_config)
    other["simulation"]["seed"] = 7
    c = _generate(other, seed_rows)
    assert c.content_sha256 != a.content_sha256
    assert c.run["run_id"] != a.run["run_id"]


def test_missing_readings_are_explicit_and_do_not_shift_the_signal(seed_rows, sim_config):
    with_missing = _generate(sim_config, seed_rows)
    missing = [r for r in with_missing.readings if r[IDX["is_missing"]] == 1]
    assert missing, "missing_rate > 0 should produce missing slots"
    assert all(r[IDX[m]] is None for r in missing for m in METRICS)

    none_missing = copy.deepcopy(sim_config)
    none_missing["simulation"]["missing_rate"] = 0.0
    full = {(r[1], r[3]): r for r in _generate(none_missing, seed_rows).readings}
    assert len(full) == len(with_missing.readings)
    for r in with_missing.readings:
        if r[IDX["is_missing"]] == 0 and r[IDX["is_anomalous"]] == 0:
            assert r[IDX["workload_intensity"]:] == full[(r[1], r[3])][IDX["workload_intensity"]:]


def test_anomalies_are_labeled_with_clean_baseline(seed_rows, sim_config):
    result = _generate(sim_config, seed_rows)
    assert len(result.events) == sum(e["count"] for e in sim_config["anomalies"]["events"])
    readings = {(r[1], r[2]): r for r in result.readings}
    for anomaly_id, _run, asset_id, ts, metric, clean, injected in result.labels:
        row = readings[(asset_id, ts)]
        assert row[IDX["is_anomalous"]] == 1
        assert row[IDX[metric]] == injected
        assert injected > clean
    flagged = sum(r[IDX["is_anomalous"]] for r in result.readings)
    assert flagged == len({(l[2], l[3]) for l in result.labels})


def test_clean_values_stay_within_source_range_per_server_type(seed_rows, sim_config):
    result = _generate(sim_config, seed_rows)
    models = fit_metric_models(seed_rows, sim_config["simulation"]["drivers"])
    server_type = {a["asset_id"]: a["server_type"] for a in result.assets}
    for r in result.readings:
        if r[IDX["is_missing"]] or r[IDX["is_anomalous"]]:
            continue
        for m in METRICS:
            model = models[server_type[r[1]]][m]
            assert model.lo <= r[IDX[m]] <= model.hi, (m, r[IDX[m]])


def test_series_are_smooth_not_independent_noise(seed_rows, sim_config):
    result = _generate(sim_config, seed_rows)
    asset_id = result.assets[0]["asset_id"]
    w = [r[IDX["workload_intensity"]] for r in result.readings
         if r[1] == asset_id and r[IDX["workload_intensity"]] is not None]
    mean = sum(w) / len(w)
    num = sum((a - mean) * (b - mean) for a, b in zip(w, w[1:]))
    den = sum((a - mean) ** 2 for a in w)
    assert num / den > 0.9


def test_assets_are_synthetic_and_seeded_from_region_rows(seed_rows, sim_config):
    result = _generate(sim_config, seed_rows)
    seed_keys = {r["source_row_number"] for r in seed_rows}
    assert all(a["identifier_origin"] == "synthetic" for a in result.assets)
    assert all(a["datacenter_region"] == "NA" for a in result.assets)
    assert all(a["baseline_source_row_number"] in seed_keys for a in result.assets)
    assert len({a["baseline_source_row_number"] for a in result.assets}) == len(result.assets)
    assert all(r[IDX["provenance"]] == "synthetic" for r in result.readings)


def test_invalid_configs_are_rejected(seed_rows, sim_config):
    bad = copy.deepcopy(sim_config)
    bad["anomalies"]["events"][0]["metrics"] = ["fan_failure_flag"]
    with pytest.raises(SimulationConfigError):
        _generate(bad, seed_rows)
    too_many = copy.deepcopy(sim_config)
    too_many["simulation"]["asset_count"] = len(seed_rows) + 1
    with pytest.raises(SimulationConfigError):
        _generate(too_many, seed_rows)
    not_utc = copy.deepcopy(sim_config)
    not_utc["simulation"]["start_ts_utc"] = "2026-01-01T00:00:00"
    with pytest.raises(SimulationConfigError):
        _generate(not_utc, seed_rows)


def test_simulation_checks_pass_on_stored_run(loaded_conn, sim_config, monkeypatch, tmp_path):
    monkeypatch.setattr("data_engineering.simulation.generate_telemetry.SIMULATION_DIR", tmp_path / "sim")
    result = run_simulation(loaded_conn, sim_config)
    checks, facts = check_simulation(loaded_conn, result.run["run_id"])
    failed = [c for c in checks if not c.passed]
    assert not failed, failed
    assert facts["expected_slots_per_asset"] == 1440
    assert facts["missing_readings"] == sum(r[IDX["is_missing"]] for r in result.readings)


@pytest.mark.real_data
@pytest.mark.skipif(not db_path().exists() or not (RAW_DATASET_DIR / SOURCE_CSV).exists(),
                    reason="run `uv run python -m data_engineering.pipeline` first")
def test_real_database_passes_validation():
    conn = connect()
    try:
        report = validate(conn, load_config())
    finally:
        conn.close()
    assert report["passed"], [c for c in report["checks"] if not c["passed"]]
    assert report["coverage"]["has_minute_level_time_series"] is False
