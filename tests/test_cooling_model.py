"""XGBoost cooling response model: features, split, metrics, validity, artifacts, inference."""

import dataclasses
import json
from datetime import timedelta

import numpy as np
import pytest

from cooling_helpers import RUN, T0, event, seed
from cooling_model_helpers import make_series, seed_world, settings
from data_engineering.cooling_model import evaluation, validity
from data_engineering.cooling_model.artifacts import (
    InvalidModelArtifactError,
    ModelNotTrainedError,
    load_artifact,
)
from data_engineering.cooling_model.config import CoolingModelConfigError, FeatureConfig, load_settings
from data_engineering.cooling_model.dataset import select_training_runs
from data_engineering.cooling_model.features import (
    US_PER_MINUTE,
    build_features,
    build_targets,
    feature_names,
    series_from_rows,
)
from data_engineering.cooling_model.predictor import CoolingResponseModel, InsufficientInferenceDataError
from data_engineering.cooling_model.training import InsufficientTrainingDataError, split_masks, train
from data_engineering.storage.sqlite_store import SqliteTelemetryStore

FC = FeatureConfig(current=("inlet_temperature_c", "fan_speed_rpm", "gpu_utilization"),
                   lags_minutes={"inlet_temperature_c": (1, 2)}, rates_minutes={"inlet_temperature_c": (1,)},
                   rolling_minutes={"inlet_temperature_c": (3,)}, time_of_day=False, categorical=("asset_type",))


def col(fc, name, categories=None):
    return feature_names(fc, categories or {}).index(name)


# ------------------------------------------------------------------ features and targets

def test_lags_rates_and_rolling_use_only_the_past():
    s = make_series([20.0, 21.0, 23.0, 26.0, 30.0])
    X = build_features(s, FC, {})
    assert np.isnan(X[0, col(FC, "inlet_temperature_c_lag_1m")])
    assert X[3, col(FC, "inlet_temperature_c_lag_1m")] == 23.0
    assert X[3, col(FC, "inlet_temperature_c_lag_2m")] == 21.0
    assert X[3, col(FC, "inlet_temperature_c_rate_1m")] == 3.0
    # rolling window (t-3, t] = rows 1..3; never includes the future row 4
    assert X[3, col(FC, "inlet_temperature_c_mean_3m")] == pytest.approx((21 + 23 + 26) / 3)
    assert X[3, col(FC, "inlet_temperature_c_max_3m")] == 26.0


def test_future_values_do_not_change_past_features():
    a = build_features(make_series([20, 21, 22, 23, 24.0]), FC, {})
    b = build_features(make_series([20, 21, 22, 23, 99.0]), FC, {})
    np.testing.assert_array_equal(a[:4], b[:4])


def test_irregular_timestamps_give_missing_lags_not_shifted_values():
    s = make_series([20.0, 21.0, 25.0], t_minutes=[0, 1, 4])      # gap: minutes 2 and 3 absent
    X = build_features(s, FC, {})
    assert np.isnan(X[2, col(FC, "inlet_temperature_c_lag_1m")])  # not 21.0 (which is 3 minutes back)
    assert np.isnan(X[2, col(FC, "inlet_temperature_c_lag_2m")])
    t = build_targets(s, [1, 3])
    assert t[1][0] == 1.0 and np.isnan(t[1][1]) and t[3][1] == 4.0


def test_targets_are_future_change_and_nan_at_the_end():
    t = build_targets(make_series([20.0, 20.5, 21.5]), [1, 2])
    np.testing.assert_allclose(t[1][:2], [0.5, 1.0])
    assert t[2][0] == 1.5 and np.isnan(t[2][1]) and np.isnan(t[1][2])


def test_missing_and_duplicate_readings():
    rows = [{"event_timestamp": T0, "ingested_at": T0, "reading_status": "observed", "inlet_temperature_c": 20.0,
             "exhaust_temperature_c": 30.0},
            {"event_timestamp": T0, "ingested_at": T0 + timedelta(seconds=5), "reading_status": "observed",
             "inlet_temperature_c": 22.0, "exhaust_temperature_c": 30.0},
            {"event_timestamp": T0 + timedelta(minutes=1), "ingested_at": T0, "reading_status": "missing",
             "inlet_temperature_c": None}]
    s = series_from_rows(rows, {"asset_type": "Compute"})
    assert len(s) == 2 and s.duplicates_dropped == 1
    assert s.values["inlet_temperature_c"][0] == 22.0        # latest ingested wins
    assert np.isnan(s.values["inlet_temperature_c"][1])        # missing stays missing, never 0
    assert np.isnan(s.values["gpu_utilization"]).all()         # structural 0 of non-GPU assets


def test_one_hot_uses_training_categories_only():
    s = make_series([20.0, 21.0], asset_type="Storage")
    X = build_features(s, FC, {"asset_type": ["Compute", "GPU"]})
    names = feature_names(FC, {"asset_type": ["Compute", "GPU"]})
    assert names[-2:] == ["asset_type=Compute", "asset_type=GPU"]
    assert (X[:, -2:] == 0).all()                             # unseen category -> all zeros


def test_config_rejects_unknown_fields_and_disabled_recommendation_only(tmp_path):
    good = (load_settings().source).read_text(encoding="utf-8")
    bad = tmp_path / "c.yaml"
    bad.write_text(good.replace("recommendation_only: true", "recommendation_only: false"), encoding="utf-8")
    with pytest.raises(CoolingModelConfigError, match="recommendation_only"):
        load_settings(bad)
    bad.write_text(good.replace("current: [inlet_temperature_c,", "current: [inlet_temperature_c, cooling_power_kw,"),
                   encoding="utf-8")
    with pytest.raises(CoolingModelConfigError, match="cooling_power_kw"):
        load_settings(bad)


def test_shipped_config_invents_no_limits():
    o = load_settings().optimizer
    assert o.t_safe_c is None and o.setting_min is None and o.setting_max is None
    assert o.measured_cooling_power_column is None and o.recommendation_only is True


# ------------------------------------------------------------------ split, metrics, baseline

def test_chronological_split_with_purge():
    t = np.arange(100, dtype=np.int64) * US_PER_MINUTE
    train_m, val_m, test_m, cut = split_masks(t, 0.6, 0.2, max_horizon=5)
    assert t[train_m].max() < t[val_m].min() and t[val_m].max() < t[test_m].min()
    # no training target (t + 5 min) reaches the validation period
    assert t[train_m].max() + 5 * US_PER_MINUTE < t[val_m].min() + 5 * US_PER_MINUTE
    assert (t[train_m] + 5 * US_PER_MINUTE < 59.4 * US_PER_MINUTE).all()
    assert cut["purged_rows"] == 10 and not (train_m & test_m).any()


def test_metrics_and_persistence_comparison():
    y = np.array([20.0, 21.0, 22.0, 23.0])
    m = evaluation.metrics(y, y + 0.5)
    assert m["mae"] == 0.5 and m["rmse"] == 0.5 and m["n"] == 4
    c = evaluation.compare(y, y, y - 1.0)                     # perfect model vs persistence
    assert c["rmse_skill_vs_persistence"] == 1.0
    assert c["model"]["r2_of_change"] is None                 # constant true change -> undefined, not invented
    c2 = evaluation.compare(y, np.array([20.1, 21.2, 21.9, 23.3]), np.array([19.0, 21.5, 21.0, 23.5]))
    assert c2["persistence"]["r2_of_change"] == pytest.approx(
        1 - (np.array([1, -0.5, 1, -0.5]) ** 2).sum() / ((np.array([1, -0.5, 1, -0.5]) - 0.25) ** 2).sum())
    assert evaluation.metrics([], [])["rmse"] is None


def test_segments_too_small_are_not_evaluated():
    y = np.arange(40, dtype=float)
    labels = np.array(["a"] * 35 + ["b"] * 5)
    report = evaluation.segment_report(y, y, y, labels, min_rows=30)
    assert "model" in report["a"] and report["b"]["note"].startswith("fewer than 30")


# ------------------------------------------------------------------ validity

def _series_list(kind, assets=3, minutes=600):
    from cooling_model_helpers import world
    rng = np.random.default_rng(7)
    out = []
    for a in range(assets):
        rows = world(kind, minutes, a, rng)
        fields = {k: [r[k] for r in rows] for k in rows[0]}
        out.append(make_series(**{"inlet": fields.pop("inlet_temperature_c")}, **fields))
    return out


def test_validity_rejects_fan_without_effect():
    r = validity.setting_response(_series_list("observational"), "fan_speed_rpm", [5, 10], -1, 0.2, 3.0, 0.8)
    assert r["validated"] is False and any("0 of 3 assets" in x for x in r["reasons"])


def test_validity_accepts_fan_with_real_effect():
    r = validity.setting_response(_series_list("causal"), "fan_speed_rpm", [5, 10], -1, 0.2, 3.0, 0.8)
    assert r["validated"] is True, r["reasons"]
    assert r["by_horizon"]["5"]["median_coef_per_unit_step"] < 0


def test_validity_rejects_setting_that_only_follows_workload():
    s = _series_list("observational")
    for x in s:                        # fan steps exactly with workload: no independent variation
        x.values["fan_speed_rpm"] = 2000 + 2000 * x.values["workload_intensity"]
    r = validity.setting_response(s, "fan_speed_rpm", [5], -1, 0.2, 3.0, 0.8)
    assert r["validated"] is False and any("not explained by workload" in x for x in r["reasons"])


# ------------------------------------------------------------------ training end to end

@pytest.fixture(scope="module")
def trained(tmp_path_factory):
    """Both synthetic worlds trained once per module (each ~1 s)."""
    out = {}
    for kind in ("observational", "causal"):
        d = tmp_path_factory.mktemp(kind)
        store = SqliteTelemetryStore(d / "t.db")
        store.initialize()
        seed_world(store, kind)
        meta = train(store, settings(d / "models"))
        out[kind] = {"store": store, "meta": meta, "dir": d / "models"}
    yield out
    for v in out.values():
        v["store"].close()


def test_training_labels_and_metadata(trained):
    obs, causal = trained["observational"]["meta"], trained["causal"]["meta"]
    assert obs["model_label"] == "forecasting_baseline"
    assert causal["model_label"] == "intervention_validated"
    for h in ("5", "10"):                                     # each horizon trained and evaluated
        e = obs["evaluation"][h]
        assert e["test"]["model"]["n"] > 0 and e["test"]["persistence"]["rmse"] > 0
        assert set(e["test_segments"]) == {"asset_type", "workload_level", "inlet_range", "injected_anomaly"}
        assert e["test"]["rmse_skill_vs_persistence"] > 0      # synthetic world is forecastable
    split = obs["training_data"]["split"]
    assert split["train_end"] < split["validation_end"] and split["purged_rows"] > 0
    assert "cooling_power" in obs["validity"]["unavailable_inputs"]


def test_saved_model_loads_without_retraining_and_matches(trained):
    t = trained["observational"]
    model = CoolingResponseModel.load(t["dir"])
    assert model.version == t["meta"]["version"]
    p1 = model.predict_for_asset(t["store"], RUN, "T-00", T0 + timedelta(minutes=500))
    p2 = CoolingResponseModel.load(t["dir"], t["meta"]["version"]).predict_for_asset(
        t["store"], RUN, "T-00", T0 + timedelta(minutes=500))
    assert [f["predicted_inlet_temperature_c"] for f in p1["forecasts"]] == \
           [f["predicted_inlet_temperature_c"] for f in p2["forecasts"]]
    assert [f["horizon_minutes"] for f in p1["forecasts"]] == [5, 10]


def test_training_is_reproducible_with_fixed_seed(trained, tmp_path):
    t = trained["observational"]
    again = train(t["store"], settings(tmp_path / "m2"))
    for h in ("5", "10"):
        assert again["evaluation"][h]["test"]["model"]["rmse"] == t["meta"]["evaluation"][h]["test"]["model"]["rmse"]
    assert again["training_data"]["fingerprint"] == t["meta"]["training_data"]["fingerprint"]


def test_insufficient_training_data(tmp_path):
    store = SqliteTelemetryStore(tmp_path / "t.db")
    store.initialize()
    try:
        with pytest.raises(InsufficientTrainingDataError, match="at least 60 minutes"):
            train(store, settings(tmp_path / "m"))           # empty store
        seed(store, [event("A", m) for m in range(120)])      # long enough run, too few rows
        with pytest.raises(InsufficientTrainingDataError, match="train rows"):
            train(store, settings(tmp_path / "m"))
        assert not (tmp_path / "m" / "ACTIVE").exists()       # a failed run never becomes active
    finally:
        store.close()


def test_duplicate_seed_runs_are_used_once(tmp_path):
    store = SqliteTelemetryStore(tmp_path / "t.db")
    store.initialize()
    try:
        seed(store, [event("A", m, run="sim-a") for m in range(90)])
        seed(store, [event("A", m, run="sim-b") for m in range(70)])     # same seed (helper uses seed 1)
        sel = select_training_runs(store, 60)
        assert [r["run_id"] for r in sel.used] == ["sim-a"]
        assert sel.skipped[0]["reason"].startswith("same seed")
        with pytest.raises(LookupError):
            select_training_runs(store, 60, ["sim-zzz"])
    finally:
        store.close()


# ------------------------------------------------------------------ artifacts

def _copy_artifact(trained, tmp_path):
    import shutil
    src = trained["observational"]["dir"]
    dst = tmp_path / "models"
    shutil.copytree(src, dst)
    version = (dst / "ACTIVE").read_text().strip()
    return dst, dst / version


def test_no_model_trained(tmp_path):
    with pytest.raises(ModelNotTrainedError):
        load_artifact(tmp_path / "nothing")


@pytest.mark.parametrize("damage, match", [
    (lambda d: (d / "metadata.json").write_text("{not json"), "not valid JSON"),
    (lambda d: (d / "metadata.json").unlink(), "metadata.json missing"),
    (lambda d: (d / "model_h5.ubj").unlink(), "model_h5.ubj missing"),
    (lambda d: (d / "model_h10.ubj").write_bytes(b"garbage"), "not a valid XGBoost model"),
    (lambda d: _edit_meta(d, artifact_schema_version=99), "schema version 99"),
    (lambda d: _edit_meta(d, xgboost_version="1.0.0"), "major version differs"),
    (lambda d: _edit_meta(d, feature_names=["a", "b"]), "expects"),
    (lambda d: _edit_meta(d, drop="validity"), "lacks"),
])
def test_invalid_artifacts_are_rejected(trained, tmp_path, damage, match):
    root, version_dir = _copy_artifact(trained, tmp_path)
    damage(version_dir)
    with pytest.raises(InvalidModelArtifactError, match=match):
        load_artifact(root)


def _edit_meta(d, drop=None, **changes):
    meta = json.loads((d / "metadata.json").read_text())
    meta.update(changes)
    if drop:
        meta.pop(drop)
    (d / "metadata.json").write_text(json.dumps(meta))


# ------------------------------------------------------------------ inference

def test_prediction_reports_quality_and_label(trained):
    t = trained["observational"]
    model = CoolingResponseModel.load(t["dir"])
    p = model.predict_for_asset(t["store"], RUN, "T-01", T0 + timedelta(minutes=400))
    assert p["model_label"] == "forecasting_baseline"
    assert any("not validated" in w for w in p["model_validity_warnings"])
    assert p["forecasts"][0]["forecast_timestamp"] == (T0 + timedelta(minutes=405)).isoformat()
    assert p["data_quality"]["status"] == "OK"
    with pytest.raises(LookupError):
        model.predict_for_asset(t["store"], RUN, "NOPE")


def test_prediction_with_stale_or_missing_data(trained, tmp_path):
    model = CoolingResponseModel.load(trained["observational"]["dir"])
    store = SqliteTelemetryStore(tmp_path / "t.db")
    store.initialize()
    try:
        seed(store, [event("A", m) for m in range(5)] + [event("A", 5, status="missing")])
        with pytest.raises(InsufficientInferenceDataError, match="no inlet"):
            model.predict_for_asset(store, RUN, "A")
        p = model.predict_for_asset(store, RUN, "A", T0 + timedelta(minutes=4))
        assert p["data_quality"]["status"] == "DEGRADED"                 # 5 of 16 lookback readings
        stale = model.predict_for_asset(store, RUN, "A", T0 + timedelta(minutes=4),
                                        clock=lambda: T0 + timedelta(days=400))
        assert any("stream may have stopped" in n for n in stale["data_quality"]["notes"])
        with pytest.raises(InsufficientInferenceDataError, match="no readings"):
            model.predict_for_asset(store, RUN, "A", T0 - timedelta(hours=1))
    finally:
        store.close()


def test_extrapolated_inputs_are_flagged(trained, tmp_path):
    model = CoolingResponseModel.load(trained["observational"]["dir"])
    store = SqliteTelemetryStore(tmp_path / "t.db")
    store.initialize()
    try:
        seed(store, [event("A", m, inlet_temperature_c=45.0) for m in range(20)])
        p = model.predict_for_asset(store, RUN, "A")
        assert any(x.startswith("inlet_temperature_c=45") for x in p["extrapolated_features"])
        assert any("extrapolated" in w for w in p["model_validity_warnings"])
    finally:
        store.close()


def test_model_settings_replace_keeps_dataclass_frozen():
    s = settings(load_settings().model.artifact_dir)
    with pytest.raises(dataclasses.FrozenInstanceError):
        s.optimizer.t_safe_c = 30.0
