"""Reproducible training of the XGBoost inlet-temperature forecaster.

    uv run python -m data_engineering.cooling_model.training             # all eligible runs
    uv run python -m data_engineering.cooling_model.training --runs sim-c9944cc35156 sim-b85066662da9

Pipeline: select runs (one per seed) -> per-asset features/targets -> chronological split on event
time with purging -> one XGBoost model per horizon (target: inlet change over h minutes; early
stopping on validation) -> test evaluation vs persistence -> validity checks -> versioned artifact.
"""

import argparse
import hashlib
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
import xgboost as xgb

from data_engineering.cooling_model import evaluation, validity
from data_engineering.cooling_model.artifacts import ARTIFACT_SCHEMA_VERSION, save_artifact
from data_engineering.cooling_model.config import SERIES_FIELDS, CoolingModelSettings, load_settings
from data_engineering.cooling_model.dataset import load_run_series, select_training_runs
from data_engineering.cooling_model.features import (
    US_PER_MINUTE, Series, build_features, build_targets, feature_names)
from data_engineering.logging_utils import get_logger

log = get_logger("cooling_model")
MODEL_LABELS = ("forecasting_baseline", "intervention_validated")


class InsufficientTrainingDataError(ValueError):
    pass


@dataclass
class Dataset:
    X_numeric: np.ndarray        # features without one-hot columns
    categorical: np.ndarray      # (n, len(fc.categorical)) object
    t_us: np.ndarray
    run_id: np.ndarray
    asset_id: np.ndarray
    asset_type: np.ndarray
    inlet_now: np.ndarray
    workload_now: np.ndarray
    anomaly: np.ndarray
    targets: dict[int, np.ndarray]
    numeric_names: list[str]


def build_dataset(training_series, fc, horizons) -> Dataset:
    parts = []
    for ts in training_series:
        s = ts.series
        if not len(s):
            continue
        parts.append(dict(
            X=build_features(s, fc, {}), cat=np.array([[s.asset.get(c) for c in fc.categorical]] * len(s), dtype=object)
            .reshape(len(s), len(fc.categorical)), t=s.t_us, run=np.full(len(s), ts.run_id, dtype=object),
            asset=np.full(len(s), s.asset.get("asset_id"), dtype=object),
            atype=np.full(len(s), s.asset.get("asset_type"), dtype=object),
            inlet=s.values["inlet_temperature_c"], work=s.values["workload_intensity"], anom=ts.injected_anomaly,
            targets=build_targets(s, horizons)))
    if not parts:
        raise InsufficientTrainingDataError("no telemetry series available for training")
    cat = lambda k: np.concatenate([p[k] for p in parts])  # noqa: E731
    return Dataset(X_numeric=np.vstack([p["X"] for p in parts]), categorical=np.vstack([p["cat"] for p in parts]),
                   t_us=cat("t"), run_id=cat("run"), asset_id=cat("asset"), asset_type=cat("atype"),
                   inlet_now=cat("inlet"), workload_now=cat("work"), anomaly=cat("anom"),
                   targets={h: np.concatenate([p["targets"][h] for p in parts]) for h in horizons},
                   numeric_names=feature_names(fc, {}))


def encode(ds: Dataset, fc, categories: dict[str, list[str]]) -> np.ndarray:
    onehot = [(ds.categorical[:, i] == v).astype(float) for i, c in enumerate(fc.categorical) for v in categories[c]]
    return np.column_stack([ds.X_numeric, *onehot]) if onehot else ds.X_numeric


def split_masks(t_us: np.ndarray, train_fraction: float, validation_fraction: float, max_horizon: int):
    """Chronological split on event time; rows whose longest-horizon target crosses a boundary are purged."""
    t0, t1 = int(t_us.min()), int(t_us.max())
    train_end = t0 + (t1 - t0) * train_fraction
    val_end = t0 + (t1 - t0) * (train_fraction + validation_fraction)
    reach = t_us + max_horizon * US_PER_MINUTE
    train = (t_us < train_end) & (reach < train_end)
    val = (t_us >= train_end) & (t_us < val_end) & (reach < val_end)
    test = t_us >= val_end
    cut = {"train_end": datetime.fromtimestamp(train_end / 1e6, timezone.utc).isoformat(),
           "validation_end": datetime.fromtimestamp(val_end / 1e6, timezone.utc).isoformat(),
           "purged_rows": int((~(train | val | test)).sum())}
    return train, val, test, cut


def _truncate(series: Series, end_us: float) -> Series:
    keep = series.t_us < end_us
    return Series(series.t_us[keep], {k: v[keep] for k, v in series.values.items()}, series.asset)


def _fingerprint(training_series) -> str:
    h = hashlib.sha256()
    for ts in sorted(training_series, key=lambda x: (x.run_id, x.series.asset.get("asset_id"))):
        s = ts.series
        h.update(f"{ts.run_id}|{s.asset.get('asset_id')}|{len(s)}|{s.t_us[:1]}|{s.t_us[-1:]}|"
                 f"{np.nansum(s.values['inlet_temperature_c']):.6f}".encode())
    return h.hexdigest()[:16]


def train(store, settings: CoolingModelSettings | None = None, run_ids: list[str] | None = None) -> dict:
    """Train, evaluate, validate and save a new active artifact. Returns its metadata."""
    started = time.monotonic()
    settings = settings or load_settings()
    mc, fc = settings.model, settings.model.features
    horizons = list(mc.horizons_minutes)
    selection = select_training_runs(store, mc.min_series_minutes, run_ids)
    if not selection.used:
        raise InsufficientTrainingDataError(f"no run has at least {mc.min_series_minutes} minutes of telemetry")
    training_series = [ts for run in selection.used for ts in load_run_series(store, run["run_id"])]
    ds = build_dataset(training_series, fc, horizons)
    train_m, val_m, test_m, cutoffs = split_masks(ds.t_us, mc.train_fraction, mc.validation_fraction, max(horizons))
    categories = {c: sorted({str(v) for v in ds.categorical[train_m, i] if v is not None})
                  for i, c in enumerate(fc.categorical)}
    X = encode(ds, fc, categories)
    names = feature_names(fc, categories)
    params = {k: v for k, v in mc.xgboost.items() if k not in ("num_boost_round", "early_stopping_rounds")}

    q_low, q_high = settings.optimizer.supported_range_quantiles
    with np.errstate(all="ignore"):
        Xtr = X[train_m]
        ranges = {n: {"min": _f(np.nanmin(Xtr[:, j])), "max": _f(np.nanmax(Xtr[:, j])),
                      "q_low": _f(np.nanquantile(Xtr[:, j], q_low)), "q_high": _f(np.nanquantile(Xtr[:, j], q_high))}
                  if np.isfinite(Xtr[:, j]).any() else None for j, n in enumerate(names)}

    workload_edges = [float(x) for x in np.nanquantile(ds.workload_now[train_m], [1 / 3, 2 / 3])]
    inlet_edges = [float(x) for x in np.nanquantile(ds.inlet_now[train_m], [1 / 3, 2 / 3])]
    boosters, results, row_counts = {}, {}, {}
    for h in horizons:
        y = ds.targets[h]
        usable = np.isfinite(y) & np.isfinite(ds.inlet_now)
        tr, va, te = train_m & usable, val_m & usable, test_m & usable
        row_counts[str(h)] = {"train": int(tr.sum()), "validation": int(va.sum()), "test": int(te.sum())}
        if tr.sum() < mc.min_training_rows or va.sum() == 0 or te.sum() == 0:
            raise InsufficientTrainingDataError(
                f"horizon {h} min: {int(tr.sum())} train / {int(va.sum())} validation / {int(te.sum())} test rows "
                f"(needs >= {mc.min_training_rows} train rows and non-empty validation and test splits)")
        dtrain = xgb.DMatrix(X[tr], label=y[tr], missing=np.nan, feature_names=names)
        dval = xgb.DMatrix(X[va], label=y[va], missing=np.nan, feature_names=names)
        booster = xgb.train(params, dtrain, num_boost_round=mc.xgboost["num_boost_round"],
                            evals=[(dval, "validation")], early_stopping_rounds=mc.xgboost.get("early_stopping_rounds"),
                            verbose_eval=False)
        boosters[h] = booster
        best = booster.best_iteration + 1 if hasattr(booster, "best_iteration") else None

        def predict(mask, bst=booster, b=best):
            d = xgb.DMatrix(X[mask], missing=np.nan, feature_names=names)
            return ds.inlet_now[mask] + bst.predict(d, iteration_range=(0, b) if b else (0, 0))

        y_te, p_te, base_te = ds.inlet_now[te] + y[te], predict(te), ds.inlet_now[te]
        y_va, p_va, base_va = ds.inlet_now[va] + y[va], predict(va), ds.inlet_now[va]
        per_asset = evaluation.segment_report(y_te, p_te, base_te, ds.asset_id[te].astype(str))
        maes = [(v["model"]["mae"], v["persistence"]["mae"]) for v in per_asset.values() if "model" in v]
        results[str(h)] = {
            "best_iteration": best,
            "validation": evaluation.compare(y_va, p_va, base_va),
            "test": evaluation.compare(y_te, p_te, base_te),
            "test_segments": {
                "asset_type": evaluation.segment_report(y_te, p_te, base_te, ds.asset_type[te].astype(str)),
                "workload_level": evaluation.segment_report(
                    y_te, p_te, base_te, evaluation.bin_labels(ds.workload_now[te], workload_edges, ["low", "medium", "high"])),
                "inlet_range": evaluation.segment_report(
                    y_te, p_te, base_te, evaluation.bin_labels(ds.inlet_now[te], inlet_edges, ["cool", "mid", "warm"])),
                "injected_anomaly": evaluation.segment_report(
                    y_te, p_te, base_te, np.where(ds.anomaly[te], "during_injected_anomaly", "no_anomaly")),
            },
            "per_asset_mae": {"assets": len(maes),
                              "model_better_than_persistence": sum(1 for m, b in maes if m < b),
                              "model_median": float(np.median([m for m, _ in maes])) if maes else None,
                              "persistence_median": float(np.median([b for _, b in maes])) if maes else None},
            "top_features_by_gain": dict(sorted(booster.get_score(importance_type="gain").items(),
                                                key=lambda kv: -kv[1])[:12]),
        }

    # Validity: training split only.
    train_end_us = ds.t_us[train_m].max() + 1
    train_series = [_truncate(ts.series, train_end_us) for ts in training_series]
    vconf = mc.validity
    settings_checks = {name: validity.setting_response(
        train_series, name, horizons, spec["expected_effect_sign"], vconf.get("min_independent_variation_share", 0.2),
        vconf.get("min_abs_t", 3.0), vconf.get("min_asset_share", 0.8))
        for name, spec in (vconf.get("controllable_settings") or {}).items()}
    for name, check in settings_checks.items():
        check["is_model_feature"] = name in fc.current
        if not check["is_model_feature"]:
            check["validated"] = False
            check["reasons"].insert(0, f"{name} is not a model feature")
    # Associational sensitivity of the trained model (NOT a causal estimate).
    sensitivity = {}
    for name in settings_checks:
        if name in names:
            j = names.index(name)
            for h in horizons:
                base = X[test_m].copy()
                up, down = base.copy(), base.copy()
                up[:, j] += 100.0
                down[:, j] -= 100.0
                pu = boosters[h].predict(xgb.DMatrix(up, missing=np.nan, feature_names=names))
                pd_ = boosters[h].predict(xgb.DMatrix(down, missing=np.nan, feature_names=names))
                sensitivity.setdefault(name, {})[str(h)] = {
                    "mean_prediction_change_per_100_units": float(np.nanmean(pu - pd_) / 2),
                    "note": "model output change when only this input is shifted; reflects learned associations, "
                            "not a validated physical response"}
    validated_all = bool(settings_checks) and all(c["validated"] for c in settings_checks.values())
    label = MODEL_LABELS[1] if validated_all else MODEL_LABELS[0]

    created = datetime.now(timezone.utc)
    fingerprint = _fingerprint(training_series)
    config_hash = hashlib.sha256(json.dumps({"features": fc.to_dict(), "xgb": mc.xgboost, "h": horizons,
                                             "split": [mc.train_fraction, mc.validation_fraction]},
                                            sort_keys=True).encode()).hexdigest()[:8]
    version = f"crm-{created:%Y%m%dT%H%M%SZ}-{fingerprint[:8]}{config_hash}"
    metadata = {
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION, "version": version, "created_at": created.isoformat(),
        "model_label": label, "target": "inlet_temperature_c(t+h) - inlet_temperature_c(t), deg C",
        "horizons_minutes": horizons, "feature_names": names, "feature_config": fc.to_dict(),
        "categories": categories, "feature_ranges": ranges, "xgboost_params": mc.xgboost,
        "xgboost_version": xgb.__version__,
        "training_data": {"runs_used": selection.used, "runs_skipped": selection.skipped,
                          "series": len(training_series), "rows": int(len(ds.t_us)), "fingerprint": fingerprint,
                          "split": cutoffs, "rows_per_horizon": row_counts,
                          "source_type": "simulated"},
        "evaluation": results,
        "validity": {"model_label": label, "controllable_settings": settings_checks,
                     "model_sensitivity": sensitivity,
                     "variable_profile": validity.variable_profile(train_series, [f for f in SERIES_FIELDS]),
                     "unavailable_inputs": {"cooling_efficiency": "not in the telemetry schema",
                                            "cooling_power": "no cooling-system power is measured; power_consumption_kw is per-server IT power",
                                            "cooling_setpoints": "no setpoint or actuator-command field exists"}},
        "duration_seconds": round(time.monotonic() - started, 2),
    }
    path = save_artifact(mc.artifact_dir, version, boosters, metadata)
    log.info("cooling response model trained", extra={"context": {
        "version": version, "label": label, "artifact": str(path), "rows": int(len(ds.t_us)),
        **{f"test_rmse_h{h}": results[str(h)]["test"]["model"]["rmse"] for h in horizons},
        **{f"persistence_rmse_h{h}": results[str(h)]["test"]["persistence"]["rmse"] for h in horizons}}})
    return metadata


def _f(x) -> float | None:
    x = float(x)
    return x if np.isfinite(x) else None


def main(argv: list[str] | None = None) -> int:
    from data_engineering.storage.base import open_store
    from data_engineering.streaming.settings import load_consumer_settings
    ap = argparse.ArgumentParser(description="Train the XGBoost cooling response (inlet forecast) model.")
    ap.add_argument("--runs", nargs="+", help="simulation_run_ids to train on (default: all eligible, one per seed)")
    ap.add_argument("--config", help="cooling model config (default config/cooling_model.yaml)")
    args = ap.parse_args(argv)
    store = open_store(load_consumer_settings())
    try:
        store.check_initialized()
        meta = train(store, load_settings(args.config), args.runs)
    except (InsufficientTrainingDataError, LookupError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        store.close()
    summary = {"version": meta["version"], "model_label": meta["model_label"],
               "runs_used": [r["run_id"] for r in meta["training_data"]["runs_used"]],
               "split": meta["training_data"]["split"], "rows_per_horizon": meta["training_data"]["rows_per_horizon"],
               "test": {h: {k: v["test"][k] for k in ("model", "persistence", "rmse_skill_vs_persistence")}
                        for h, v in meta["evaluation"].items()},
               "validity": {k: {"validated": v["validated"], "reasons": v["reasons"]}
                            for k, v in meta["validity"]["controllable_settings"].items()}}
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
