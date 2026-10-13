"""Online inference: load the active artifact once, forecast inlet temperature for one asset.

The forecast is made under the OBSERVED conditions (no hypothetical settings). Features are built
from the latest readings with the same code as training (features.build_features).
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import xgboost as xgb

from data_engineering.cooling_model.artifacts import load_artifact
from data_engineering.cooling_model.config import FeatureConfig
from data_engineering.cooling_model.features import build_features, series_from_rows

LIMITATIONS = [
    "Trained and evaluated on SIMULATED telemetry only; offline metrics are not evidence of physical validity.",
    "Forecast under observed conditions only; it does not predict the effect of changing any cooling setting.",
]


class InsufficientInferenceDataError(ValueError):
    pass


@dataclass
class CoolingResponseModel:
    boosters: dict[int, xgb.Booster]
    metadata: dict

    @classmethod
    def load(cls, artifact_dir: Path, version: str | None = None) -> "CoolingResponseModel":
        boosters, metadata = load_artifact(artifact_dir, version)
        return cls(boosters, metadata)

    @property
    def version(self) -> str:
        return self.metadata["version"]

    @property
    def label(self) -> str:
        return self.metadata["model_label"]

    @property
    def feature_config(self) -> FeatureConfig:
        return FeatureConfig.from_dict(self.metadata["feature_config"])

    def predict_delta(self, X: np.ndarray) -> dict[int, np.ndarray]:
        d = xgb.DMatrix(X, missing=np.nan, feature_names=self.metadata["feature_names"])
        out = {}
        for h, booster in self.boosters.items():
            best = self.metadata["evaluation"][str(h)].get("best_iteration")
            out[h] = booster.predict(d, iteration_range=(0, best) if best else (0, 0))
        return out

    def out_of_range(self, x: np.ndarray) -> list[str]:
        """Features outside the training split's [min, max] (extrapolation)."""
        flagged = []
        for name, value in zip(self.metadata["feature_names"], x):
            r = self.metadata["feature_ranges"].get(name)
            if r and np.isfinite(value) and r["min"] is not None and not (r["min"] <= value <= r["max"]):
                flagged.append(f"{name}={value:.4g} outside training range [{r['min']:.4g}, {r['max']:.4g}]")
        return flagged

    def predict_for_asset(self, store, run_id: str, asset_id: str, as_of: datetime | None = None,
                          max_ingest_age_s: float = 300.0, clock=lambda: datetime.now(timezone.utc)) -> dict:
        assets = {a["asset_id"]: a for a in store.run_assets(run_id)}
        if asset_id not in assets:
            raise LookupError(f"asset {asset_id!r} not found in simulation run {run_id!r}")
        asset = assets[asset_id]
        fc = self.feature_config
        as_of = as_of or asset["last_event_timestamp"]
        start = as_of - timedelta(minutes=fc.lookback_minutes + 1)
        series = series_from_rows(store.readings_window(run_id, asset_id, start, as_of), asset)
        if not len(series):
            raise InsufficientInferenceDataError(f"no readings for {asset_id} in ({start.isoformat()}, {as_of.isoformat()}]")
        X = build_features(series, fc, self.metadata["categories"])
        x = X[-1]
        latest_ts = datetime.fromtimestamp(series.t_us[-1] / 1e6, timezone.utc)
        inlet = series.values["inlet_temperature_c"][-1]
        warnings = list(LIMITATIONS)
        if self.label != "intervention_validated":
            warnings.append("Model label 'forecasting_baseline': the response to cooling settings is not validated.")
        quality, dq_notes = "OK", []
        if not np.isfinite(inlet):
            raise InsufficientInferenceDataError(f"latest reading of {asset_id} at {latest_ts.isoformat()} has no inlet "
                                                 "temperature (missing reading); no forecast")
        n_missing = int((~np.isfinite(x[:len(fc.current)])).sum())
        lag_missing = int((~np.isfinite(x)).sum()) - n_missing
        expected_rows = fc.lookback_minutes + 1
        if len(series) < expected_rows or lag_missing:
            quality = "DEGRADED"
            dq_notes.append(f"{len(series)} of {expected_rows} readings in the lookback window; "
                            f"{lag_missing} lag/rolling features missing")
        if (as_of - latest_ts).total_seconds() > 120:
            quality = "DEGRADED"
            dq_notes.append(f"latest reading is {(as_of - latest_ts).total_seconds():.0f} s older than as_of")
        ingest_age = (clock() - asset["updated_at"]).total_seconds()
        if ingest_age > max_ingest_age_s:
            quality = "DEGRADED"
            dq_notes.append(f"nothing stored for {ingest_age:.0f} s (wall clock): the stream may have stopped")
        extrapolation = self.out_of_range(x)
        if extrapolation:
            warnings.append(f"{len(extrapolation)} feature(s) outside the training range; forecast is extrapolated")
        deltas = self.predict_delta(x[None, :])
        forecasts = []
        for h in sorted(deltas):
            ev = self.metadata["evaluation"][str(h)]["test"]
            forecasts.append({
                "horizon_minutes": h,
                "forecast_timestamp": (latest_ts + timedelta(minutes=h)).isoformat(),
                "predicted_inlet_temperature_c": round(float(inlet + deltas[h][0]), 4),
                "persistence_inlet_temperature_c": round(float(inlet), 4),
                "offline_test_rmse_c": ev["model"]["rmse"], "offline_persistence_rmse_c": ev["persistence"]["rmse"]})
        return {"asset_id": asset_id, "simulation_run_id": run_id, "asset_type": asset.get("asset_type"),
                "observed_at": latest_ts.isoformat(), "latest_inlet_temperature_c": round(float(inlet), 4),
                "forecasts": forecasts, "model_version": self.version, "model_label": self.label,
                "data_quality": {"status": quality, "notes": dq_notes, "ingest_age_seconds": round(ingest_age, 1),
                                 "readings_in_lookback": int(len(series)),
                                 "duplicates_dropped": series.duplicates_dropped},
                "model_validity_warnings": warnings, "extrapolated_features": extrapolation,
                "_features": x, "_series": series}
