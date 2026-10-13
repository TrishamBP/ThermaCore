"""Cooling model service: wraps data_engineering.cooling_model and maps its errors to domain exceptions.

ModelRegistry is created once by the lifespan and holds the loaded active model (no retraining
at startup or per request). Training replaces it under a lock; concurrent training returns 409.
"""

import threading
from datetime import datetime, timezone

from app.common.logging import get_logger
from app.cooling.exceptions import (
    InsufficientDataError,
    InvalidModelArtifactApiError,
    ModelNotTrainedApiError,
    TrainingInProgressError,
)
from app.cooling.schemas import PredictQuery, RecommendRequest, TrainRequest
from app.exceptions import DatabaseUnavailableError, InvalidRequestError
from app.telemetry.exceptions import AssetNotFoundError, RunNotFoundError
from data_engineering.cooling_model.artifacts import InvalidModelArtifactError, ModelNotTrainedError
from data_engineering.cooling_model.config import CoolingModelSettings
from data_engineering.cooling_model.optimizer import recommend
from data_engineering.cooling_model.predictor import LIMITATIONS, CoolingResponseModel, InsufficientInferenceDataError
from data_engineering.cooling_model.training import InsufficientTrainingDataError, train
from data_engineering.storage.base import SqlTelemetryStore

log = get_logger("cooling_model")


class ModelRegistry:
    def __init__(self, settings: CoolingModelSettings) -> None:
        self.settings = settings
        self.model: CoolingResponseModel | None = None
        self.load_error: Exception | None = None
        self._train_lock = threading.Lock()

    def load(self) -> None:
        try:
            self.model, self.load_error = CoolingResponseModel.load(self.settings.model.artifact_dir), None
            log.info("cooling model loaded", extra={"context": {"version": self.model.version,
                                                                "label": self.model.label}})
        except (ModelNotTrainedError, InvalidModelArtifactError) as exc:
            self.model, self.load_error = None, exc
            log.warning("no usable cooling model", extra={"context": {"error": str(exc)}})

    def require(self) -> CoolingResponseModel:
        if self.model is not None:
            return self.model
        if isinstance(self.load_error, InvalidModelArtifactError):
            raise InvalidModelArtifactApiError(str(self.load_error))
        raise ModelNotTrainedApiError("No cooling response model is trained "
                                      "(run `uv run python -m data_engineering.cooling_model.training`)")


class CoolingModelService:
    def __init__(self, store: SqlTelemetryStore, registry: ModelRegistry) -> None:
        self.store, self.registry = store, registry

    def _run_id(self, run_id: str | None) -> str:
        run_id = run_id or self.store.latest_run_id()
        if run_id is None:
            raise RunNotFoundError("the telemetry store has no simulation runs")
        if not self.store.run_assets(run_id):
            raise RunNotFoundError(f"simulation run {run_id!r} not found")
        return run_id

    def _call(self, fn):
        try:
            return fn()
        except LookupError as exc:
            raise AssetNotFoundError(str(exc)) from exc
        except (InsufficientInferenceDataError, InsufficientTrainingDataError) as exc:
            raise InsufficientDataError(str(exc)) from exc
        except Exception as exc:          # driver errors while querying; details are logged, not returned
            log.error("cooling model request failed", exc_info=exc)
            raise DatabaseUnavailableError("Telemetry store query failed") from exc

    def predict(self, query: PredictQuery) -> dict:
        model = self.registry.require()
        run_id = self._run_id(query.run_id)
        result = self._call(lambda: model.predict_for_asset(self.store, run_id, query.asset_id, query.as_of))
        result.pop("_features", None)
        result.pop("_series", None)
        return result

    def recommend(self, request: RecommendRequest) -> dict:
        model = self.registry.require()
        run_id = self._run_id(request.run_id)
        return self._call(lambda: recommend(model, self.store, run_id, request.asset_id,
                                            self.registry.settings.optimizer, request.as_of))

    # ----- /cooling/predict and /cooling/optimize (explicit status instead of 503) -----
    def availability(self, max_age_days: int | None) -> list[dict]:
        """Reasons the active model must not be used (empty = usable)."""
        reg = self.registry
        if reg.model is None:
            code = "MODEL_ARTIFACT_INVALID" if isinstance(reg.load_error, InvalidModelArtifactError) else \
                "MODEL_NOT_TRAINED"
            return [{"code": code, "detail": str(reg.load_error or "no active cooling response model")}]
        reasons, meta = [], reg.model.metadata
        if meta["feature_config"] != reg.settings.model.features.to_dict():
            reasons.append({"code": "FEATURE_SCHEMA_MISMATCH",
                            "detail": "the artifact was trained with a different feature configuration; retrain"})
        if max_age_days is not None:
            created = datetime.fromisoformat(meta["created_at"])
            age = (datetime.now(timezone.utc) - created).total_seconds() / 86400
            if age > max_age_days:
                reasons.append({"code": "MODEL_STALE",
                                "detail": f"artifact is {age:.1f} days old (API_MODEL_MAX_AGE_DAYS={max_age_days})"})
        return reasons

    def predict_v2(self, request, max_age_days: int | None) -> dict:
        run_id = self._run_id(request.run_id)
        out = {"status": "UNAVAILABLE", "reasons": [], "asset_id": request.asset_id, "simulation_run_id": run_id,
               "observed_at": None, "latest_inlet_temperature_c": None, "forecasts": [], "unsupported_horizons": [],
               "model": None, "input_data_quality": None,
               "uncertainty": {"method": "offline test-split RMSE per horizon (simulated data); not a calibrated "
                                         "prediction interval"},
               "warnings": [], "recommendation_only": True}
        if request.asset_id not in {a["asset_id"] for a in self.store.run_assets(run_id)}:
            raise AssetNotFoundError(f"asset {request.asset_id!r} not found in simulation run {run_id!r}")
        out["reasons"] = self.availability(max_age_days)
        if out["reasons"]:
            return out
        model = self.registry.model
        meta = model.metadata
        trained = meta["horizons_minutes"]
        wanted = request.horizons_minutes or trained
        out["unsupported_horizons"] = [h for h in wanted if h not in trained]
        out["model"] = {"version": model.version, "label": model.label, "created_at": meta["created_at"],
                        "trained_horizons_minutes": trained, "training_source": meta["training_data"].get("source_type")}
        if out["unsupported_horizons"]:
            out["warnings"].append(f"horizons {out['unsupported_horizons']} are not trained; supported: {trained}")
        if not any(h in trained for h in wanted):
            out["reasons"].append({"code": "HORIZON_NOT_SUPPORTED", "detail": f"trained horizons are {trained}"})
            return out
        try:
            p = model.predict_for_asset(self.store, run_id, request.asset_id, request.as_of)
        except InsufficientInferenceDataError as exc:
            out["reasons"].append({"code": "INSUFFICIENT_TELEMETRY", "detail": str(exc)})
            return out
        except LookupError as exc:
            raise AssetNotFoundError(str(exc)) from exc
        p.pop("_features", None)
        p.pop("_series", None)
        for f in p["forecasts"]:
            if f["horizon_minutes"] in wanted:
                ev = meta["evaluation"][str(f["horizon_minutes"])]["test"]
                out["forecasts"].append({**{k: f[k] for k in ("horizon_minutes", "predicted_inlet_temperature_c",
                                                              "persistence_inlet_temperature_c")},
                                         "forecast_timestamp": f["forecast_timestamp"].replace("+00:00", "Z"),
                                         "kind": "model_prediction",
                                         "evaluation": {"offline_test_rmse_c": ev["model"]["rmse"],
                                                        "offline_test_mae_c": ev["model"]["mae"],
                                                        "offline_persistence_rmse_c": ev["persistence"]["rmse"],
                                                        "rmse_skill_vs_persistence": ev["rmse_skill_vs_persistence"],
                                                        "test_rows": ev["model"]["n"]}})
        out.update(status="AVAILABLE", observed_at=p["observed_at"].replace("+00:00", "Z"),
                   latest_inlet_temperature_c=p["latest_inlet_temperature_c"], input_data_quality=p["data_quality"],
                   warnings=[*out["warnings"], *p["model_validity_warnings"],
                             *(f"extrapolated input: {x}" for x in p["extrapolated_features"])])
        return out

    def optimize(self, request, max_age_days: int | None) -> dict:
        cfg = self.registry.settings.optimizer
        unknown = set(request.candidate_settings) - {cfg.controllable_setting}
        if unknown:
            raise InvalidRequestError(
                f"only {cfg.controllable_setting!r} is a configured controllable setting; got {sorted(unknown)}")
        run_id = self._run_id(request.run_id)
        candidates = request.candidate_settings.get(cfg.controllable_setting) or []
        blocked = self.availability(max_age_days)
        model = None if blocked else self.registry.model
        result = self._call(lambda: recommend(model, self.store, run_id, request.asset_id, cfg, request.as_of,
                                              candidates=candidates))
        if blocked and model is None and self.registry.model is not None:
            result["reasons"] = blocked + [r for r in result["reasons"] if r["code"] != "MODEL_NOT_TRAINED"]
        result["conclusion"] = "RECOMMENDATION" if result["status"] == "RECOMMENDATION_AVAILABLE" else "INCONCLUSIVE"
        result["candidate_values_note"] = ("candidate values come from the request; they are not approved operating "
                                           "limits and are checked against configured bounds only")
        return result

    def evaluate(self) -> dict:
        m = self.registry.require().metadata
        return {"model_version": m["version"], "model_label": m["model_label"], "created_at": m["created_at"],
                "target": m.get("target", ""), "horizons_minutes": m["horizons_minutes"],
                "training_data": m["training_data"], "evaluation": m["evaluation"], "validity": m["validity"],
                "limitations": LIMITATIONS}

    def train(self, request: TrainRequest) -> dict:
        if not self.registry._train_lock.acquire(blocking=False):
            raise TrainingInProgressError()
        try:
            try:
                meta = train(self.store, self.registry.settings, request.run_ids)
            except InsufficientTrainingDataError as exc:
                raise InsufficientDataError(str(exc)) from exc
            except LookupError as exc:
                raise RunNotFoundError(str(exc)) from exc
            self.registry.load()
        finally:
            self.registry._train_lock.release()
        return {"model_version": meta["version"], "model_label": meta["model_label"],
                "duration_seconds": meta["duration_seconds"],
                "test": {h: v["test"] for h, v in meta["evaluation"].items()}}
