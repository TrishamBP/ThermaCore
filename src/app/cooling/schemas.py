"""Cooling model API schemas. Detailed evaluation/validity payloads are passed through as the
metadata recorded at training time (dicts), so the API cannot drift from the artifact."""

import math
from typing import Any, Literal

from pydantic import Field, field_validator

from app.common.schemas.base import CustomModel, UtcDatetime
from app.telemetry.constants import ID_PATTERN


class PredictQuery(CustomModel):
    asset_id: str = Field(..., pattern=ID_PATTERN)
    run_id: str | None = Field(None, pattern=ID_PATTERN,
                               description="simulation_run_id; default: the run that most recently received readings")
    as_of: UtcDatetime | None = Field(None, description="Forecast origin in event time; default: newest reading")


class RecommendRequest(PredictQuery):
    pass


class TrainRequest(CustomModel):
    run_ids: list[str] | None = Field(None, max_length=50, description="Default: all eligible runs, one per seed")


class Forecast(CustomModel):
    horizon_minutes: int
    forecast_timestamp: str
    predicted_inlet_temperature_c: float
    persistence_inlet_temperature_c: float
    offline_test_rmse_c: float | None
    offline_persistence_rmse_c: float | None


class DataQuality(CustomModel):
    status: Literal["OK", "DEGRADED"]
    notes: list[str]
    ingest_age_seconds: float
    readings_in_lookback: int
    duplicates_dropped: int


class PredictionResponse(CustomModel):
    asset_id: str
    simulation_run_id: str
    asset_type: str | None
    observed_at: str
    latest_inlet_temperature_c: float
    forecasts: list[Forecast]
    model_version: str
    model_label: Literal["forecasting_baseline", "intervention_validated"]
    data_quality: DataQuality
    model_validity_warnings: list[str]
    extrapolated_features: list[str]


class Reason(CustomModel):
    code: str
    detail: str


class RecommendationResponse(CustomModel):
    status: Literal["RECOMMENDATION_AVAILABLE", "NO_FEASIBLE_SETTING", "CONSTRAINTS_ONLY", "UNAVAILABLE"]
    reasons: list[Reason]
    recommendation_only: Literal[True]
    actuation: str
    asset_id: str
    simulation_run_id: str
    controllable_setting: str
    current_setting: float | None
    recommended_setting: float | None
    selection_reason: str | None
    forecast_under_current_conditions: list[Forecast]
    current_conditions_check: dict[str, Any] | None
    candidates: list[dict[str, Any]]
    energy_objective: dict[str, Any]
    model_version: str | None
    model_label: str | None
    warnings: list[str]


class EvaluationResponse(CustomModel):
    model_version: str
    model_label: str
    created_at: str
    target: str
    horizons_minutes: list[int]
    training_data: dict[str, Any]
    evaluation: dict[str, Any]
    validity: dict[str, Any]
    limitations: list[str]


class TrainResponse(CustomModel):
    model_version: str
    model_label: str
    duration_seconds: float
    test: dict[str, Any]


# ---------------------------------------------------------------- /cooling/predict, /cooling/optimize

class CoolingPredictRequest(PredictQuery):
    horizons_minutes: list[int] | None = Field(None, min_length=1, max_length=10,
                                               description="Subset of the trained horizons; default: all trained")


class HorizonForecast(CustomModel):
    horizon_minutes: int
    forecast_timestamp: str
    predicted_inlet_temperature_c: float
    persistence_inlet_temperature_c: float
    kind: Literal["model_prediction"]
    evaluation: dict[str, Any]


class CoolingPredictResponse(CustomModel):
    status: Literal["AVAILABLE", "UNAVAILABLE"]
    reasons: list[Reason]
    asset_id: str
    simulation_run_id: str
    observed_at: str | None
    latest_inlet_temperature_c: float | None
    forecasts: list[HorizonForecast]
    unsupported_horizons: list[int]
    model: dict[str, Any] | None
    input_data_quality: dict[str, Any] | None
    uncertainty: dict[str, str]
    warnings: list[str]
    recommendation_only: Literal[True]


class CoolingOptimizeRequest(PredictQuery):
    candidate_settings: dict[str, list[float]] = Field(
        ..., description="e.g. {\"fan_speed_rpm\": [3000, 3200]}: illustrative values, not approved limits")

    @field_validator("candidate_settings")
    @classmethod
    def _bounded(cls, v):
        if not v or any(not 1 <= len(vals) <= 50 for vals in v.values()):
            raise ValueError("give 1 to 50 candidate values per setting")
        if any(not math.isfinite(x) for vals in v.values() for x in vals):
            raise ValueError("candidate values must be finite numbers")
        return v


class CoolingOptimizeResponse(RecommendationResponse):
    conclusion: Literal["RECOMMENDATION", "INCONCLUSIVE"]
    candidate_values_note: str
