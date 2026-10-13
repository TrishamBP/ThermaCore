"""Cooling model and optimizer routes (mounted under /api/v1). Nothing here actuates equipment."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from app.common.schemas.responses import ErrorResponse
from app.common.security.auth import OperatorScope
from app.cooling.dependencies import get_cooling_model_service, get_model_registry, require_training_enabled
from app.cooling.schemas import (
    CoolingOptimizeRequest,
    CoolingOptimizeResponse,
    CoolingPredictRequest,
    CoolingPredictResponse,
    EvaluationResponse,
    PredictionResponse,
    PredictQuery,
    RecommendationResponse,
    RecommendRequest,
    TrainRequest,
    TrainResponse,
)
from app.cooling.service import CoolingModelService, ModelRegistry

router = APIRouter(tags=["cooling-model"])
ERRORS = {404: {"model": ErrorResponse, "description": "unknown_asset / unknown_run"},
          422: {"model": ErrorResponse, "description": "validation_error / insufficient_data"},
          503: {"model": ErrorResponse,
                "description": "model_not_trained / model_artifact_invalid / database_unavailable"}}


@router.post("/cooling-model/train", response_model=TrainResponse,
             responses={**ERRORS, 403: {"model": ErrorResponse, "description": "training_disabled"},
                        409: {"model": ErrorResponse, "description": "training_in_progress"}},
             dependencies=[Depends(require_training_enabled)],
             summary="Retrain the forecaster on stored telemetry (disabled unless API_MODEL_TRAINING_ENABLED)")
def train_model(request: TrainRequest,
                service: Annotated[CoolingModelService, Depends(get_cooling_model_service)]) -> dict:
    return service.train(request)


@router.get("/cooling-model/evaluate", response_model=EvaluationResponse, responses=ERRORS,
            summary="Offline evaluation and validity checks of the active model")
def evaluate_model(registry: Annotated[ModelRegistry, Depends(get_model_registry)]) -> dict:
    return CoolingModelService(None, registry).evaluate()    # metadata only; no database access


@router.get("/cooling-model/predict", response_model=PredictionResponse, responses=ERRORS,
            summary="Inlet temperature forecast (5/10 min) under observed conditions")
def predict(query: Annotated[PredictQuery, Query()], scope: OperatorScope,
            service: Annotated[CoolingModelService, Depends(get_cooling_model_service)]) -> dict:
    scope.check(query.asset_id)
    return service.predict(query)


@router.post("/cooling-optimizer/recommend", response_model=RecommendationResponse, responses=ERRORS,
             summary="Constrained setting recommendation (recommendation-only; may be UNAVAILABLE)")
def recommend(request: RecommendRequest, scope: OperatorScope,
              service: Annotated[CoolingModelService, Depends(get_cooling_model_service)]) -> dict:
    scope.check(request.asset_id)
    return service.recommend(request)


@router.post("/cooling/predict", response_model=CoolingPredictResponse, responses=ERRORS,
             summary="XGBoost inlet forecast for the latest telemetry; status UNAVAILABLE when the model can't be used")
def cooling_predict(body: CoolingPredictRequest, request: Request, scope: OperatorScope,
                    service: Annotated[CoolingModelService, Depends(get_cooling_model_service)]) -> dict:
    scope.check(body.asset_id)
    return service.predict_v2(body, request.app.state.settings.model_max_age_days)


@router.post("/cooling/optimize", response_model=CoolingOptimizeResponse, responses=ERRORS,
             summary="Evaluate operator-supplied candidate settings (recommendation-only; never actuates)")
def cooling_optimize(body: CoolingOptimizeRequest, request: Request, scope: OperatorScope,
                     service: Annotated[CoolingModelService, Depends(get_cooling_model_service)]) -> dict:
    scope.check(body.asset_id)
    return service.optimize(body, request.app.state.settings.model_max_age_days)
