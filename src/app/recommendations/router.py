"""Recommendation routes (mounted under /api/v1). Recommendation-only: nothing here commands equipment."""

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from app.common.schemas.responses import ErrorResponse
from app.recommendations.dependencies import get_analysis_service, get_recommendation_service, get_status_service
from app.recommendations.schemas import (
    ALERT_ID_PATTERN,
    REC_ID_PATTERN,
    AlertList,
    AlertQuery,
    AgentStatus,
    AnalysisResponse,
    AnalyzeRequest,
    DecisionRequest,
    DecisionResponse,
    ListQuery,
    RecommendationDetail,
    RecommendationList,
)
from app.recommendations.service import RecommendationService

router = APIRouter(prefix="/recommendations", tags=["recommendations"])
ERRORS = {404: {"model": ErrorResponse, "description": "unknown_asset / unknown_run / unknown_recommendation"},
          422: {"model": ErrorResponse, "description": "validation_error"},
          503: {"model": ErrorResponse, "description": "database_unavailable / store_not_initialized"}}
RecId = Annotated[str, Path(pattern=REC_ID_PATTERN)]


@router.post("/generate", response_model=AnalysisResponse, responses=ERRORS,
             summary="Generate recommendations for one asset (multi-agent; may take tens of seconds with LLMs)")
def generate(request: AnalyzeRequest,
            service: Annotated[RecommendationService, Depends(get_analysis_service)]) -> dict:
    return service.analyze(request)


@router.get("", response_model=RecommendationList, responses=ERRORS, summary="Recent recommendations")
def list_recommendations(query: Annotated[ListQuery, Query()],
                         service: Annotated[RecommendationService, Depends(get_recommendation_service)]) -> dict:
    return service.list(query)


@router.get("/agents/status", response_model=AgentStatus, summary="Agent and model configuration (no secrets)")
def agent_status(service: Annotated[RecommendationService, Depends(get_status_service)]) -> dict:
    return service.status()


@router.get("/analyses/{analysis_id}", responses=ERRORS, summary="Execution trace of one analysis")
def get_analysis(analysis_id: RecId,
                 service: Annotated[RecommendationService, Depends(get_recommendation_service)]) -> dict:
    return service.analysis(analysis_id)


@router.get("/{recommendation_id}", response_model=RecommendationDetail, responses=ERRORS,
            summary="One recommendation with its evidence and review decisions")
def get_recommendation(recommendation_id: RecId,
                       service: Annotated[RecommendationService, Depends(get_recommendation_service)]) -> dict:
    return service.detail(recommendation_id)


@router.post("/{recommendation_id}/decisions", response_model=DecisionResponse, responses=ERRORS,
             summary="Record a human review decision (never executes anything)")
def decide(recommendation_id: RecId, request: DecisionRequest,
           service: Annotated[RecommendationService, Depends(get_recommendation_service)]) -> dict:
    return service.decide(recommendation_id, request)


alerts_router = APIRouter(prefix="/alerts", tags=["alerts"])


@alerts_router.get("", response_model=AlertList, responses=ERRORS,
                   summary="Alerts (WARNING/CRITICAL estimator findings captured at generation time)")
def list_alerts(query: Annotated[AlertQuery, Query()],
                service: Annotated[RecommendationService, Depends(get_recommendation_service)]) -> dict:
    return service.alerts(query)


@alerts_router.get("/{alert_id}", responses=ERRORS, summary="One alert with its evidence")
def get_alert(alert_id: Annotated[str, Path(pattern=ALERT_ID_PATTERN)],
              service: Annotated[RecommendationService, Depends(get_recommendation_service)]) -> dict:
    return service.alert(alert_id)
