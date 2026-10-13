"""Recommendation domain dependencies."""

from typing import Annotated

from fastapi import Depends, Request

from app.common.security.auth import get_asset_scope
from app.database import get_db_session
from app.recommendations.service import RecommendationService
from data_engineering.storage.base import SqlTelemetryStore


def _service(request: Request, store) -> RecommendationService:
    state = request.app.state
    return RecommendationService(store, state.recommendation_engine, state.llm_settings, state.cooling_model_registry,
                                 get_asset_scope(request))


def get_recommendation_service(request: Request,
                               store: Annotated[SqlTelemetryStore, Depends(get_db_session)]) -> RecommendationService:
    return _service(request, store)


def get_analysis_service(request: Request) -> RecommendationService:
    """For analyses: the engine opens its own short-lived connections (no request-held connection)."""
    request.app.state.store_factory.open().close()      # 503 early if the store is unavailable
    return _service(request, None)


def get_status_service(request: Request) -> RecommendationService:
    return _service(request, None)
