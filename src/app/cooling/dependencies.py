"""Cooling model domain dependencies."""

from typing import Annotated

from fastapi import Depends, Request

from app.cooling.exceptions import TrainingDisabledError
from app.cooling.service import CoolingModelService, ModelRegistry
from app.database import get_db_session
from data_engineering.storage.base import SqlTelemetryStore


def get_model_registry(request: Request) -> ModelRegistry:
    """Created by the lifespan; the active artifact is loaded once, not per request."""
    return request.app.state.cooling_model_registry


def get_cooling_model_service(store: Annotated[SqlTelemetryStore, Depends(get_db_session)],
                              registry: Annotated[ModelRegistry, Depends(get_model_registry)]) -> CoolingModelService:
    return CoolingModelService(store, registry)


def require_training_enabled(request: Request) -> None:
    if not request.app.state.settings.model_training_enabled:
        raise TrainingDisabledError("Model training over the API is disabled (set API_MODEL_TRAINING_ENABLED=true)")
