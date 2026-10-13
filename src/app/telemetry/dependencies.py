"""Telemetry domain dependencies."""

from typing import Annotated

from fastapi import Depends, Request

from app.database import get_db_session
from app.telemetry.service import TelemetryService
from data_engineering.cooling.config import CoolingConfig
from data_engineering.storage.base import SqlTelemetryStore


def get_cooling_config(request: Request) -> CoolingConfig:
    """Loaded and validated once by the lifespan (an invalid config stops the app at startup)."""
    return request.app.state.cooling_config


def get_telemetry_service(store: Annotated[SqlTelemetryStore, Depends(get_db_session)],
                          config: Annotated[CoolingConfig, Depends(get_cooling_config)]) -> TelemetryService:
    return TelemetryService(store, config)
