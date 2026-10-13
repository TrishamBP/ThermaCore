"""Telemetry routes (mounted under /api/v1)."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.common.schemas.responses import ErrorResponse
from app.telemetry.dependencies import get_telemetry_service
from app.telemetry.schemas import CoolingStateQuery, CoolingStateSnapshot
from app.telemetry.service import TelemetryService

router = APIRouter(prefix="/telemetry", tags=["telemetry"])
ERRORS = {404: {"model": ErrorResponse, "description": "unknown_asset / unknown_run"},
          422: {"model": ErrorResponse, "description": "validation_error / invalid_parameter"},
          503: {"model": ErrorResponse, "description": "database_unavailable / store_not_initialized"}}


@router.get("/cooling-state", response_model=CoolingStateSnapshot, responses=ERRORS,
            summary="Current cooling/thermal state snapshot (read-only, rules-based)")
def get_cooling_state(query: Annotated[CoolingStateQuery, Query()],
                      service: Annotated[TelemetryService, Depends(get_telemetry_service)]) -> CoolingStateSnapshot:
    # Sync on purpose: database work runs in FastAPI's threadpool, never on the event loop.
    return service.cooling_state(query)
