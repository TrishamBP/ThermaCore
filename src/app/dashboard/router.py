"""Dashboard routes (mounted under /api/v1). Read-only."""

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request
from pydantic import Field

from app.common.schemas.base import CustomModel, UtcDatetime
from app.common.schemas.responses import ErrorResponse
from app.common.security.auth import OperatorScope
from app.dashboard.service import DashboardService
from app.database import get_read_session
from app.telemetry.constants import ID_PATTERN
from data_engineering.storage.base import SqlTelemetryStore

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


class OverviewQuery(CustomModel):
    asset_id: str | None = Field(None, pattern=ID_PATTERN)
    run_id: str | None = Field(None, pattern=ID_PATTERN)
    start_time: UtcDatetime | None = Field(None, description="Trend window start (exclusive)")
    end_time: UtcDatetime | None = Field(None, description="Window end (inclusive); default: newest reading")


class Overview(CustomModel):
    simulation_run_id: str
    scope: dict[str, Any]
    window: dict[str, Any]
    latest_telemetry_timestamp: str | None
    assets: dict[str, Any]
    kpis: dict[str, dict[str, Any]]
    thermal_risk: dict[str, Any]
    anomalies: dict[str, Any]
    trends: dict[str, Any]
    data_quality: dict[str, Any]
    unavailable_measurements: dict[str, str]
    semantics: str
    warnings: list[str]


@router.get("/overview", response_model=Overview,
            responses={403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 422: {"model": ErrorResponse},
                       503: {"model": ErrorResponse}},
            summary="Dashboard KPIs, thermal risk, anomalies, trends and data quality")
def overview(request: Request, query: Annotated[OverviewQuery, Query()],
             store: Annotated[SqlTelemetryStore, Depends(get_read_session)], scope: OperatorScope) -> dict:
    service = DashboardService(store, request.app.state.settings, scope, request.app.state.cooling_config)
    return service.overview(query.asset_id, query.run_id, query.start_time, query.end_time)
