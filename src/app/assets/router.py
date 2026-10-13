"""Telemetry history and asset routes (mounted under /api/v1). Read-only."""

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Request

from app.assets.schemas import (
    AssetDetail,
    AssetList,
    AssetListQuery,
    AssetTelemetryQuery,
    TelemetryQuery,
    TelemetryResponse,
)
from app.assets.service import AssetService
from app.common.schemas.responses import ErrorResponse
from app.common.security.auth import OperatorScope
from app.database import get_read_session
from app.telemetry.constants import ID_PATTERN
from data_engineering.storage.base import SqlTelemetryStore

router = APIRouter(tags=["assets"])
ERRORS = {403: {"model": ErrorResponse, "description": "asset_not_authorized"},
          404: {"model": ErrorResponse, "description": "unknown_asset / unknown_run"},
          422: {"model": ErrorResponse, "description": "validation_error / invalid_parameter"},
          503: {"model": ErrorResponse, "description": "database_unavailable"}}
AssetId = Annotated[str, Path(pattern=ID_PATTERN)]


def get_asset_service(request: Request, store: Annotated[SqlTelemetryStore, Depends(get_read_session)],
                      scope: OperatorScope) -> AssetService:
    return AssetService(store, request.app.state.settings, scope, request.app.state.cooling_config)


Service = Annotated[AssetService, Depends(get_asset_service)]


@router.get("/telemetry", response_model=TelemetryResponse, responses=ERRORS,
            summary="Telemetry history (raw or bucketed) for chart rendering")
def telemetry(query: Annotated[TelemetryQuery, Query()], service: Service) -> dict:
    return service.telemetry(query)


@router.get("/assets", response_model=AssetList, responses=ERRORS, summary="Assets of a run with freshness")
def list_assets(query: Annotated[AssetListQuery, Query()], service: Service) -> dict:
    return service.list(query)


@router.get("/assets/{asset_id}", response_model=AssetDetail, responses=ERRORS,
            summary="Asset metadata, latest reading and current cooling state")
def asset_detail(asset_id: AssetId, service: Service,
                 run_id: Annotated[str | None, Query(pattern=ID_PATTERN)] = None) -> dict:
    return service.detail(asset_id, run_id)


@router.get("/assets/{asset_id}/telemetry", response_model=TelemetryResponse, responses=ERRORS,
            summary="Telemetry history of one asset")
def asset_telemetry(asset_id: AssetId, query: Annotated[AssetTelemetryQuery, Query()], service: Service) -> dict:
    return service.telemetry(query, asset_id)
