"""Digital twin routes (mounted under /api/v1/digital-twin). Read-only and recommendation-only."""

import asyncio
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Header, Path, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import Field

from app.common.schemas.base import CustomModel, UtcDatetime
from app.common.schemas.responses import ErrorResponse
from app.common.security.auth import AssetScope, OperatorScope
from app.database import get_read_session
from app.digital_twin.service import OVERLAY_TYPES, LayoutUnavailableError, TwinService
from app.digital_twin.stream import sse
from app.exceptions import AppError
from app.telemetry.constants import ID_PATTERN
from data_engineering.digital_twin.layout import LayoutError, LayoutNotConfiguredError
from data_engineering.storage.base import SqlTelemetryStore

router = APIRouter(prefix="/digital-twin", tags=["digital-twin"])
ERRORS = {403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 422: {"model": ErrorResponse},
          503: {"model": ErrorResponse, "description": "layout_not_configured / layout_invalid / database_unavailable"}}
ObjectId = Annotated[str, Path(pattern=ID_PATTERN)]


class LayoutInvalidError(AppError):
    status_code, code, message = 503, "layout_invalid", "The digital twin layout file is invalid"


class TooManySubscribersError(AppError):
    status_code, code, message = 503, "stream_capacity_reached", "Too many live-update subscribers"


def layout(request: Request):
    try:
        return request.app.state.layout_provider.get()
    except LayoutNotConfiguredError as exc:
        raise LayoutUnavailableError(str(exc)) from exc
    except LayoutError as exc:
        raise LayoutInvalidError(str(exc)) from exc


def twin(request: Request, store: Annotated[SqlTelemetryStore, Depends(get_read_session)],
         scope: OperatorScope) -> TwinService:
    s = request.app.state
    return TwinService(store, layout(request), s.settings, scope, s.cooling_config, s.cooling_model_registry,
                       s.copilot, s.recommendation_engine.workflow.fan_peer)


Twin = Annotated[TwinService, Depends(twin)]
RunId = Annotated[str | None, Query(pattern=ID_PATTERN)]


@router.get("/layout", responses=ERRORS, summary="Facility hierarchy with coordinates (demonstration layout)")
def get_layout(service: Twin, room_id: Annotated[str | None, Query(pattern=ID_PATTERN)] = None,
               include_servers: bool = True, include_cooling_equipment: bool = True,
               layout_version: Annotated[str | None, Query(max_length=80)] = None) -> dict:
    return service.layout_view(room_id, include_servers, include_cooling_equipment, layout_version)


@router.get("/layout/{object_id}", responses=ERRORS, summary="One layout object with parents and children")
def get_layout_object(object_id: ObjectId, service: Twin) -> dict:
    return service.layout_object(object_id)


@router.get("/state", responses=ERRORS, summary="Latest telemetry and status of every mapped asset (batch)")
def get_state(service: Twin, run_id: RunId = None, room_id: Annotated[str | None, Query(pattern=ID_PATTERN)] = None,
              asset_type: Literal["server"] | None = None,
              asset_ids: Annotated[str | None, Query(max_length=4000, description="comma-separated")] = None,
              since: Annotated[UtcDatetime | None, Query(description="only assets with newer readings")] = None,
              include_derived_metrics: bool = True) -> dict:
    ids = [a.strip() for a in asset_ids.split(",") if a.strip()] if asset_ids else None
    return service.state(run_id, room_id, asset_type, ids, since, include_derived_metrics)


@router.get("/overlays", responses=ERRORS, summary=f"Overlay values per object ({', '.join(OVERLAY_TYPES)})")
def get_overlays(service: Twin, type: Annotated[str, Query(max_length=40)] = "temperature", run_id: RunId = None,
                 room_id: Annotated[str | None, Query(pattern=ID_PATTERN)] = None,
                 asset_type: Literal["server"] | None = None,
                 timestamp: Annotated[UtcDatetime | None, Query(description="historical overlay at this time")] = None
                 ) -> dict:
    return service.overlays(type, run_id, room_id, asset_type, timestamp)


@router.get("/assets/{object_id}", responses=ERRORS,
            summary="Inspection panel: metadata, parents, latest values, history, forecast, alerts, recommendations")
def inspect(object_id: ObjectId, service: Twin, request: Request, run_id: RunId = None,
            start_time: UtcDatetime | None = None, end_time: UtcDatetime | None = None,
            metrics: Annotated[str | None, Query(max_length=500)] = None, include_forecast: bool = True,
            include_recommendations: bool = True) -> dict:
    from app.recommendations.service import RecommendationService
    s = request.app.state
    recs = RecommendationService(service.store, s.recommendation_engine, s.llm_settings, s.cooling_model_registry,
                                 service.scope)
    return service.inspect(object_id, run_id, start_time, end_time, metrics, include_forecast,
                           include_recommendations, recs)


class TimeRange(CustomModel):
    start: UtcDatetime | None = None
    end: UtcDatetime | None = None


class AnalyzeRequest(CustomModel):
    object_id: str | None = Field(None, pattern=ID_PATTERN, description="Layout object id (server, rack, ...)")
    asset_id: str | None = Field(None, pattern=ID_PATTERN, description="Telemetry asset id (alternative)")
    question: str = Field(..., min_length=1, max_length=2000)
    analysis_type: Literal["thermal", "cooling_efficiency", "workload", "energy", "maintenance", "forecast",
                           "recommendations"] | None = None
    run_id: str | None = Field(None, pattern=ID_PATTERN)
    time_range: TimeRange | None = None


@router.post("/analyze", responses=ERRORS, summary="Analyse the selected object with the copilot workflow")
def analyze(body: AnalyzeRequest, service: Twin) -> dict:
    target = body.object_id or body.asset_id
    if target is None:
        from app.exceptions import InvalidRequestError
        raise InvalidRequestError("give object_id or asset_id")
    tr = body.time_range or TimeRange()
    return service.analyze(target, body.question, body.analysis_type, tr.start, tr.end, body.run_id)


@router.get("/stream", summary="Server-Sent Events: telemetry_update, asset_status_change, alert_created, "
                               "recommendation_created, layout_changed, heartbeat, resync_required")
async def stream(request: Request, scope: OperatorScope,
                 last_event_id: Annotated[int | None, Header(alias="Last-Event-ID", ge=0)] = None,
                 max_events: Annotated[int | None, Query(ge=1, le=10000, description="close after N events "
                                                                                    "(bounded/polling clients)")] = None
                 ) -> StreamingResponse:
    broadcaster = request.app.state.twin_broadcaster
    heartbeat_s = request.app.state.settings.twin_heartbeat_s
    try:
        sub = broadcaster.subscribe(scope, last_event_id)
    except OverflowError as exc:
        raise TooManySubscribersError() from exc

    async def events():
        sent = 0
        try:
            yield sse(broadcaster._event("heartbeat", None, {"subscribed": True, "poll_interval_s":
                                                             broadcaster.poll_interval_s}))
            sent += 1
            while max_events is None or sent < max_events:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(sub.queue.get(), timeout=heartbeat_s)
                except TimeoutError:
                    event = broadcaster._event("heartbeat", None, {})
                yield sse(event)
                sent += 1
        finally:
            broadcaster.unsubscribe(sub)

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})


def snapshot_fn(app_state):
    """Compact all-asset snapshot for the broadcaster (unrestricted; events are scope-filtered per client)."""
    def snapshot() -> dict:
        lay = app_state.layout_provider.get()
        store = app_state.store_factory.open(read_only=True)
        try:
            service = TwinService(store, lay, app_state.settings, AssetScope([]), app_state.cooling_config)
            _, _, states = service._states(None)
            alerts = store.list_alerts(limit=200)
            recs = store.list_recommendations(limit=200)
        finally:
            store.close()
        keep = ("object_id", "asset_id", "status", "thermal_status", "freshness", "last_telemetry_timestamp",
                "active_alerts", "open_recommendations")
        return {"layout_version": lay.version,
                "assets": {aid: {**{k: s.get(k) for k in keep},
                                 "inlet_temperature_c": s["values"].get("inlet_temperature_c", {}).get("value")}
                           for aid, s in states.items()},
                "alerts": {a["alert_id"]: {"alert_id": a["alert_id"], "asset_id": a["asset_id"],
                                           "rule_id": a["rule_id"], "severity": a["severity"]} for a in alerts},
                "recommendations": {r["recommendation_id"]: {"recommendation_id": r["recommendation_id"],
                                                             "asset_id": r["asset_id"],
                                                             "type": r["recommendation_type"], "status": r["status"]}
                                    for r in recs}}
    return snapshot

