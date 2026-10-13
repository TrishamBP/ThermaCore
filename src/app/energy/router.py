"""Energy routes (mounted under /api/v1). Read-only; relative indicators only (no measured cooling power)."""

from collections import Counter
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from app.assets.scope import run_assets
from app.common.schemas.base import UtcDatetime
from app.common.schemas.responses import ErrorResponse
from app.common.security.auth import OperatorScope
from app.database import get_read_session
from app.exceptions import InvalidRequestError
from app.telemetry.constants import ID_PATTERN
from data_engineering.energy import fan_peer
from data_engineering.storage.base import SqlTelemetryStore

router = APIRouter(prefix="/energy", tags=["energy"])


@router.get("/fan-peer-comparison",
            responses={403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 422: {"model": ErrorResponse}},
            summary="Fan over-speed vs comparable peers per asset (energy-waste screening; relative only, no kW)")
def fan_peer_comparison(request: Request, store: Annotated[SqlTelemetryStore, Depends(get_read_session)],
                        scope: OperatorScope, run_id: Annotated[str | None, Query(pattern=ID_PATTERN)] = None,
                        asset_id: Annotated[str | None, Query(pattern=ID_PATTERN)] = None,
                        as_of: Annotated[UtcDatetime | None, Query(description="window end; default newest reading")]
                        = None) -> dict:
    run_id, assets = run_assets(store, run_id, scope, asset_id)
    if not assets:
        raise InvalidRequestError("no assets of this run are within the operator's scope")
    end = as_of or max(a["last_event_timestamp"] for a in assets.values())
    cfg = request.app.state.recommendation_engine.workflow.fan_peer
    # peers may include assets outside the operator's scope (they are only aggregated: median/percentile)
    result = fan_peer.analyze(store, run_id, end, cfg, sorted(assets))
    for r in result["results"]:
        r["reference_assets"] = [a for a in r["reference_assets"] if scope.permits(a)]
    return {"simulation_run_id": run_id, **result,
            "summary": dict(Counter(r["status"] for r in result["results"])),
            "energy_units": "none: relative indicators only (no measured cooling power; power_consumption_kw is "
                            "server IT power)", "recommendation_only": True}
