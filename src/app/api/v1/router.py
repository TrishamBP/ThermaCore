"""Aggregates the v1 domain routers. New domains add one include_router line here."""

from typing import Annotated, Literal

from fastapi import APIRouter, Depends

from app.common.schemas.base import CustomModel
from app.common.schemas.responses import ErrorResponse
from app.database import StoreFactory, get_store_factory
from app.assets.router import router as assets_router
from app.common.security.auth import require_operator
from app.cooling.router import router as cooling_router
from app.copilot.router import router as copilot_router
from app.dashboard.router import router as dashboard_router
from app.digital_twin.router import router as twin_router
from app.energy.router import router as energy_router
from app.recommendations.router import alerts_router
from app.recommendations.router import router as recommendations_router
from app.telemetry.router import router as telemetry_router
from data_engineering.storage.base import SCHEMA_VERSION

api_router = APIRouter()


class HealthResponse(CustomModel):
    status: Literal["ok", "degraded"]
    database: str                 # backend name only, never the connection target
    schema_version: int | None


@api_router.get("/health", tags=["system"], response_model=HealthResponse,
                responses={503: {"model": ErrorResponse}})
def health(factory: Annotated[StoreFactory, Depends(get_store_factory)]) -> HealthResponse:
    store = factory.open(require_schema=False)
    try:
        version = store.schema_version()
    finally:
        store.close()
    return HealthResponse(status="ok" if version == SCHEMA_VERSION else "degraded",
                          database=factory.settings.db_backend, schema_version=version)


# Every domain except /health sits behind the (opt-in) operator token; see app.common.security.auth.
_protected = [Depends(require_operator)]
for _router in (dashboard_router, telemetry_router, assets_router, cooling_router, recommendations_router, alerts_router,
                copilot_router, twin_router, energy_router):
    api_router.include_router(_router, dependencies=_protected)
