"""Aggregates the v1 domain routers. New domains add one include_router line here."""

from typing import Annotated, Literal

from fastapi import APIRouter, Depends

from app.common.schemas.base import CustomModel
from app.common.schemas.responses import ErrorResponse
from app.database import StoreFactory, get_store_factory
from app.telemetry.router import router as telemetry_router

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
    return HealthResponse(status="ok" if version == 1 else "degraded",
                          database=factory.settings.db_backend, schema_version=version)


api_router.include_router(telemetry_router)
