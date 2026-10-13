"""The single FastAPI entry point.

    uv run uvicorn app.main:app --host 127.0.0.1 --port 8000

The lifespan creates the shared, long-lived objects once:
  - the cooling estimator config (validated; an invalid config stops startup instead of failing requests),
  - the StoreFactory (per-request connections to the telemetry store),
  - the threadpool limiter for sync handlers (API_THREADPOOL_TOKENS).
A database that is down at startup does not stop the app: requests return 503 until it is back.
"""

from contextlib import asynccontextmanager

import anyio.to_thread
from fastapi import FastAPI

from app.api.v1.router import api_router
from app.common.logging import get_logger
from app.config import AppSettings
from app.constants import API_TITLE, API_V1_PREFIX, API_VERSION, LEGACY_API_PREFIX
from app.database import StoreFactory
from app.exceptions import register_exception_handlers
from app.middleware import register_middleware
from data_engineering.cooling.config import CoolingConfig, load_cooling_config
from data_engineering.streaming.settings import ConsumerSettings, load_consumer_settings

log = get_logger("main")


def create_app(settings: AppSettings | None = None, *, store_settings: ConsumerSettings | None = None,
               cooling_config: CoolingConfig | None = None) -> FastAPI:
    """Build the app. Arguments override environment-derived settings (used by tests)."""
    settings = settings or AppSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.cooling_config = cooling_config or load_cooling_config()
        app.state.store_factory = StoreFactory(store_settings or load_consumer_settings(),
                                               settings.db_connect_timeout_s)
        anyio.to_thread.current_default_thread_limiter().total_tokens = settings.threadpool_tokens
        log.info("api starting", extra={"context": {
            "environment": settings.environment, "store": app.state.store_factory.describe(),
            "window_minutes": app.state.cooling_config.window_minutes,
            "threadpool_tokens": settings.threadpool_tokens}})
        yield
        log.info("api stopped")

    docs = settings.docs_enabled
    app = FastAPI(title=API_TITLE, version=API_VERSION, lifespan=lifespan,
                  description="Read-only access to SIMULATED data center telemetry. No control actions.",
                  docs_url="/docs" if docs else None, redoc_url="/redoc" if docs else None,
                  openapi_url="/openapi.json" if docs else None)
    app.state.settings = settings
    register_exception_handlers(app)
    register_middleware(app, settings)
    app.include_router(api_router, prefix=API_V1_PREFIX)
    # Deprecated pre-v1 paths (/api/health, /api/telemetry/cooling-state): same handlers, hidden from the schema.
    app.include_router(api_router, prefix=LEGACY_API_PREFIX, deprecated=True, include_in_schema=False)
    return app


app = create_app()
