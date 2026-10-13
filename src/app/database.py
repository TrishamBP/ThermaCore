"""Database access for the API: wraps the existing telemetry store (no ORM).

The Kafka consumer writes the store through data_engineering.storage (SQLite or PostgreSQL,
hand-written SQL in two dialects); the API reads the same store through the same adapters, so
there is one schema and no ORM metadata to keep in sync. (No SQLAlchemy metadata/naming
convention exists yet for that reason.)

The lifespan creates one StoreFactory; `get_db_session` opens one store connection per request
and closes it afterwards. Connections are per request and never shared between requests. Because FastAPI runs a sync
dependency and the sync handler in different threadpool threads, SQLite connections are opened
with check_same_thread=False; each is still used by only one thread at a time.
"""

from collections.abc import Iterator

from fastapi import Request

from app.common.logging import get_logger
from app.exceptions import DatabaseUnavailableError, StoreNotInitializedError
from data_engineering.storage.base import (
    SqlTelemetryStore,
    StoreNotInitialized,
    open_store,
)
from data_engineering.streaming.settings import ConsumerSettings, redact_url

log = get_logger("database")


class StoreFactory:
    def __init__(self, settings: ConsumerSettings, connect_timeout_s: int, query_timeout_s: float | None = None) -> None:
        self.settings, self.connect_timeout_s, self.query_timeout_s = settings, connect_timeout_s, query_timeout_s

    def describe(self) -> str:
        """Backend and target without credentials."""
        if self.settings.db_backend == "postgres":
            return f"postgres:{redact_url(self.settings.postgres_url)}"
        return f"sqlite:{self.settings.resolve_path(self.settings.sqlite_path)}"

    def open(self, require_schema: bool = True, read_only: bool = False) -> SqlTelemetryStore:
        try:
            # FastAPI runs the sync dependency, the sync handler and the teardown in different worker
            # threads; the per-request connection is used by them one after another, never concurrently.
            store = open_store(self.settings, connect_timeout_s=self.connect_timeout_s,
                               sqlite_check_same_thread=False, read_only=read_only,
                               query_timeout_s=self.query_timeout_s)
        except Exception as exc:
            log.error("telemetry store unavailable", extra={"context": {
                "target": self.describe(), "error_type": type(exc).__name__}})
            raise DatabaseUnavailableError() from exc
        if require_schema:
            try:
                store.check_initialized()
            except StoreNotInitialized as exc:
                store.close()
                raise StoreNotInitializedError(
                    "Telemetry store schema is not initialized; run `uv run python db/init_telemetry_store.py`") from exc
            except Exception as exc:
                store.close()
                raise DatabaseUnavailableError() from exc
        return store


def get_store_factory(request: Request) -> StoreFactory:
    return request.app.state.store_factory


def get_db_session(request: Request) -> Iterator[SqlTelemetryStore]:
    """Per-request store connection (FastAPI dependency)."""
    store = get_store_factory(request).open()
    try:
        yield store
    finally:
        store.close()


def get_read_session(request: Request) -> Iterator[SqlTelemetryStore]:
    """Per-request READ-ONLY store connection (dashboard, telemetry, assets, copilot)."""
    store = get_store_factory(request).open(read_only=True)
    try:
        yield store
    finally:
        store.close()
