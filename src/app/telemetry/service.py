"""Telemetry service: runs the Cooling State Estimator and maps its errors to domain exceptions."""

from app.exceptions import DatabaseUnavailableError
from app.telemetry.exceptions import (
    AssetNotFoundError,
    InvalidTelemetryQueryError,
    RunNotFoundError,
)
from app.telemetry.schemas import CoolingStateQuery
from data_engineering.cooling.config import CoolingConfig, CoolingConfigError
from data_engineering.cooling.estimator import CoolingStateEstimator, UnknownScopeError
from data_engineering.cooling.models import CoolingStateSnapshot
from data_engineering.storage.base import SqlTelemetryStore


class TelemetryService:
    def __init__(self, store: SqlTelemetryStore, cooling_config: CoolingConfig) -> None:
        self.store, self.cooling_config = store, cooling_config

    def cooling_state(self, query: CoolingStateQuery) -> CoolingStateSnapshot:
        estimator = CoolingStateEstimator(self.store, self.cooling_config)
        try:
            return estimator.evaluate(query.asset_id, query.run_id, query.window_minutes, query.as_of)
        except UnknownScopeError as exc:
            raise (AssetNotFoundError if exc.kind == "asset" else RunNotFoundError)(str(exc)) from exc
        except (CoolingConfigError, ValueError) as exc:
            raise InvalidTelemetryQueryError(str(exc)) from exc
        except Exception as exc:          # driver errors while querying; details are logged, not returned
            raise DatabaseUnavailableError("Telemetry store query failed") from exc
