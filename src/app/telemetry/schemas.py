"""Telemetry API schemas.

The response model is the estimator's own CoolingStateSnapshot (data_engineering.cooling.models),
re-exported here so the HTTP contract and the computed snapshot cannot drift apart.
"""

from pydantic import Field

from app.common.schemas.base import CustomModel, UtcDatetime
from app.telemetry.constants import ID_PATTERN, WINDOW_MINUTES_MAX, WINDOW_MINUTES_MIN
from data_engineering.cooling.models import CoolingStateSnapshot

__all__ = ["CoolingStateQuery", "CoolingStateSnapshot"]


class CoolingStateQuery(CustomModel):
    """Query parameters of GET /telemetry/cooling-state. Unknown parameters are rejected."""
    asset_id: str | None = Field(None, pattern=ID_PATTERN, description="One asset; omit for the whole data center")
    run_id: str | None = Field(None, pattern=ID_PATTERN,
                               description="simulation_run_id; default: the run that most recently received readings")
    window_minutes: int | None = Field(None, ge=WINDOW_MINUTES_MIN, le=WINDOW_MINUTES_MAX,
                                       description="Rolling window; default from config/cooling_state.yaml")
    as_of: UtcDatetime | None = Field(None, description="Window end in event time, with timezone; default: newest reading")
