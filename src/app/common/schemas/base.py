"""Base model and shared annotated types."""

from datetime import datetime, timezone
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict


class CustomModel(BaseModel):
    """Base for API schemas: unknown fields are rejected, values are validated on assignment."""
    model_config = ConfigDict(extra="forbid", validate_assignment=True, populate_by_name=True)


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must include a timezone, e.g. 2026-01-01T14:00:00Z")
    return value.astimezone(timezone.utc)


# A timezone-aware datetime, normalized to UTC. Naive datetimes are rejected rather than
# silently interpreted as local time or UTC.
UtcDatetime = Annotated[datetime, AfterValidator(_require_utc)]
