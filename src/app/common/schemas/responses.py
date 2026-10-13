"""Shared response shapes."""

from typing import Any

from app.common.schemas.base import CustomModel


class ErrorDetail(CustomModel):
    code: str                         # stable, machine-readable (e.g. "unknown_asset")
    message: str                      # human-readable; never contains credentials or raw driver errors
    details: list[dict[str, Any]] | None = None   # e.g. per-field validation problems


class ErrorResponse(CustomModel):
    """Body of every non-2xx response."""
    error: ErrorDetail
    request_id: str | None = None
