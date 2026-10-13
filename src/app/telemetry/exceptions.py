"""Telemetry domain exceptions (rendered as ErrorResponse by app.exceptions)."""

from app.exceptions import InvalidRequestError, NotFoundError


class AssetNotFoundError(NotFoundError):
    code = "unknown_asset"


class RunNotFoundError(NotFoundError):
    code = "unknown_run"


class InvalidTelemetryQueryError(InvalidRequestError):
    code = "invalid_parameter"
