"""Base domain exceptions and the handlers that render every error as ErrorResponse.

Domains raise subclasses of AppError (each with a status code and a stable `code`); they never
build HTTP responses themselves. Validation errors, unknown routes and unexpected exceptions
are rendered in the same shape:

    {"error": {"code": "...", "message": "...", "details": [...] | null}, "request_id": "..."}
"""

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.common.logging import get_logger
from app.common.schemas.responses import ErrorDetail, ErrorResponse

log = get_logger("errors")


class AppError(Exception):
    status_code = 500
    code = "internal_error"
    message = "Internal server error"

    def __init__(self, message: str | None = None, *, details: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message or self.message)
        self.message = message or self.message
        self.details = details


class NotFoundError(AppError):
    status_code, code, message = 404, "not_found", "Resource not found"


class InvalidRequestError(AppError):
    status_code, code, message = 422, "invalid_parameter", "Invalid request parameter"


class ServiceUnavailableError(AppError):
    status_code, code, message = 503, "service_unavailable", "Service unavailable"


class DatabaseUnavailableError(ServiceUnavailableError):
    code, message = "database_unavailable", "Telemetry store is unavailable"


class StoreNotInitializedError(ServiceUnavailableError):
    code, message = "store_not_initialized", "Telemetry store schema is not initialized"


_STATUS_CODES = {400: "bad_request", 401: "unauthorized", 403: "forbidden", 404: "not_found",
                 405: "method_not_allowed", 406: "not_acceptable", 413: "payload_too_large",
                 415: "unsupported_media_type", 429: "too_many_requests"}


def _render(request: Request, status: int, code: str, message: str, details=None) -> JSONResponse:
    body = ErrorResponse(error=ErrorDetail(code=code, message=message, details=details),
                         request_id=getattr(request.state, "request_id", None))
    return JSONResponse(status_code=status, content=body.model_dump(mode="json"))


async def _app_error(request: Request, exc: AppError) -> JSONResponse:
    if exc.status_code >= 500:
        log.warning("request failed", extra={"context": {"code": exc.code, "path": request.url.path,
                                                          "request_id": getattr(request.state, "request_id", None)}})
    return _render(request, exc.status_code, exc.code, exc.message, exc.details)


async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    # The submitted input is deliberately not echoed back.
    details = [{"loc": list(e.get("loc", ())), "msg": e.get("msg", ""), "type": e.get("type", "")}
               for e in exc.errors()]
    return _render(request, 422, "validation_error", "Request validation failed", details)


async def _http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    code = _STATUS_CODES.get(exc.status_code, "http_error")
    message = exc.detail if isinstance(exc.detail, str) else code.replace("_", " ")
    return _render(request, exc.status_code, code, message)


async def _unexpected(request: Request, exc: Exception) -> JSONResponse:
    log.error("unhandled exception", exc_info=exc, extra={"context": {
        "path": request.url.path, "request_id": getattr(request.state, "request_id", None)}})
    return _render(request, 500, "internal_error", "Internal server error")


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(AppError, _app_error)
    app.add_exception_handler(RequestValidationError, _validation_error)
    app.add_exception_handler(StarletteHTTPException, _http_error)
    app.add_exception_handler(Exception, _unexpected)
