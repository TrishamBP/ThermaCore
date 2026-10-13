"""Middleware registration (order matters: the last added runs first).

- RequestContextMiddleware: accepts a well-formed X-Request-ID or generates one, exposes it as
  request.state.request_id, echoes it in the response, and writes one structured access-log line
  per request (method, path without query string, status, duration).
- SecurityHeadersMiddleware (app.common.security.headers).
- CORSMiddleware only when API_CORS_ORIGINS is set.
"""

import re
import time
import uuid

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.common.logging import get_logger
from app.common.security.headers import SecurityHeadersMiddleware
from app.config import AppSettings
from app.constants import REQUEST_ID_HEADER, REQUEST_ID_PATTERN

log = get_logger("access")
_REQUEST_ID_RE = re.compile(REQUEST_ID_PATTERN)
_HEADER = REQUEST_ID_HEADER.lower().encode()


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        incoming = dict(scope.get("headers", [])).get(_HEADER, b"").decode("latin-1")
        request_id = incoming if _REQUEST_ID_RE.match(incoming) else uuid.uuid4().hex
        scope.setdefault("state", {})["request_id"] = request_id
        started = time.perf_counter()
        status = {"code": 500}

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
                message["headers"] = list(message.get("headers", [])) + [(_HEADER, request_id.encode())]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            log.info("request", extra={"context": {
                "method": scope["method"], "path": scope["path"], "status": status["code"],
                "ms": round((time.perf_counter() - started) * 1000, 1), "request_id": request_id}})


def register_middleware(app: FastAPI, settings: AppSettings) -> None:
    if settings.cors_origins:
        app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origins, allow_methods=["GET"],
                           allow_headers=[REQUEST_ID_HEADER], expose_headers=[REQUEST_ID_HEADER])
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(RequestContextMiddleware)      # outermost: every response gets an ID
