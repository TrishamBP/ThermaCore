"""Security-headers ASGI middleware (registered in app/middleware.py).

Pure ASGI (no BaseHTTPMiddleware), so it adds no per-request task overhead. The API serves
JSON only; the interactive docs (/docs, /redoc) need scripts and styles from a CDN, so the
strict Content-Security-Policy is applied to every path except those.
"""

from starlette.types import ASGIApp, Message, Receive, Scope, Send

BASE_HEADERS = {
    b"x-content-type-options": b"nosniff",
    b"x-frame-options": b"DENY",
    b"referrer-policy": b"no-referrer",
    b"cross-origin-resource-policy": b"same-origin",
}
API_ONLY_HEADERS = {
    b"content-security-policy": b"default-src 'none'; frame-ancestors 'none'",
    b"cache-control": b"no-store",
}
DOC_PATHS = ("/docs", "/redoc", "/openapi.json")


class SecurityHeadersMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        extra = dict(BASE_HEADERS)
        if not scope["path"].startswith(DOC_PATHS):
            extra.update(API_ONLY_HEADERS)

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                present = {name.lower() for name, _ in message.get("headers", [])}
                message["headers"] = list(message.get("headers", [])) + [
                    (name, value) for name, value in extra.items() if name not in present]
            await send(message)

        await self.app(scope, receive, send_with_headers)
