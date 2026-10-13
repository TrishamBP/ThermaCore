"""Operator access control (opt-in; the API had no authentication before).

- API_AUTH_TOKEN: when set, every /api/v1 route except /health requires `Authorization: Bearer <token>`
  (constant-time comparison). Unset = local development without authentication.
- API_ALLOWED_ASSETS: the operator's asset scope. Empty = all assets. Requests for other assets get
  403 asset_not_authorized; list endpoints only return assets in scope. The copilot's tools use the same
  scope, so an LLM cannot read assets the operator may not see.
"""

import secrets
from typing import Annotated

from fastapi import Depends, Header, Request

from app.exceptions import AppError


class UnauthorizedError(AppError):
    status_code, code, message = 401, "unauthorized", "Missing or invalid bearer token"


class AssetNotAuthorizedError(AppError):
    status_code, code, message = 403, "asset_not_authorized", "Asset is outside the operator's scope"


def require_operator(request: Request, authorization: Annotated[str | None, Header()] = None) -> None:
    token = request.app.state.settings.auth_token
    if token is None:
        return
    scheme, _, value = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not secrets.compare_digest(value.encode(), token.get_secret_value().encode()):
        raise UnauthorizedError()


class AssetScope:
    def __init__(self, allowed: list[str]) -> None:
        self.allowed = frozenset(allowed)

    @property
    def unrestricted(self) -> bool:
        return not self.allowed

    def permits(self, asset_id: str) -> bool:
        return self.unrestricted or asset_id in self.allowed

    def check(self, asset_id: str | None) -> None:
        if asset_id is not None and not self.permits(asset_id):
            raise AssetNotAuthorizedError(f"asset {asset_id!r} is outside the operator's scope")

    def filter(self, asset_ids):
        return [a for a in asset_ids if self.permits(a)]


def get_asset_scope(request: Request) -> AssetScope:
    return AssetScope(request.app.state.settings.allowed_assets)


OperatorScope = Annotated[AssetScope, Depends(get_asset_scope)]
