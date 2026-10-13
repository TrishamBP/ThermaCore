"""Opaque keyset cursors: base64url(JSON [created_at, id]) of the last row returned."""

import base64
import json
from datetime import datetime

from app.exceptions import InvalidRequestError


def encode_cursor(created_at: datetime, row_id: str) -> str:
    raw = json.dumps([created_at.isoformat(), row_id], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str | None) -> tuple[datetime, str] | None:
    if cursor is None:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        ts, row_id = json.loads(raw)
        value = datetime.fromisoformat(ts)
        if value.tzinfo is None or not isinstance(row_id, str):
            raise ValueError
        return value, row_id
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise InvalidRequestError("cursor is not valid", details=[{"loc": ["query", "cursor"],
                                                                   "msg": "invalid cursor"}]) from exc
