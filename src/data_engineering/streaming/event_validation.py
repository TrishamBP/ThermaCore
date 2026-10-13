"""Consumer-side parsing and validation of `datacenter.telemetry` messages.

The contract is the producer's own schema (telemetry_event.v1.schema.json, via
events.validate_event). On top of it, the consumer checks what a producer-side check
cannot: bytes that are not UTF-8 JSON, non-standard JSON (NaN, Infinity, numbers that
overflow to inf), calendar-invalid timestamps that match the regex (2026-02-30), and
an event_id that does not match <simulation_run_id>:<asset_id>:<slot_index>.

Every failure returns a Rejection with a stable `reason` code (used in DLQ records and
counters); nothing raises for bad input.
"""

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone

from data_engineering.streaming.events import validate_event

SUPPORTED_SCHEMA_VERSIONS = frozenset({"1.0"})


class Reason:
    MALFORMED_JSON = "malformed_json"
    NOT_OBJECT = "not_json_object"
    UNSUPPORTED_VERSION = "unsupported_schema_version"
    INVALID_TIMESTAMP = "invalid_timestamp"
    INVALID_METRIC = "invalid_metric_value"
    INVALID_IDENTIFIER = "invalid_identifier"
    SCHEMA_VIOLATION = "schema_violation"


@dataclass(frozen=True)
class Rejection:
    reason: str
    details: tuple[str, ...]


@dataclass(frozen=True)
class ValidEvent:
    event: dict
    event_timestamp: datetime      # timezone-aware UTC
    payload_sha256: str


def _reject_constant(name: str):
    raise ValueError(f"non-standard JSON constant {name}")


def payload_fingerprint(event: dict) -> str:
    """SHA-256 of the event content, ignoring how it was published (simulator vs stored run).

    Two deliveries with the same event_id and fingerprint are true duplicates; the same
    event_id with a different fingerprint is a conflicting duplicate.
    """
    content = json.loads(json.dumps(event))
    content.get("provenance", {}).pop("published_from", None)
    canonical = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_message(value: bytes | None) -> ValidEvent | Rejection:
    if not value:
        return Rejection(Reason.MALFORMED_JSON, ("empty message value",))
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        return Rejection(Reason.MALFORMED_JSON, (f"not valid UTF-8: {exc.reason} at byte {exc.start}",))
    try:
        event = json.loads(text, parse_constant=_reject_constant)
    except ValueError as exc:     # json.JSONDecodeError is a ValueError
        return Rejection(Reason.MALFORMED_JSON, (str(exc)[:200],))
    if not isinstance(event, dict):
        return Rejection(Reason.NOT_OBJECT, (f"top-level JSON value is {type(event).__name__}",))

    version = event.get("schema_version")
    if version is None:
        return Rejection(Reason.SCHEMA_VIOLATION, ("schema_version is required",))
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        return Rejection(Reason.UNSUPPORTED_VERSION,
                         (f"schema_version {version!r} not in {sorted(SUPPORTED_SCHEMA_VERSIONS)}",))

    metrics = event.get("metrics")
    if isinstance(metrics, dict):
        bad = [k for k, v in metrics.items() if isinstance(v, float) and not math.isfinite(v)]
        if bad:
            return Rejection(Reason.INVALID_METRIC, tuple(f"metrics/{k}: not a finite number" for k in bad))

    problems = validate_event(event)
    if problems:
        paths = [p.split(":", 1)[0] for p in problems]
        if any(not (p.startswith("metrics") or p.startswith("event_timestamp")) for p in paths):
            reason = Reason.SCHEMA_VIOLATION
        elif any(p.startswith("event_timestamp") for p in paths):
            reason = Reason.INVALID_TIMESTAMP
        else:
            reason = Reason.INVALID_METRIC
        return Rejection(reason, tuple(problems[:10]))

    raw_ts = event["event_timestamp"]
    try:
        ts = datetime.strptime(raw_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        return Rejection(Reason.INVALID_TIMESTAMP, (f"event_timestamp {raw_ts!r}: {exc}",))

    expected_id = f"{event['simulation_run_id']}:{event['asset_id']}:{event['slot_index']}"
    if event["event_id"] != expected_id:
        return Rejection(Reason.INVALID_IDENTIFIER,
                         (f"event_id {event['event_id']!r} does not match {expected_id!r}",))

    return ValidEvent(event, ts, payload_fingerprint(event))
