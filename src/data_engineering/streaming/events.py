"""Telemetry event construction, JSON normalization, validation and serialization.

The event format is defined by telemetry_event.v1.schema.json (next to this file).
"""

import json
import math
from datetime import date, datetime, timezone
from decimal import Decimal
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator

from data_engineering.paths import DATASET_SLUG
from data_engineering.simulation.generate_telemetry import METRICS

SCHEMA_VERSION = "1.0"
SCHEMA_PATH = Path(__file__).with_name("telemetry_event.v1.schema.json")
ASSET_ATTRIBUTE_FIELDS = ("server_type", "cooling_type", "server_age_years", "rack_u_position",
                          "energy_source_primary")
FIELD_SOURCES = {
    "metrics": "simulated",
    "event_timestamp": "simulated",
    "asset_id": "simulated",
    "data_center_id": "simulated",
    "region_code": "source_dataset",
    "region": "transformed",
    "asset_attributes": "source_dataset",
    "source_labels": "source_dataset",
    "injected_anomalies": "simulated",
}


def to_json_value(value):
    """Convert Python/NumPy/pandas scalars and containers into JSON-safe Python values.

    Missing values (None, NaN, pandas NA/NaT) become None; they are never turned into 0.
    Timezone-aware datetimes become UTC ISO-8601 strings with a trailing Z; naive datetimes
    are rejected because their timezone is unknown.
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    type_name = type(value).__name__
    module = type(value).__module__
    if type_name in {"NAType", "NaTType"} and module.startswith("pandas"):
        return None
    if module == "numpy" and hasattr(value, "item"):
        return to_json_value(value.item())
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, Decimal):
        return None if not value.is_finite() else float(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise TypeError(f"naive datetime {value!r} has no timezone")
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): to_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_value(v) for v in value]
    raise TypeError(f"cannot convert {type_name} to JSON")


def build_event(*, run_id: str, asset: dict, slot_index: int, ts_utc: str, is_missing: bool,
                values: dict | None, anomalies: list[dict], scenario: dict, data_center_id: str,
                run_info: dict, published_from: str) -> dict:
    """Assemble one event. `anomalies` items: {anomaly_id, anomaly_type, metrics}."""
    metrics = {m: None if is_missing or values is None else values.get(m) for m in METRICS}
    event = {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{run_id}:{asset['asset_id']}:{slot_index}",
        "event_timestamp": ts_utc,
        "timestamp_source": "simulated",
        "asset_id": asset["asset_id"],
        "data_center_id": data_center_id,
        "region": scenario["region_label"],
        "region_code": scenario["region_code"],
        "source_type": "simulated",
        "simulation_run_id": run_id,
        "slot_index": slot_index,
        "reading_status": "missing" if is_missing else "observed",
        "metrics": metrics,
        "asset_attributes": {f: asset.get(f) for f in ASSET_ATTRIBUTE_FIELDS},
        "labels": {
            "source_failure_risk_level": asset.get("failure_risk_level"),
            "source_energy_efficiency_class": asset.get("energy_efficiency_class"),
            "source_maintenance_status": asset.get("maintenance_status"),
            "injected_anomalies": [] if is_missing else [
                {"anomaly_id": a["anomaly_id"], "anomaly_type": a["anomaly_type"],
                 "affected_metrics": list(a["metrics"])} for a in anomalies],
        },
        "provenance": {
            "dataset": DATASET_SLUG,
            "source_file": run_info["source_file"],
            "source_sha256": run_info["source_sha256"],
            "baseline_source_row_number": asset["baseline_source_row_number"],
            "generator": run_info["generator_name"],
            "generator_version": run_info["generator_version"],
            "seed": run_info["seed"],
            "published_from": published_from,
            "field_sources": dict(FIELD_SOURCES),
        },
    }
    return to_json_value(event)


@lru_cache(maxsize=1)
def _validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def validate_event(event: dict) -> list[str]:
    """Return human-readable schema violations; an empty list means the event is valid."""
    errors = sorted(_validator().iter_errors(event), key=lambda e: list(e.absolute_path))
    return [f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}" for e in errors]


def serialize_event(event: dict) -> bytes:
    """Compact UTF-8 JSON. Raises ValueError for NaN/Infinity instead of emitting invalid JSON."""
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
