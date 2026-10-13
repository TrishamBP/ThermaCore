import copy
import json
import re
from datetime import timezone

import pytest

from telemetry_helpers import make_events
from data_engineering.streaming.event_validation import Reason, Rejection, ValidEvent, parse_message
from data_engineering.streaming.events import serialize_event


@pytest.fixture
def events(loaded_conn, sim_config):
    return make_events(loaded_conn, sim_config, assets=2, minutes=30, missing_rate=0.2)


@pytest.fixture
def observed(events):
    return next(e for e in events if e["reading_status"] == "observed")


def _raw(event) -> bytes:
    return json.dumps(event).encode()


def _reason(value) -> str:
    result = parse_message(value)
    assert isinstance(result, Rejection), result
    return result.reason


def test_valid_event_is_accepted_with_utc_timestamp(observed):
    result = parse_message(serialize_event(observed))
    assert isinstance(result, ValidEvent)
    assert result.event == observed
    assert result.event_timestamp.tzinfo is timezone.utc
    assert result.event_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") == observed["event_timestamp"]
    assert len(result.payload_sha256) == 64


def test_missing_measurements_are_valid_and_stay_null(events):
    missing = next(e for e in events if e["reading_status"] == "missing")
    result = parse_message(serialize_event(missing))
    assert isinstance(result, ValidEvent)
    assert all(v is None for v in result.event["metrics"].values())


@pytest.mark.parametrize("value, reason", [
    (None, Reason.MALFORMED_JSON),
    (b"", Reason.MALFORMED_JSON),
    (b"{not json", Reason.MALFORMED_JSON),
    (b"\xff\xfe\x00garbage", Reason.MALFORMED_JSON),
    (b'{"schema_version": "1.0", "metrics": {"pue": NaN}}', Reason.MALFORMED_JSON),
    (b"[1, 2, 3]", Reason.NOT_OBJECT),
    (b'"just a string"', Reason.NOT_OBJECT),
])
def test_malformed_payloads_are_rejected(value, reason):
    assert _reason(value) == reason


def test_number_overflowing_to_infinity_is_an_invalid_metric(observed):
    text, n = re.subn(r'"pue":[-0-9.eE+]+', '"pue":1e400', serialize_event(observed).decode())
    assert n == 1
    assert _reason(text.encode()) == Reason.INVALID_METRIC


def test_unsupported_or_missing_schema_version(observed):
    e = copy.deepcopy(observed)
    e["schema_version"] = "2.0"
    assert _reason(_raw(e)) == Reason.UNSUPPORTED_VERSION
    del e["schema_version"]
    assert _reason(_raw(e)) == Reason.SCHEMA_VIOLATION


@pytest.mark.parametrize("field", ["event_id", "asset_id", "simulation_run_id", "event_timestamp", "metrics"])
def test_missing_required_fields_are_rejected(observed, field):
    e = copy.deepcopy(observed)
    del e[field]
    assert _reason(_raw(e)) == Reason.SCHEMA_VIOLATION


def test_empty_identifier_is_rejected(observed):
    e = copy.deepcopy(observed)
    e["asset_id"] = ""
    assert _reason(_raw(e)) == Reason.SCHEMA_VIOLATION


def test_event_id_must_match_its_components(observed):
    e = copy.deepcopy(observed)
    e["event_id"] = e["event_id"].replace(":0", ":999") if e["event_id"].endswith(":0") else e["event_id"] + "9"
    assert _reason(_raw(e)) == Reason.INVALID_IDENTIFIER


@pytest.mark.parametrize("ts", ["2026-01-01 00:00:00", "2026-01-01T00:00:00", "2026-01-01T00:00:00+00:00",
                                "yesterday", "2026-02-30T00:00:00Z", "2026-13-01T00:00:00Z"])
def test_invalid_timestamps_are_rejected(observed, ts):
    e = copy.deepcopy(observed)
    e["event_timestamp"] = ts
    assert _reason(_raw(e)) == Reason.INVALID_TIMESTAMP


@pytest.mark.parametrize("bad", ["12.5", True, [1.0], {"v": 1}])
def test_invalid_metric_types_are_rejected(observed, bad):
    e = copy.deepcopy(observed)
    e["metrics"]["cpu_utilization"] = bad
    assert _reason(_raw(e)) == Reason.INVALID_METRIC


def test_null_metric_on_an_observed_reading_is_rejected(observed):
    e = copy.deepcopy(observed)
    e["metrics"]["power_consumption_kw"] = None
    assert _reason(_raw(e)) == Reason.INVALID_METRIC


def test_unknown_metric_is_rejected(observed):
    e = copy.deepcopy(observed)
    e["metrics"]["fan_failure_flag"] = 1.0
    assert _reason(_raw(e)) == Reason.INVALID_METRIC


def test_duplicate_event_ids_validate_identically(observed):
    """Validation is per message: a redelivered duplicate is valid; the store deduplicates it."""
    a, b = parse_message(serialize_event(observed)), parse_message(serialize_event(observed))
    assert a.payload_sha256 == b.payload_sha256


def test_fingerprint_ignores_publish_path_but_not_content(observed):
    base = parse_message(serialize_event(observed)).payload_sha256
    replayed = copy.deepcopy(observed)
    replayed["provenance"]["published_from"] = "stored_run"
    assert parse_message(serialize_event(replayed)).payload_sha256 == base
    changed = copy.deepcopy(observed)
    changed["metrics"]["cpu_utilization"] += 1
    assert parse_message(serialize_event(changed)).payload_sha256 != base
