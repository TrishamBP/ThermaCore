import copy
import json
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import islice

import pytest

from data_engineering.simulation.generate_telemetry import METRICS
from data_engineering.streaming.events import (
    SCHEMA_PATH, serialize_event, to_json_value, validate_event)
from data_engineering.streaming.sources import build_simulator, simulated_events


@pytest.fixture
def small_config(sim_config):
    config = copy.deepcopy(sim_config)
    config["simulation"].update(asset_count=3, duration_minutes=30, missing_rate=0.1)
    return config


@pytest.fixture
def events(loaded_conn, small_config):
    return list(simulated_events(build_simulator(loaded_conn, small_config), "dc-test"))


def test_every_simulated_event_is_schema_valid(events):
    assert len(events) == 3 * 30
    assert all(validate_event(e) == [] for e in events)


def test_schema_metrics_match_simulated_metrics():
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert schema["properties"]["metrics"]["required"] == list(METRICS)
    assert list(schema["properties"]["metrics"]["properties"]) == list(METRICS)


def test_event_timestamp_is_simulated_slot_time_not_wall_clock(events, small_config):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert small_config["simulation"]["start_ts_utc"] == "2026-01-01T00:00:00Z"
    for e in events:
        expected = start + timedelta(seconds=60 * e["slot_index"])
        assert e["event_timestamp"] == expected.strftime("%Y-%m-%dT%H:%M:%SZ")
        assert e["timestamp_source"] == "simulated"


def test_event_ids_are_deterministic_and_unique(events, loaded_conn, small_config):
    again = list(simulated_events(build_simulator(loaded_conn, small_config), "dc-test"))
    assert [e["event_id"] for e in events] == [e["event_id"] for e in again]
    assert len({e["event_id"] for e in events}) == len(events)
    e = events[4]
    assert e["event_id"] == f"{e['simulation_run_id']}:{e['asset_id']}:{e['slot_index']}"


def test_missing_reading_is_explicit_null_not_zero(events):
    missing = [e for e in events if e["reading_status"] == "missing"]
    assert missing, "missing_rate=0.1 should produce missing slots"
    for e in missing:
        assert all(v is None for v in e["metrics"].values())
        assert e["labels"]["injected_anomalies"] == []
        assert '"cpu_utilization":null' in serialize_event(e).decode()


def test_observed_reading_with_null_metric_is_rejected(events):
    event = copy.deepcopy(next(e for e in events if e["reading_status"] == "observed"))
    event["metrics"]["pue"] = None
    assert any("pue" in p for p in validate_event(event))


def test_missing_reading_with_a_value_is_rejected(events):
    event = copy.deepcopy(next(e for e in events if e["reading_status"] == "missing"))
    event["metrics"]["pue"] = 1.1
    assert validate_event(event)


@pytest.mark.parametrize("mutate", [
    lambda e: e.update(event_timestamp="2026-01-01 00:00:00"),
    lambda e: e.update(source_type="real"),
    lambda e: e.update(schema_version="2.0"),
    lambda e: e["metrics"].update(fan_failure_flag=1.0),
    lambda e: e.pop("asset_id"),
    lambda e: e["provenance"]["field_sources"].update(metrics="source_dataset"),
])
def test_invalid_events_are_rejected(events, mutate):
    event = copy.deepcopy(events[0])
    mutate(event)
    assert validate_event(event)


def test_serialization_is_compact_utf8_json_and_round_trips(events):
    payload = serialize_event(events[0])
    assert isinstance(payload, bytes) and b": " not in payload
    assert json.loads(payload) == events[0]


def test_serialization_refuses_nan_instead_of_emitting_invalid_json(events):
    event = copy.deepcopy(events[0])
    event["metrics"]["pue"] = float("nan")
    with pytest.raises(ValueError):
        serialize_event(event)


def test_to_json_value_normalizes_python_values():
    aware = datetime(2026, 1, 1, 5, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    assert to_json_value(aware) == "2026-01-01T00:00:00Z"
    with pytest.raises(TypeError):
        to_json_value(datetime(2026, 1, 1))          # naive: timezone unknown
    assert to_json_value(Decimal("1.25")) == 1.25
    assert to_json_value(float("nan")) is None       # missing, not 0
    assert to_json_value(float("inf")) is None
    assert to_json_value({"a": (1, None)}) == {"a": [1, None]}
    with pytest.raises(TypeError):
        to_json_value(object())


def test_to_json_value_normalizes_numpy_scalars():
    np = pytest.importorskip("numpy")
    out = to_json_value({"f": np.float64(1.5), "i": np.int64(7), "b": np.bool_(True), "n": np.float32("nan")})
    assert out == {"f": 1.5, "i": 7, "b": True, "n": None}
    assert type(out["f"]) is float and type(out["i"]) is int and type(out["b"]) is bool
    json.dumps(out, allow_nan=False)


def test_to_json_value_normalizes_pandas_missing_values():
    # Duck-typed check that works without pandas installed ...
    NAType = type("NAType", (), {"__module__": "pandas._libs.missing"})
    assert to_json_value(NAType()) is None
    # ... and the real objects when pandas is available.
    pd = pytest.importorskip("pandas")
    assert to_json_value(pd.NA) is None
    assert to_json_value(pd.NaT) is None
    assert to_json_value(pd.Timestamp("2026-01-01T00:00:00Z")) == "2026-01-01T00:00:00Z"


def test_source_derived_fields_come_from_the_baseline_row(loaded_conn, small_config):
    simulator = build_simulator(loaded_conn, small_config)
    asset = simulator.assets[0]
    event = next(islice(simulated_events(simulator, "dc-test"), 1))
    assert event["asset_attributes"]["server_type"] == asset["server_type"]
    assert event["labels"]["source_failure_risk_level"] == asset["failure_risk_level"]
    assert event["provenance"]["baseline_source_row_number"] == asset["baseline_source_row_number"]
    assert event["region_code"] == "NA" and event["region"] == "North America"
    assert not math.isnan(event["metrics"]["cpu_utilization"])
