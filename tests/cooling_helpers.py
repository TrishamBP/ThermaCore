"""Controlled telemetry fixtures for the cooling state estimator tests.

Rows are written through the real store (persist_batch), so tests exercise the real SQL.
Values are explicit and chosen per test; nothing relies on simulated correlations.
"""

import hashlib
import uuid
from datetime import datetime, timedelta, timezone

from data_engineering.cooling.config import CoolingConfig, Limits, load_cooling_config
from data_engineering.simulation.generate_telemetry import METRICS
from data_engineering.storage.base import BatchRecord, KafkaPosition, PreparedEvent

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
RUN = "sim-test-run"
STABLE = {
    "workload_intensity": 0.5, "cpu_utilization": 50.0, "gpu_utilization": 40.0, "memory_utilization": 60.0,
    "storage_utilization": 30.0, "disk_io_iops": 5000.0, "network_throughput_gbps": 10.0, "network_latency_ms": 40.0,
    "ambient_temperature_c": 18.0, "humidity_percentage": 45.0, "inlet_temperature_c": 21.0,
    "exhaust_temperature_c": 30.0, "fan_speed_rpm": 3000.0, "airflow_cfm": 180.0, "power_consumption_kw": 100.0,
    "pue": 1.1,
}
ALL_LIMITS = dict(inlet_temperature_high_c=27.0, inlet_temperature_critical_c=32.0, exhaust_temperature_high_c=45.0,
                  temperature_delta_high_c=20.0, sustained_inlet_rise_c_per_minute=0.1,
                  rapid_inlet_rise_c_per_minute=0.3, airflow_drop_fraction=0.1, fan_airflow_divergence_fraction=0.1,
                  inlet_approach_margin_c=2.0, inlet_approach_horizon_minutes=15,
                  light_load_cpu_utilization_pct=30, light_load_gpu_utilization_pct=30)


def config(**limits) -> CoolingConfig:
    """The shipped config/cooling_state.yaml with the given limits set."""
    base = load_cooling_config()
    return base.with_overrides(**limits) if limits else base


def all_limits_config(**overrides) -> CoolingConfig:
    return config(**{**ALL_LIMITS, **overrides})


def no_limits() -> Limits:
    return Limits()


def event(asset_id: str, minute: float, *, run: str = RUN, asset_type: str = "Compute", status: str = "observed",
          slot: int | None = None, **metrics) -> PreparedEvent:
    """One reading at T0 + minute. Unspecified metrics take STABLE values; status 'missing' -> all NULL."""
    ts = T0 + timedelta(minutes=minute)
    slot = int(minute * 60) if slot is None else slot
    values = {m: None for m in METRICS} if status == "missing" else {**STABLE, **metrics}
    event_id = f"{run}:{asset_id}:{slot}"
    reading = {
        "event_id": event_id, "simulation_run_id": run, "asset_id": asset_id, "data_center_id": "dc-test",
        "region": "North America", "region_code": "NA", "event_timestamp": ts,
        "event_timestamp_raw": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "slot_index": slot, "reading_status": status,
        "schema_version": "1.0", "source_type": "simulated", "timestamp_source": "simulated",
        "published_from": "simulator", **{m: values[m] for m in METRICS},
        "payload_sha256": hashlib.sha256(repr(sorted(values.items())).encode()).hexdigest(),
        "kafka_topic": "datacenter.telemetry", "kafka_partition": 0, "kafka_offset": slot,
    }
    asset = {
        "simulation_run_id": run, "asset_id": asset_id, "data_center_id": "dc-test", "region": "North America",
        "region_code": "NA", "asset_type": asset_type, "cooling_type": "Air", "server_age_years": 3,
        "rack_u_position": 10, "energy_source_primary": "Grid", "source_failure_risk_level": "Low",
        "source_energy_efficiency_class": "Medium", "source_maintenance_status": "Normal",
        "source_type": "simulated", "baseline_source_row_number": 1, "dataset": "test", "source_file": "test.csv",
        "source_sha256": "0" * 64, "generator": "test", "generator_version": "1", "seed": 1,
    }
    return PreparedEvent(event_id, reading["payload_sha256"], ts, reading, asset, [], KafkaPosition("t", 0, slot))


def series(asset_id: str, minutes, **kwargs_per_minute) -> list[PreparedEvent]:
    """Events for each minute; keyword values may be callables f(minute) -> value."""
    out = []
    for m in minutes:
        values = {k: (v(m) if callable(v) else v) for k, v in kwargs_per_minute.items()}
        out.append(event(asset_id, m, **values))
    return out


def seed(store, events: list[PreparedEvent]) -> None:
    store.persist_batch(BatchRecord(uuid.uuid4().hex, "test", "datacenter.telemetry", datetime.now(timezone.utc),
                                    len(events), 0, {0: [0, max(0, len(events) - 1)]}), events)


def wall_clock(offset_s: float = 0.0):
    """Estimator clock: real now (so ingest age ~0) plus an offset to simulate a stopped stream."""
    return lambda: datetime.now(timezone.utc) + timedelta(seconds=offset_s)
MINUTES_15 = range(0, 15)
