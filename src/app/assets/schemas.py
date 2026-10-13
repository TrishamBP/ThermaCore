"""Asset and telemetry API schemas."""

from typing import Any, Literal

from pydantic import Field, field_validator

from app.common.schemas.base import CustomModel, UtcDatetime
from app.telemetry.constants import ID_PATTERN
from data_engineering.telemetry_query import INTERVALS, SUPPORTED_METRICS

DEFAULT_METRICS = ("inlet_temperature_c", "exhaust_temperature_c", "workload_intensity", "cpu_utilization",
                   "gpu_utilization", "fan_speed_rpm", "airflow_cfm", "power_consumption_kw", "pue")
Interval = Literal[tuple(INTERVALS)]


class TelemetryQuery(CustomModel):
    asset_id: str | None = Field(None, pattern=ID_PATTERN, description="One asset; omit for every asset in scope")
    run_id: str | None = Field(None, pattern=ID_PATTERN)
    start_time: UtcDatetime | None = Field(None, description="Exclusive; default end_time - 60 minutes")
    end_time: UtcDatetime | None = Field(None, description="Inclusive; default: newest reading of the run")
    interval: Interval = Field("raw", description="raw | 1m | 5m | 15m | 1h (epoch-aligned UTC buckets)")
    metrics: str | None = Field(None, description="Comma-separated metric names; default: the core cooling metrics")
    limit: int = Field(1000, ge=1, le=50000, description="Max points per asset (capped by API_MAX_TELEMETRY_POINTS)")
    cursor: str | None = Field(None, description="next_cursor of the previous page (single asset only)")

    @field_validator("metrics")
    @classmethod
    def _metrics(cls, v):
        if v is None:
            return v
        names = [m.strip() for m in v.split(",") if m.strip()]
        unknown = [m for m in names if m not in SUPPORTED_METRICS]
        if not names or unknown:
            raise ValueError(f"unknown metrics {unknown}; supported: {', '.join(SUPPORTED_METRICS)}")
        if len(set(names)) != len(names):
            raise ValueError("duplicate metrics")
        return ",".join(names)

    def metric_list(self) -> list[str]:
        return self.metrics.split(",") if self.metrics else list(DEFAULT_METRICS)


class AssetTelemetryQuery(TelemetryQuery):
    asset_id: None = Field(None, exclude=True)   # the asset comes from the path


class AssetListQuery(CustomModel):
    run_id: str | None = Field(None, pattern=ID_PATTERN)
    limit: int = Field(200, ge=1, le=1000)


class MetricInfo(CustomModel):
    name: str
    unit: str
    derived_from: str | None = None
    note: str | None = None


class AssetSeries(CustomModel):
    asset_id: str
    asset_type: str | None
    interval: str
    points: list[dict[str, Any]]
    truncated: bool
    readings: int
    duplicates_collapsed: int
    missing_readings: int
    gaps_over_tolerance: int
    largest_gap_seconds: float | None


class TelemetryResponse(CustomModel):
    simulation_run_id: str
    start_time: str
    end_time: str
    interval: str
    metrics: list[MetricInfo]
    aggregation: str
    series: list[AssetSeries]
    next_cursor: str | None
    unavailable_measurements: dict[str, str]
    warnings: list[str]


class AssetSummary(CustomModel):
    asset_id: str
    asset_type: str | None
    cooling_type: str | None
    data_center_id: str | None
    region_code: str | None
    first_event_timestamp: str | None
    last_event_timestamp: str | None
    latest_observed_timestamp: str | None
    fresh: bool | None
    source_type: str | None


class AssetList(CustomModel):
    simulation_run_id: str
    items: list[AssetSummary]
    count: int
    freshness_rule: str


class AssetDetail(CustomModel):
    simulation_run_id: str
    asset: dict[str, Any]
    latest_reading: dict[str, Any] | None
    cooling_state: dict[str, Any] | None
    unavailable_measurements: dict[str, str]
    warnings: list[str]
