"""Pydantic models for the cooling state snapshot (schema_version 1.0).

Everything is JSON-serializable (`snapshot.model_dump(mode="json")`). Missing measurements
are null with `available`/`applicable` flags and a reason; they are never 0.
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

State = Literal["NORMAL", "LIGHT_LOAD", "PARTIALLY_ASSESSED", "DATA_QUALITY_ISSUE", "POTENTIAL_AIRFLOW_ISSUE",
                "APPROACHING_THERMAL_LIMIT", "RAPID_TEMPERATURE_RISE", "HIGH_THERMAL_LOAD", "INSUFFICIENT_DATA"]
Severity = Literal["INFO", "WARNING", "CRITICAL"]
Direction = Literal["increasing", "decreasing", "stable"]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MetricSeries(_Model):
    """One metric of one asset over the window."""
    unit: str
    source: Literal["telemetry", "derived"] = "telemetry"
    available: bool = True            # False: the telemetry schema has no such measurement
    applicable: bool = True           # False: not meaningful for this asset (e.g. GPU on a CPU-only server)
    reason: str | None = None
    observed_points: int = 0
    null_points: int = 0
    sufficient_points: bool = False
    latest: float | None = None
    latest_timestamp: datetime | None = None
    latest_age_seconds: float | None = None
    mean: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    std_dev: float | None = None
    first: float | None = None                # first observed value in the window
    change: float | None = None               # latest - first
    change_elapsed_minutes: float | None = None
    trend_per_minute: float | None = None
    recent_rate_per_minute: float | None = None
    direction: Direction | None = None


class AggregateMetric(_Model):
    """One metric across assets (data-center scope): statistics of each asset's own values."""
    unit: str
    source: Literal["telemetry", "derived"] = "telemetry"
    available: bool = True
    reason: str | None = None
    assets_reporting: int = 0
    mean_of_latest: float | None = None
    min_of_latest: float | None = None
    max_of_latest: float | None = None
    max_asset_id: str | None = None
    sum_of_latest: float | None = None          # only for additive quantities (power, kW)
    mean_trend_per_minute: float | None = None
    max_trend_per_minute: float | None = None
    max_trend_asset_id: str | None = None
    directions: dict[str, int] = Field(default_factory=dict)


class Finding(_Model):
    rule_id: str
    state: State
    severity: Severity
    kind: Literal["observation", "inference", "data_quality"]
    asset_id: str | None = None
    explanation: str
    evidence: dict[str, Any]


class RuleNotEvaluated(_Model):
    rule_id: str
    reason: str
    asset_id: str | None = None


class GapInfo(_Model):
    start: datetime
    end: datetime
    seconds: float
    position: Literal["leading", "interior", "trailing"]


class DataQuality(_Model):
    status: Literal["OK", "DEGRADED", "INSUFFICIENT"]
    stale: bool
    ingest_age_seconds: float | None = None
    expected_points: int = 0
    present_points: int = 0
    observed_points: int = 0
    missing_readings: int = 0
    duplicate_timestamps: int = 0
    conflicting_duplicates: int = 0
    irregular_intervals: int = 0
    median_interval_seconds: float | None = None
    coverage: float | None = None
    gaps: list[GapInfo] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)       # in the schema but no usable values
    unavailable_fields: list[str] = Field(default_factory=list)   # not in the telemetry schema at all
    warnings: list[str] = Field(default_factory=list)


class Scope(_Model):
    level: Literal["asset", "data_center"]
    asset_id: str | None = None
    asset_type: str | None = None
    simulation_run_id: str | None = None
    data_center_id: str | None = None


class AssetSummary(_Model):
    asset_id: str
    asset_type: str | None
    state: State
    severity: Severity
    rule_ids: list[str]
    data_quality_status: Literal["OK", "DEGRADED", "INSUFFICIENT"]
    latest_timestamp: datetime | None


class CoolingStateSnapshot(_Model):
    schema_version: Literal["1.0"] = "1.0"
    evaluated_at: datetime                   # wall clock when the snapshot was computed
    as_of: datetime | None                   # event time the window ends at (newest telemetry)
    window_minutes: int
    window_start: datetime | None
    scope: Scope
    state: State
    severity: Severity
    workload_direction: Literal["increasing", "decreasing", "stable", "mixed", "unknown"]
    metrics: dict[str, MetricSeries] | dict[str, AggregateMetric]
    findings: list[Finding]
    rules_not_evaluated: list[RuleNotEvaluated]
    data_quality: DataQuality
    assets: list[AssetSummary] = Field(default_factory=list)   # data-center scope only
    light_load_assets: list[str] = Field(default_factory=list)          # assets with a WL_LIGHT_LOAD finding
    approaching_limit_assets: list[str] = Field(default_factory=list)   # assets with TH_APPROACHING_INLET_LIMIT
    source_type: str | None = None
    notes: list[str] = Field(default_factory=list)
