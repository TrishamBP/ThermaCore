"""Recommendation API schemas. Recommendations themselves use the workflow's own Recommendation model."""

from typing import Any, Literal

from pydantic import Field

from app.common.schemas.base import CustomModel, UtcDatetime
from app.telemetry.constants import ID_PATTERN
from data_engineering.recommendations.schemas import RecommendationType

REC_ID_PATTERN = r"^(rec|ana)-[0-9a-f]{20}$"
ALERT_ID_PATTERN = r"^alr-[0-9a-f]{20}$"


class AnalyzeRequest(CustomModel):
    asset_id: str = Field(..., pattern=ID_PATTERN)
    run_id: str | None = Field(None, pattern=ID_PATTERN,
                               description="simulation_run_id; default: the run that most recently received readings")
    as_of: UtcDatetime | None = Field(None, description="Window end in event time; omit for a live analysis")


class ListQuery(CustomModel):
    asset_id: str | None = Field(None, pattern=ID_PATTERN)
    run_id: str | None = Field(None, pattern=ID_PATTERN)
    type: RecommendationType | None = None
    status: Literal["REQUIRES_REVIEW", "INCONCLUSIVE"] | None = None
    severity: Literal["INFO", "WARNING", "CRITICAL"] | None = None
    start_time: UtcDatetime | None = Field(None, description="observation window end >= start_time")
    end_time: UtcDatetime | None = Field(None, description="observation window end <= end_time")
    limit: int = Field(50, ge=1, le=200)
    cursor: str | None = Field(None, description="next_cursor of the previous page")


class AlertQuery(CustomModel):
    asset_id: str | None = Field(None, pattern=ID_PATTERN)
    run_id: str | None = Field(None, pattern=ID_PATTERN)
    rule_id: str | None = Field(None, pattern=r"^[A-Z_]{3,64}$")
    severity: Literal["WARNING", "CRITICAL"] | None = None
    start_time: UtcDatetime | None = None
    end_time: UtcDatetime | None = None
    limit: int = Field(50, ge=1, le=200)
    cursor: str | None = None


class AlertList(CustomModel):
    items: list[dict[str, Any]]
    count: int
    next_cursor: str | None


class AnalysisResponse(CustomModel):
    analysis_id: str
    status: Literal["COMPLETED", "COMPLETED_WITH_FALLBACK", "FAILED"]
    asset_id: str
    simulation_run_id: str
    observation_window: dict[str, str | None]
    recommendations: list[dict[str, Any]]
    duplicates_of_existing: list[str]
    execution: dict[str, Any]
    human_approval_required: Literal[True]
    automatic_control_executed: Literal[False]


class RecommendationSummary(CustomModel):
    recommendation_id: str
    analysis_id: str
    asset_id: str
    simulation_run_id: str
    type: str
    status: str
    severity: str
    title: str
    observation_window_end: str
    created_at: str
    review_status: str


class RecommendationList(CustomModel):
    items: list[RecommendationSummary]
    count: int
    next_cursor: str | None


class Decision(CustomModel):
    decision_id: str
    decision: str
    reviewer: str
    note: str | None
    decided_at: str


class RecommendationDetail(CustomModel):
    recommendation: dict[str, Any]
    review_status: str
    decisions: list[Decision]
    created_at: str


class DecisionRequest(CustomModel):
    decision: Literal["APPROVED_FOR_EVALUATION", "REJECTED", "DEFERRED"] = Field(
        ..., description="APPROVED_FOR_EVALUATION approves a supervised real-world evaluation; nothing is executed")
    reviewer: str = Field(..., min_length=1, max_length=100, pattern=r"^[\w .@-]+$")
    note: str | None = Field(None, max_length=2000)


class DecisionResponse(Decision):
    recommendation_id: str
    automatic_control_executed: Literal[False] = False
    message: str = "Decision recorded. No equipment command was sent; any real-world change is a manual, " \
                   "supervised operator action."


class AgentStatus(CustomModel):
    mode: Literal["llm", "deterministic"]
    llm_enabled: bool
    api_key_configured: bool
    main_model: str
    subagent_model: str
    reasoning_effort: str
    timeout_s: float
    cooling_model: dict[str, Any] | None
    recommendation_only: Literal[True] = True
