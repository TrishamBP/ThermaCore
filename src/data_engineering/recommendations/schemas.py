"""Typed schemas: evidence, recommendations, and the structured outputs exchanged with the LLMs.

LLM output schemas deliberately contain no numeric fields and no settings: an LLM can select,
interpret and explain deterministic evidence, but cannot introduce a forecast, an energy figure,
a limit or a setpoint. Numbers in its free text are checked against the evidence (validator.py).
"""

from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

RecommendationType = Literal["COOLING_EFFICIENCY", "PREEMPTIVE_COOLING", "AIRFLOW_INVESTIGATION", "ENERGY_WASTE",
                             "MAINTENANCE_INSPECTION", "SENSOR_DATA_QUALITY"]
RECOMMENDATION_TYPES: tuple[str, ...] = RecommendationType.__args__
AgentName = Literal["thermal", "energy", "anomaly"]
AGENTS: tuple[str, ...] = AgentName.__args__
# Which agent owns (must support) each recommendation type.
TYPE_OWNER: dict[str, str] = {"COOLING_EFFICIENCY": "thermal", "PREEMPTIVE_COOLING": "thermal",
                              "ENERGY_WASTE": "energy", "AIRFLOW_INVESTIGATION": "anomaly",
                              "MAINTENANCE_INSPECTION": "anomaly", "SENSOR_DATA_QUALITY": "anomaly"}
EvidenceKind = Literal["measured", "derived", "model_prediction", "configuration", "rule_finding", "unavailable"]
ALLOWED_UNITS = frozenset({"°C", "°C/min", "rpm", "CFM", "kW", "ratio", "fraction", "percent", "count", "minutes", "seconds",
                           "status", "none"})


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _cap(n: int):
    # LLM prose: over-long text is truncated after parsing instead of rejecting the whole answer.
    return AfterValidator(lambda v: v if len(v) <= n else v[:n - 1] + "…")


def _first(n: int):
    return AfterValidator(lambda v: v[:n])


Short = Annotated[str, _cap(400)]
Medium = Annotated[str, _cap(1200)]


class Evidence(_Strict):
    evidence_id: str
    agent: Literal["thermal", "energy", "anomaly", "workload"]
    kind: EvidenceKind
    metric: str
    observed_value: float | None
    unit: str
    observation_window: str
    interpretation: str
    source: str
    details: dict = Field(default_factory=dict)


class Uncertainty(_Strict):
    level: Literal["LOW", "MEDIUM", "HIGH", "UNKNOWN"]
    basis: str
    limitations: list[str] = Field(default_factory=list)


class Recommendation(_Strict):
    recommendation_id: str
    analysis_id: str
    asset_id: str
    simulation_run_id: str
    type: RecommendationType
    status: Literal["REQUIRES_REVIEW", "INCONCLUSIVE"]
    severity: Literal["INFO", "WARNING", "CRITICAL"]
    title: str
    recommended_action: str
    observation_window_start: str | None
    observation_window_end: str
    evidence: list[Evidence]
    current_setting: float | None = None
    candidate_setting: float | None = None
    setting_name: str | None = None
    predicted_temperature: float | None = None
    temperature_horizon_minutes: int | None = None
    estimated_energy_impact: float | None = None
    energy_unit: str | None = None
    energy_impact_status: str = "UNAVAILABLE"
    relative_energy_indicator: dict | None = None    # fan peer comparison (relative only; never kW)
    uncertainty: Uncertainty
    conditions_that_invalidate_recommendation: list[str]
    constraints_checked: list[str]
    validation_warnings: list[str]
    missing_evidence: list[str]
    hypotheses: list[str] = Field(default_factory=list)     # suspected causes: NOT established facts
    agent_assessment: str | None = None                     # the owning agent's (validated) summary
    explanation: dict | None = None
    explanation_source: Literal["llm", "deterministic_template", "none"] = "none"
    human_approval_required: Literal[True] = True
    automatic_control_executed: Literal[False] = False


class NotApplicable(_Strict):
    type: RecommendationType
    reason: str


# ------------------------------------------------------------------ LLM structured outputs

class AnalysisPlan(_Strict):
    """Orchestrator (main model): which specialist analyses to run."""
    agents: list[AgentName] = Field(description="Specialist analyses to run for this snapshot")
    rationale: Medium = Field(description="One or two sentences: why these analyses")


class EvidenceInterpretation(_Strict):
    evidence_id: str = Field(description="An evidence_id from the provided evidence list")
    interpretation: Short


class AgentFinding(_Strict):
    """Specialist (subagent model): interpretation of deterministic evidence."""
    summary: Medium = Field(description="What the evidence shows; cite evidence_ids")
    interpretations: Annotated[list[EvidenceInterpretation], _first(25)] = Field(default_factory=list)
    supported_types: list[RecommendationType] = Field(
        default_factory=list, description="Eligible recommendation types that the evidence supports")
    hypotheses: Annotated[list[Short], _first(6)] = Field(default_factory=list,
                                  description="Possible causes, explicitly NOT established by the evidence")
    missing_evidence: Annotated[list[Short], _first(10)] = Field(default_factory=list)
    conclusion: Literal["SUPPORTED", "NOT_SUPPORTED", "INCONCLUSIVE"]


class RecommendationExplanation(_Strict):
    recommendation_id: str
    what_to_evaluate: Medium
    why: Medium
    measured_observations: Annotated[list[Short], _first(10)] = Field(default_factory=list)
    model_predictions: Annotated[list[Short], _first(6)] = Field(default_factory=list)
    energy_impact: Short
    uncertainty: Medium
    invalidation_conditions: Annotated[list[Short], _first(8)] = Field(default_factory=list)
    missing_evidence: Annotated[list[Short], _first(8)] = Field(default_factory=list)
    why_human_review: Short


class Explanation(_Strict):
    """Orchestrator (main model): explanation of validated recommendations only."""
    overall_summary: Medium
    items: list[RecommendationExplanation]
