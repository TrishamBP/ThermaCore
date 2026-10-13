"""Copilot schemas: the interpretation of a question, evidence items and the LLM outputs."""

from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

Intent = Literal["status", "thermal", "workload", "history", "trend", "comparison", "forecast", "energy", "anomaly",
                 "maintenance_priority", "recommendations", "cooling_setting", "explanation"]
INTENTS: tuple[str, ...] = Intent.__args__
EvidenceKind = Literal["measured", "calculated", "prediction", "hypothesis", "recommendation", "configuration",
                       "unavailable"]


def _cap(n):
    return AfterValidator(lambda v: v if len(v) <= n else v[:n - 1] + "…")


def _first(n):
    return AfterValidator(lambda v: v[:n])


Text = Annotated[str, _cap(1500)]
Line = Annotated[str, _cap(400)]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EvidenceItem(_Strict):
    id: str
    kind: EvidenceKind
    asset_id: str | None = None
    metric: str
    value: float | str | None = None
    unit: str | None = None
    window: str | None = None
    source: str
    note: str | None = None


class Interpretation(_Strict):
    intents: list[str]
    asset_ids: list[str]
    all_assets: bool
    start_time: str | None
    end_time: str | None
    time_basis: str
    resolved_from_context: list[str] = Field(default_factory=list)
    source: Literal["deterministic", "llm+deterministic"]
    candidate_settings: list[float] = Field(default_factory=list)


# ------------------------------------------------------------------ LLM outputs

class QueryPlan(_Strict):
    """Main model: what the operator is asking. Validated against known assets and bounds before use."""
    intents: list[Intent] = Field(description="What the question asks for (one or more)")
    asset_ids: list[str] = Field(default_factory=list, description="Asset ids named or referenced (from the list)")
    all_assets: bool = Field(False, description="True when the question is about all/which/any assets")
    window_minutes: int | None = Field(None, description="Length of the requested time window in minutes, if any")
    uses_previous_context: bool = Field(False, description="True when the question refers to the previous turn")
    needs_clarification: bool = False
    clarification_question: Line | None = None


class CopilotAnswer(_Strict):
    """Main model: the answer, built only from the evidence items (cite their ids)."""
    answer: Text = Field(description="Concise answer citing evidence ids like [E3]")
    measured: Annotated[list[Line], _first(10)] = Field(default_factory=list)
    calculated: Annotated[list[Line], _first(10)] = Field(default_factory=list)
    predictions: Annotated[list[Line], _first(6)] = Field(default_factory=list)
    hypotheses: Annotated[list[Line], _first(6)] = Field(default_factory=list,
                                                       description="Possible causes; NOT established")
    recommendations: Annotated[list[Line], _first(6)] = Field(default_factory=list)
    limitations: Annotated[list[Line], _first(8)] = Field(default_factory=list)
