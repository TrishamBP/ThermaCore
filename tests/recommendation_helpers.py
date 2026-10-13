"""Fixtures for the recommendation workflow: scripted fake LLMs and engines over a test store."""

from datetime import datetime, timedelta, timezone

from data_engineering.recommendations.config import WorkflowConfig
from data_engineering.recommendations.graph import RecommendationEngine
from data_engineering.recommendations.llm import LLMCall, LLMError
from data_engineering.recommendations.schemas import AgentFinding, AnalysisPlan, Explanation
from data_engineering.storage.sqlite_store import SqliteTelemetryStore


class ScriptedLLM:
    """Returns scripted outputs per schema; a callable receives (system, user) and returns an object
    or raises. Records every call (schema name, user prompt)."""

    def __init__(self, model="fake/model", **script):
        self.model, self.script, self.calls = model, script, []

    def invoke(self, schema, system, user):
        self.calls.append((schema.__name__, user))
        step = self.script.get(schema.__name__)
        if isinstance(step, list):
            step = step.pop(0) if len(step) > 1 else step[0]
        if step is None:
            raise LLMError("parse", f"no script for {schema.__name__}")
        if isinstance(step, Exception):
            raise step
        out = step(system, user) if callable(step) else step
        return out, LLMCall(self.model, 1.0, 1, 10, 5, 15)


def plan(*agents, rationale="test plan"):
    return AnalysisPlan(agents=list(agents), rationale=rationale)


def finding(*types, summary="Evidence reviewed.", conclusion="SUPPORTED", **kw):
    return AgentFinding(summary=summary, supported_types=list(types), conclusion=conclusion, **kw)


def echo_explanation(system, user):
    """An explanation that only restates fields (so it passes the number check)."""
    import json
    items = json.loads(user.split("\n\nYour previous")[0])
    return Explanation(overall_summary="Recommendations for review. Nothing has been executed.", items=[
        {"recommendation_id": r["recommendation_id"], "what_to_evaluate": r["recommended_action"], "why": r["title"],
         "energy_impact": "Unavailable: no measured cooling power.", "uncertainty": r["uncertainty"]["level"],
         "why_human_review": "An operator must review; nothing was changed."} for r in items])


def engine(store_path, *, model=None, main=None, sub=None, cooling_config=None, model_settings=None,
           workflow=None, clock=None):
    from cooling_helpers import all_limits_config
    from cooling_model_helpers import settings
    from pathlib import Path

    def factory():
        return SqliteTelemetryStore(store_path, check_same_thread=False)

    return RecommendationEngine(
        store_factory=factory, cooling_config=cooling_config or all_limits_config(),
        model_provider=lambda: model, model_settings=model_settings or settings(Path(store_path).parent / "m"),
        workflow=workflow or WorkflowConfig(), main_llm=main, sub_llm=sub,
        clock=clock or (lambda: datetime.now(timezone.utc)))


def at(minute):
    from cooling_helpers import T0
    return T0 + timedelta(minutes=minute)
