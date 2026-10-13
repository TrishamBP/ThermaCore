"""LangGraph workflow for cooling recommendations (recommendation-only).

    START -> load_telemetry -> validate_telemetry -> assess_cooling_state
          -> plan_analysis (main LLM)            [skipped when telemetry is insufficient]
          -> thermal_agent | energy_agent | anomaly_agent   (subagent LLM, in parallel, as planned)
          -> validate_evidence  --(bounded re-analysis)--> agent(s)
          -> evaluate_candidates (deterministic rules + validator) -> rank_recommendations
          -> generate_explanation (main LLM, checked; template fallback) -> persist_recommendation -> END

The graph has no tool that can run code or reach equipment: agent nodes receive precomputed
evidence (all database reads happen in load_telemetry) and return structured findings. A failed
LLM call never yields a successful recommendation: the affected types become INCONCLUSIVE.
"""

import hashlib
import json
import operator
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph

from data_engineering.logging_utils import get_logger
from data_engineering.recommendations import evidence as ev
from data_engineering.recommendations import rules, validator
from data_engineering.recommendations.llm import LLMError, StructuredLLM
from data_engineering.recommendations.specialists import (  # noqa: F401  (re-exported)
    AGENT_ROLES,
    SUBAGENT_RULES,
    TYPE_REQUIREMENTS,
    run_specialist,
)
from data_engineering.recommendations.schemas import (
    AGENTS,
    TYPE_OWNER,
    AnalysisPlan,
    Explanation,
    Recommendation,
)

log = get_logger("recommendations")

def _merge(a: dict, b: dict) -> dict:
    return {**(a or {}), **(b or {})}


class WorkflowState(TypedDict, total=False):
    analysis_id: str
    request: dict
    context: Any
    telemetry_status: str
    evidence: dict
    rule_candidates: list
    not_applicable: list
    required_agents: list
    plan: dict
    agent_results: Annotated[dict, _merge]
    attempts: Annotated[dict, _merge]
    feedback: dict
    rerun: list
    recommendations: list
    explanation: dict
    persisted: dict
    trace: Annotated[list, operator.add]


def _evidence_json(items) -> str:
    return json.dumps([{"evidence_id": e.evidence_id, "kind": e.kind, "metric": e.metric,
                        "observed_value": e.observed_value, "unit": e.unit, "interpretation": e.interpretation}
                       for e in items], ensure_ascii=False)


def _trace(node, status, started, **extra) -> list[dict]:
    return [{"node": node, "status": status, "latency_ms": round((time.monotonic() - started) * 1000, 1), **extra}]


def recommendation_id(run_id: str, asset_id: str, rec_type: str, window_end: str) -> str:
    return "rec-" + hashlib.sha256(f"{run_id}|{asset_id}|{rec_type}|{window_end}".encode()).hexdigest()[:20]


class RecommendationEngine:
    def __init__(self, *, store_factory: Callable[[], Any], cooling_config, model_provider: Callable[[], Any],
                 model_settings, workflow, main_llm: StructuredLLM | None, sub_llm: StructuredLLM | None,
                 clock: Callable[[], datetime] | None = None) -> None:
        self.store_factory, self.cooling_config, self.model_provider = store_factory, cooling_config, model_provider
        self.model_settings, self.workflow, self.main_llm, self.sub_llm = model_settings, workflow, main_llm, sub_llm
        self.clock = clock
        self.graph = self._build()

    @property
    def mode(self) -> str:
        return "llm" if self.main_llm and self.sub_llm else "deterministic"

    # ------------------------------------------------------------------ graph
    def _build(self):
        g = StateGraph(WorkflowState)
        for name, fn in (("load_telemetry", self.load_telemetry), ("validate_telemetry", self.validate_telemetry),
                         ("assess_cooling_state", self.assess_cooling_state), ("plan_analysis", self.plan_analysis),
                         ("thermal_agent", self._agent_node("thermal")), ("energy_agent", self._agent_node("energy")),
                         ("anomaly_agent", self._agent_node("anomaly")), ("validate_evidence", self.validate_evidence),
                         ("evaluate_candidates", self.evaluate_candidates),
                         ("rank_recommendations", self.rank_recommendations),
                         ("generate_explanation", self.generate_explanation),
                         ("persist_recommendation", self.persist_recommendation)):
            g.add_node(name, fn)
        g.add_edge(START, "load_telemetry")
        g.add_edge("load_telemetry", "validate_telemetry")
        g.add_edge("validate_telemetry", "assess_cooling_state")
        g.add_conditional_edges("assess_cooling_state", self._route_after_assess,
                                ["plan_analysis", "anomaly_agent"])
        g.add_conditional_edges("plan_analysis", self._route_agents, [f"{a}_agent" for a in AGENTS])
        for a in AGENTS:
            g.add_edge(f"{a}_agent", "validate_evidence")
        g.add_conditional_edges("validate_evidence", self._route_after_validation,
                                [*(f"{a}_agent" for a in AGENTS), "evaluate_candidates"])
        g.add_edge("evaluate_candidates", "rank_recommendations")
        g.add_edge("rank_recommendations", "generate_explanation")
        g.add_edge("generate_explanation", "persist_recommendation")
        g.add_edge("persist_recommendation", END)
        return g.compile()

    def analyze(self, asset_id: str, run_id: str | None = None, as_of: datetime | None = None) -> dict:
        analysis_id = f"ana-{uuid.uuid4().hex[:20]}"
        started = time.monotonic()
        state = self.graph.invoke({"analysis_id": analysis_id,
                                   "request": {"asset_id": asset_id, "run_id": run_id, "as_of": as_of},
                                   "agent_results": {}, "attempts": {}, "feedback": {}, "trace": []},
                                  {"recursion_limit": 40})
        result = state["persisted"]
        log.info("recommendation analysis finished", extra={"context": {
            "analysis_id": analysis_id, "asset_id": asset_id, "status": result["status"], "mode": self.mode,
            "recommendations": len(result["recommendations"]), "ms": round((time.monotonic() - started) * 1000, 1),
            "tokens": result["execution"]["tokens"]}})
        return result

    # ------------------------------------------------------------------ nodes
    def load_telemetry(self, state):
        t, req = time.monotonic(), state["request"]
        store = self.store_factory()
        try:
            run_id = req["run_id"] or store.latest_run_id()
            if run_id is None:
                from data_engineering.cooling.estimator import UnknownScopeError
                raise UnknownScopeError("run", "the telemetry store has no simulation runs")
            ctx = ev.gather_context(store, run_id, req["asset_id"], req["as_of"], self.cooling_config,
                                    self.model_provider(), self.model_settings, self.workflow, self.clock)
        finally:
            store.close()
        return {"context": ctx, "trace": _trace("load_telemetry", "OK", t, detail={
            "run_id": run_id, "as_of": ctx.snapshot.as_of.isoformat() if ctx.snapshot.as_of else None,
            "history_windows": len(ctx.history), "forecast": ctx.forecast is not None,
            "optimizer_status": (ctx.optimizer or {}).get("status")})}

    def validate_telemetry(self, state):
        t, s = time.monotonic(), state["context"].snapshot
        status = "INSUFFICIENT" if s.as_of is None or s.data_quality.status == "INSUFFICIENT" else s.data_quality.status
        return {"telemetry_status": status,
                "trace": _trace("validate_telemetry", "OK", t, detail={"telemetry_status": status,
                                                                       "warnings": s.data_quality.warnings[:5]})}

    def assess_cooling_state(self, state):
        t, ctx = time.monotonic(), state["context"]
        evidence = {"thermal": ev.thermal_evidence(ctx), "energy": ev.energy_evidence(ctx),
                    "anomaly": ev.anomaly_evidence(ctx)}
        candidates, not_applicable = rules.build_candidates(ctx, evidence)
        s = ctx.snapshot
        required = {"anomaly"} if state["telemetry_status"] != "OK" or any(
            f.rule_id.startswith(("AF_", "DQ_")) for f in s.findings) else set()
        inlet = s.metrics.get("inlet_temperature_c")
        if inlet is not None and inlet.observed_points:
            required.add("thermal")
        required |= {TYPE_OWNER[c.type] for c in candidates}
        return {"evidence": evidence, "rule_candidates": candidates, "not_applicable": not_applicable,
                "required_agents": sorted(required),
                "trace": _trace("assess_cooling_state", "OK", t, detail={
                    "state": s.state, "severity": s.severity, "candidates": [c.type for c in candidates],
                    "required_agents": sorted(required)})}

    def _route_after_assess(self, state):
        return ["anomaly_agent"] if state["telemetry_status"] == "INSUFFICIENT" else ["plan_analysis"]

    def plan_analysis(self, state):
        t, ctx, required = time.monotonic(), state["context"], set(state["required_agents"])
        if self.main_llm is None:
            return {"plan": {"agents": list(AGENTS), "rationale": "deterministic mode: all analyses run",
                             "source": "deterministic"}, "trace": _trace("plan_analysis", "SKIPPED", t)}
        s = ctx.snapshot
        user = json.dumps({
            "asset_id": ctx.asset_id, "window": ctx.window, "state": s.state, "severity": s.severity,
            "workload_direction": s.workload_direction, "data_quality": s.data_quality.status,
            "findings": [{"rule_id": f.rule_id, "severity": f.severity, "explanation": f.explanation}
                         for f in s.findings],
            "forecast_available": ctx.forecast is not None,
            "optimizer_status": (ctx.optimizer or {}).get("status"),
            "measured_cooling_power_available": ev.energy_measurement_available(ctx),
            "fan_peer_comparison": (ctx.fan_peer or {}).get("status"),
            "available_analyses": {a: AGENT_ROLES[a] for a in AGENTS}}, ensure_ascii=False)
        system = ("You are the orchestrator of a data center cooling RECOMMENDATION system (it never controls "
                  "equipment). Choose which specialist analyses are relevant for this cooling-state snapshot. "
                  "Run only what the snapshot makes relevant. Answer with the tool.")
        try:
            plan, call = self.main_llm.invoke(AnalysisPlan, system, user)
        except LLMError as exc:
            return {"plan": {"agents": list(AGENTS), "rationale": f"planner failed ({exc.kind}); all analyses run",
                             "source": "fallback"},
                    "trace": _trace("plan_analysis", "FALLBACK", t, model=self.main_llm.model, error=exc.kind, error_detail=str(exc)[:400])}
        agents = sorted(set(plan.agents) | required)
        return {"plan": {"agents": agents, "rationale": plan.rationale, "source": "llm",
                         "llm_selected": sorted(set(plan.agents)), "added_as_required": sorted(required - set(plan.agents))},
                "trace": _trace("plan_analysis", "OK", t, **call.to_dict())}

    def _route_agents(self, state):
        return [f"{a}_agent" for a in state["plan"]["agents"]]

    def _agent_node(self, agent: str):
        def node(state):
            t, ctx = time.monotonic(), state["context"]
            items = state["evidence"][agent]
            attempt = (state.get("attempts") or {}).get(agent, 0) + 1
            eligible = sorted(k for k, v in TYPE_OWNER.items() if v == agent)
            if self.sub_llm is None or (agent == "energy" and not ev.energy_analysis_available(ctx)):
                reason = "deterministic mode" if self.sub_llm is None else \
                    "no measured cooling power and no fan peer comparison: deterministic result"
                return {"agent_results": {agent: {"status": "DETERMINISTIC", "finding": None, "issues": [],
                                                  "conflicts": [], "detail": reason}},
                        "attempts": {agent: attempt}, "trace": _trace(f"{agent}_agent", "DETERMINISTIC", t,
                                                                      detail=reason)}
            feedback = (state.get("feedback") or {}).get(agent)
            try:
                finding, call = run_specialist(self.sub_llm, agent, {
                    "asset_id": ctx.asset_id, "simulation_run_id": ctx.run_id, "window": ctx.window},
                    items, eligible, feedback)
            except LLMError as exc:
                return {"agent_results": {agent: {"status": "FAILED", "finding": None, "issues": [], "conflicts": [],
                                                  "error": f"{exc.kind}: {str(exc)[:300]}"}},
                        "attempts": {agent: attempt},
                        "trace": _trace(f"{agent}_agent", "FAILED", t, model=self.sub_llm.model, error=exc.kind, error_detail=str(exc)[:400],
                                        attempt=attempt)}
            return {"agent_results": {agent: {"status": "OK", "finding": finding, "issues": [], "conflicts": []}},
                    "attempts": {agent: attempt},
                    "trace": _trace(f"{agent}_agent", "OK", t, attempt=attempt, **call.to_dict())}
        return node

    def validate_evidence(self, state):
        t = time.monotonic()
        results, rerun, feedback = dict(state["agent_results"]), [], {}
        supported = {}
        for c in state["rule_candidates"]:
            if c.status == "REQUIRES_REVIEW":
                supported.setdefault(TYPE_OWNER[c.type], set()).add(c.type)
        for agent, r in results.items():
            if r["status"] != "OK":
                continue
            eligible = {k for k, v in TYPE_OWNER.items() if v == agent}
            issues, conflicts = validator.check_finding(r["finding"], state["evidence"][agent], eligible,
                                                        supported.get(agent, set()))
            results[agent] = {**r, "issues": issues, "conflicts": conflicts}
            if (issues or conflicts) and state["attempts"].get(agent, 0) <= self.workflow.max_reanalysis_attempts:
                rerun.append(agent)
                rule_view = [f"{c.type}: requirements {'met' if c.status == 'REQUIRES_REVIEW' else 'not met'}"
                             + (f" (missing: {'; '.join(c.missing_evidence)})" if c.missing_evidence else "")
                             for c in state["rule_candidates"] if TYPE_OWNER[c.type] == agent]
                feedback[agent] = [*issues, *conflicts, *(f"deterministic rule result: {x}" for x in rule_view)]
        return {"agent_results": results, "rerun": rerun, "feedback": feedback,
                "trace": _trace("validate_evidence", "OK", t, detail={
                    "rerun": rerun, "issues": {a: r.get("issues") for a, r in results.items() if r.get("issues")},
                    "conflicts": {a: r.get("conflicts") for a, r in results.items() if r.get("conflicts")}})}

    def _route_after_validation(self, state):
        return [f"{a}_agent" for a in state["rerun"]] or ["evaluate_candidates"]

    def evaluate_candidates(self, state):
        t, ctx = time.monotonic(), state["context"]
        recs = validator.validate_candidates(ctx, state["rule_candidates"], state["evidence"], state["agent_results"])
        return {"recommendations": recs, "trace": _trace("evaluate_candidates", "OK", t, detail={
            "requires_review": [r["type"] for r in recs if r["status"] == "REQUIRES_REVIEW"],
            "inconclusive": [r["type"] for r in recs if r["status"] == "INCONCLUSIVE"],
            "optimizer_status": (ctx.optimizer or {}).get("status")})}

    def rank_recommendations(self, state):
        t, ctx = time.monotonic(), state["context"]
        window_end = ctx.snapshot.as_of.isoformat() if ctx.snapshot.as_of else "none"
        ranked = []
        for r in validator.rank(state["recommendations"]):
            ranked.append({**r, "recommendation_id": recommendation_id(ctx.run_id, ctx.asset_id, r["type"], window_end)})
        return {"recommendations": ranked, "trace": _trace("rank_recommendations", "OK", t,
                                                           detail={"order": [r["type"] for r in ranked]})}

    def generate_explanation(self, state):
        t, ctx, recs = time.monotonic(), state["context"], state["recommendations"]
        if not recs:
            return {"explanation": {"source": "none", "overall_summary": "No recommendation: no recommendation "
                                    "type had any triggering evidence.", "items": {}},
                    "trace": _trace("generate_explanation", "SKIPPED", t)}
        templates = {r["recommendation_id"]: template_explanation(r) for r in recs}
        if self.main_llm is None:
            return {"explanation": {"source": "deterministic_template", "overall_summary": _template_summary(recs),
                                    "items": templates}, "trace": _trace("generate_explanation", "SKIPPED", t)}
        payload = json.dumps([{
            "recommendation_id": r["recommendation_id"], "type": r["type"], "status": r["status"],
            "title": r["title"], "recommended_action": r["recommended_action"],
            "evidence": json.loads(_evidence_json(r["evidence"])),
            "predicted_temperature_c": r["predicted_temperature"], "horizon_minutes": r["temperature_horizon_minutes"],
            "energy_impact": r["energy_impact_status"], "uncertainty": r["uncertainty"].model_dump(),
            "invalidation_conditions": r["conditions_that_invalidate_recommendation"],
            "missing_evidence": r["missing_evidence"], "hypotheses": r["hypotheses"],
            "validation_warnings": r["validation_warnings"]} for r in recs], ensure_ascii=False)
        system = ("You explain VALIDATED data center cooling recommendations to an operator. One item per "
                  "recommendation_id. Use only the facts given; quote numbers exactly as given and add no new ones "
                  "(no arithmetic, no unit conversions, no rounding beyond 2 decimals, no new timestamps). "
                  "Label measured values, model predictions and hypotheses as such. Say what evidence is missing. "
                  "Nothing has been executed: never claim any change was made; human review is required. "
                  "INCONCLUSIVE items: explain what is missing instead of recommending. Answer with the tool.")
        calls, issues = [], []
        user = payload
        for attempt in (1, 2):
            try:
                explanation, call = self.main_llm.invoke(Explanation, system, user)
            except LLMError as exc:
                return {"explanation": {"source": "deterministic_template", "overall_summary": _template_summary(recs),
                                        "items": templates, "llm_error": exc.kind},
                        "trace": _trace("generate_explanation", "FALLBACK", t, model=self.main_llm.model,
                                        error=exc.kind, error_detail=str(exc)[:400], calls=calls)}
            calls.append(call.to_dict())
            issues = validator.check_explanation(explanation, recs, ctx)
            if not issues:
                return {"explanation": {"source": "llm", "overall_summary": explanation.overall_summary,
                                        "items": {i.recommendation_id: i.model_dump() for i in explanation.items}},
                        "trace": _trace("generate_explanation", "OK", t, model=self.main_llm.model, calls=calls,
                                        **_sum_tokens(calls))}
            user = payload + "\n\nYour previous explanation was rejected: " + "; ".join(issues[:10])
        return {"explanation": {"source": "deterministic_template", "overall_summary": _template_summary(recs),
                                "items": templates, "rejected_llm_explanation": issues[:10]},
                "trace": _trace("generate_explanation", "FALLBACK", t, model=self.main_llm.model, calls=calls,
                                rejected=issues[:10], **_sum_tokens(calls))}

    def persist_recommendation(self, state):
        t, ctx = time.monotonic(), state["context"]
        expl = state["explanation"]
        created = datetime.now(timezone.utc)
        s = ctx.snapshot
        recs = []
        for r in state["recommendations"]:
            item = expl["items"].get(r["recommendation_id"])
            recs.append(Recommendation(
                **{k: v for k, v in r.items()}, analysis_id=state["analysis_id"], asset_id=ctx.asset_id,
                simulation_run_id=ctx.run_id,
                observation_window_start=s.window_start.isoformat() if s.window_start else None,
                observation_window_end=s.as_of.isoformat() if s.as_of else "none",
                explanation=item, explanation_source=expl["source"] if item else "none"))
        trace = [*state["trace"]]
        fallback = any(x["status"] in ("FALLBACK", "FAILED") for x in trace)
        status = "COMPLETED_WITH_FALLBACK" if fallback else "COMPLETED"
        execution = {
            "mode": self.mode, "status": status,
            "models": {"main": getattr(self.main_llm, "model", None), "subagent": getattr(self.sub_llm, "model", None)},
            "cooling_model": ctx.model_summary and {k: ctx.model_summary[k] for k in ("version", "label")},
            "plan": state.get("plan"), "telemetry_status": state["telemetry_status"],
            "agents": {a: {"status": r["status"], "issues": r.get("issues", []), "conflicts": r.get("conflicts", []),
                           "error": r.get("error"), "summary": r["finding"].summary if r.get("finding") else None,
                           "attempts": state["attempts"].get(a)} for a, r in state["agent_results"].items()},
            "not_applicable": [n.model_dump() for n in state["not_applicable"]],
            "explanation_source": expl["source"], "overall_summary": expl["overall_summary"],
            "tokens": _sum_tokens(trace)["tokens"], "nodes": trace,
            "latency_ms": round(sum(x.get("latency_ms", 0) for x in trace), 1)}
        store = self.store_factory()
        try:
            duplicates = store.save_analysis(
                {"analysis_id": state["analysis_id"], "simulation_run_id": ctx.run_id, "asset_id": ctx.asset_id,
                 "window_start": s.window_start, "window_end": s.as_of, "status": status,
                 "trace_json": execution, "created_at": created},
                [{"recommendation_id": r.recommendation_id, "analysis_id": state["analysis_id"],
                  "simulation_run_id": ctx.run_id, "asset_id": ctx.asset_id, "recommendation_type": r.type,
                  "status": r.status, "severity": r.severity, "window_start": s.window_start,
                  "window_end": s.as_of or created, "payload_json": r.model_dump(mode="json"), "created_at": created}
                 for r in recs],
                alert_rows(state["analysis_id"], ctx, recs, created))
        finally:
            store.close()
        out = {"analysis_id": state["analysis_id"], "status": status, "asset_id": ctx.asset_id,
               "simulation_run_id": ctx.run_id,
               "observation_window": {"start": s.window_start.isoformat() if s.window_start else None,
                                      "end": s.as_of.isoformat() if s.as_of else None},
               "recommendations": [r.model_dump(mode="json") for r in recs],
               "duplicates_of_existing": duplicates, "execution": execution,
               "human_approval_required": True, "automatic_control_executed": False}
        return {"persisted": out, "trace": _trace("persist_recommendation", "OK", t)}


def alert_id(run_id: str, asset_id: str, rule_id: str, window_end: str) -> str:
    return "alr-" + hashlib.sha256(f"{run_id}|{asset_id}|{rule_id}|{window_end}".encode()).hexdigest()[:20]


def alert_rows(analysis_id: str, ctx, recs, created) -> list[dict]:
    """WARNING/CRITICAL estimator findings of the analysed window (observations or inferences, never confirmed
    faults). In a historical analysis the wall-clock 'pipeline stopped' finding is not an alert for that window."""
    s, out = ctx.snapshot, []
    if s.as_of is None:
        return out
    for f in s.findings:
        if f.severity not in ("WARNING", "CRITICAL") or \
                (f.rule_id == "DQ_PIPELINE_STALE" and ctx.as_of_requested is not None):
            continue
        related = [r.recommendation_id for r in recs if any(e.metric == f.rule_id or e.details.get("rule_id") == f.rule_id
                                                             for e in r.evidence)]
        payload = {"alert_id": alert_id(ctx.run_id, ctx.asset_id, f.rule_id, s.as_of.isoformat()),
                   "asset_id": ctx.asset_id, "simulation_run_id": ctx.run_id, "rule_id": f.rule_id,
                   "state": f.state, "severity": f.severity, "kind": f.kind, "explanation": f.explanation,
                   "evidence": f.evidence, "observation_window_start": s.window_start.isoformat() if s.window_start
                   else None, "observation_window_end": s.as_of.isoformat(), "related_recommendations": related,
                   "source": "cooling_state_estimator", "analysis_id": analysis_id,
                   "note": "observed pattern or inference from configured rules; not a confirmed equipment fault"}
        out.append({"alert_id": payload["alert_id"], "analysis_id": analysis_id, "simulation_run_id": ctx.run_id,
                    "asset_id": ctx.asset_id, "rule_id": f.rule_id, "severity": f.severity,
                    "window_start": s.window_start, "window_end": s.as_of, "payload_json": payload,
                    "created_at": created})
    return out


def _sum_tokens(entries) -> dict:
    total = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    stack = list(entries)
    while stack:
        e = stack.pop()
        if isinstance(e, dict):
            if "calls" in e:
                stack += e["calls"]
            elif "total_tokens" in e:
                for k in total:
                    total[k] += int(e.get(k) or 0)
    return {"tokens": total}


def _template_summary(recs) -> str:
    review = [r["type"] for r in recs if r["status"] == "REQUIRES_REVIEW"]
    inconclusive = [r["type"] for r in recs if r["status"] == "INCONCLUSIVE"]
    return (f"{len(review)} recommendation(s) for human review ({', '.join(review) or 'none'}); "
            f"{len(inconclusive)} inconclusive ({', '.join(inconclusive) or 'none'}). Nothing has been executed.")


def template_explanation(r: dict) -> dict:
    """Deterministic explanation built only from the validated recommendation's own fields."""
    measured = [f"{e.metric} = {e.observed_value:g} {e.unit} ({e.interpretation})" if e.observed_value is not None
                else f"{e.metric}: {e.interpretation}" for e in r["evidence"]
                if e.kind in ("measured", "derived", "rule_finding", "configuration")]
    predicted = [f"{e.metric} = {e.observed_value:g} {e.unit} ({e.interpretation})" for e in r["evidence"]
                 if e.kind == "model_prediction" and e.observed_value is not None]
    return {"recommendation_id": r["recommendation_id"],
            "what_to_evaluate": r["recommended_action"] if r["status"] == "REQUIRES_REVIEW" else
            "Nothing yet: this result is inconclusive until the missing evidence is available.",
            "why": r["title"] + ".", "measured_observations": measured[:10], "model_predictions": predicted[:6],
            "energy_impact": r["energy_impact_status"],
            "uncertainty": f"{r['uncertainty'].level}: {r['uncertainty'].basis}",
            "invalidation_conditions": r["conditions_that_invalidate_recommendation"],
            "missing_evidence": r["missing_evidence"],
            "why_human_review": "Recommendation-only system: nothing has been executed; an operator must review and "
                                "approve any real-world test."}
