"""Copilot LangGraph workflow.

    START -> validate_input -> understand (deterministic parse + optional main-LLM plan, re-validated)
          -> [clarify -> remember -> END]
          -> select_tools -> run_tool x N (parallel, read-only)
          -> specialists (subagent LLM; only for why/anomaly/maintenance questions about one asset)
          -> validate_results -> compose_answer (main LLM, number-checked; deterministic fallback)
          -> remember -> END

Routing uses the minimum tool set for the detected intents. The LLM never sees credentials or SQL; it
receives evidence items and returns structured text whose numbers must come from that evidence.
"""

import json
import operator
import re
import time
from collections.abc import Callable
from datetime import datetime
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from data_engineering import telemetry_query as tq
from data_engineering.copilot.memory import ConversationStore, Turn
from data_engineering.copilot.schemas import INTENTS, CopilotAnswer, QueryPlan
from data_engineering.copilot.tools import CopilotTools
from data_engineering.copilot.understanding import Understanding, resolve_window, understand
from data_engineering.logging_utils import get_logger
from data_engineering.recommendations.llm import LLMError, StructuredLLM

log = get_logger("copilot")
MAX_QUESTION_CHARS = 2000
CORRELATION_NOTE = ("Co-occurring changes in this telemetry show correlation, not proven causation; causes listed "
                    "are hypotheses.")
CONTROL_REFUSAL = ("This copilot is read-only and recommendation-only: it cannot change fan speeds, setpoints or any "
                   "equipment. ")
_CONTROL_REQUEST = re.compile(r"\b(set|change|turn|switch|increase|decrease|lower|raise|reduce|adjust)\b.*\b(fan|"
                              r"setpoint|chiller|crac|crah|cooling)\b.*\b(now|immediately|for me|please)\b",
                              re.IGNORECASE)
_NUMBER = re.compile(r"(?<![\w.])[-+]?\d+(?:\.\d+)?")
_IGNORED = re.compile(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?)?"
                      r"|\b\d{1,2}:\d{2}(?::\d{2})?\b|\[E\d+\]|\bE\d+\b|\b[A-Z]+-[A-Z]+-\d+\b|\bsim-[0-9a-f]+\b"
                      r"|\b(?:rec|ana|alr|crm)-[0-9a-zA-Z-]+")


def _merge(a, b):
    return {**(a or {}), **(b or {})}


class CopilotState(TypedDict, total=False):
    request: dict
    previous: Any
    understanding: Any
    plan_source: str
    interpretation: dict
    clarification: str | None
    tools: list
    tool_results: Annotated[dict, _merge]
    evidence: list
    answer: dict
    warnings: Annotated[list, operator.add]
    trace: Annotated[list, operator.add]
    response: dict


def _t(node, status, started, **extra):
    return [{"node": node, "status": status, "latency_ms": round((time.monotonic() - started) * 1000, 1), **extra}]


def check_numbers(texts: list[str], evidence: list[dict], extra: list[float]) -> list[str]:
    allowed = [float(e["value"]) for e in evidence if isinstance(e.get("value"), (int, float))] + extra
    bad = []
    for text in texts:
        for raw in _NUMBER.findall(_IGNORED.sub(" ", text)):
            x = float(raw)
            if abs(x) <= 10 and x.is_integer():
                continue
            if not any(abs(x - a) <= max(0.051, abs(a) * 0.005) or abs(x - a * 100) <= max(0.051, abs(a) * 0.5)
                       for a in allowed):
                bad.append(raw)
    return sorted(set(bad))


class CopilotEngine:
    def __init__(self, *, tools_factory: Callable[[Any], CopilotTools], known_assets: Callable[[str], list[str]],
                 data_end: Callable[[str], datetime | None], latest_run: Callable[[], str | None],
                 memory: ConversationStore, main_llm: StructuredLLM | None, sub_llm: StructuredLLM | None,
                 default_window_minutes: int = 60, max_window_minutes: int = 1440) -> None:
        self.tools_factory, self.known_assets, self.data_end, self.latest_run = tools_factory, known_assets, \
            data_end, latest_run
        self.memory, self.main_llm, self.sub_llm = memory, main_llm, sub_llm
        self.default_window, self.max_window = default_window_minutes, max_window_minutes
        self.graph = self._build()

    @property
    def mode(self) -> str:
        return "llm" if self.main_llm else "deterministic"

    def _build(self):
        g = StateGraph(CopilotState)
        for name, fn in (("validate_input", self.validate_input), ("understand", self.understand),
                         ("clarify", self.clarify), ("select_tools", self.select_tools), ("run_tool", self.run_tool),
                         ("validate_results", self.validate_results), ("compose_answer", self.compose_answer),
                         ("remember", self.remember)):
            g.add_node(name, fn)
        g.add_edge(START, "validate_input")
        g.add_conditional_edges("validate_input", lambda s: "clarify" if s.get("clarification") else "understand",
                                ["clarify", "understand"])
        g.add_conditional_edges("understand", lambda s: "clarify" if s.get("clarification") else "select_tools",
                                ["clarify", "select_tools"])
        g.add_edge("clarify", "remember")
        g.add_conditional_edges("select_tools", lambda s: [Send("run_tool", {**s, "tool": t}) for t in s["tools"]]
                                or ["validate_results"], ["run_tool", "validate_results"])
        g.add_edge("run_tool", "validate_results")
        g.add_edge("validate_results", "compose_answer")
        g.add_edge("compose_answer", "remember")
        g.add_edge("remember", END)
        return g.compile()

    def chat(self, message: str, *, scope, conversation_id: str | None = None, asset_id: str | None = None,
             run_id: str | None = None, start_time: datetime | None = None, end_time: datetime | None = None) -> dict:
        conv = self.memory.get(conversation_id)
        state = self.graph.invoke({"request": {"message": message, "conversation_id": conversation_id,
                                               "asset_id": asset_id, "run_id": run_id, "start_time": start_time,
                                               "end_time": end_time, "scope": scope},
                                   "previous": conv.turns[-1] if conv and conv.turns else None,
                                   "tool_results": {}, "warnings": [], "trace": []}, {"recursion_limit": 30})
        return state["response"]

    # ------------------------------------------------------------------ nodes
    def validate_input(self, state):
        t, req = time.monotonic(), state["request"]
        msg = (req["message"] or "").strip()
        if not msg:
            return {"clarification": "Please ask a question.", "trace": _t("validate_input", "REJECTED", t)}
        if len(msg) > MAX_QUESTION_CHARS:
            return {"clarification": f"Please keep questions under {MAX_QUESTION_CHARS} characters.",
                    "trace": _t("validate_input", "REJECTED", t)}
        if req["asset_id"]:
            req["scope"].check(req["asset_id"])          # 403 for an explicitly requested out-of-scope asset
        warnings = [CONTROL_REFUSAL + "I can describe the evidence and the evaluation status of cooling settings "
                    "instead."] if _CONTROL_REQUEST.search(msg) else []
        return {"warnings": warnings, "trace": _t("validate_input", "OK", t)}

    def understand(self, state):
        t, req, prev = time.monotonic(), state["request"], state.get("previous")
        run_id = req["run_id"] or self.latest_run()
        if run_id is None:
            return {"clarification": "The telemetry store has no data yet.", "trace": _t("understand", "NO_DATA", t)}
        known = req["scope"].filter(self.known_assets(run_id))
        u: Understanding = understand(req["message"], known, prev, req["asset_id"], self.max_window)
        source, calls, planner_warnings = "deterministic", [], []
        if self.main_llm is not None:
            try:
                plan, call = self.main_llm.invoke(QueryPlan, (
                    "You interpret an operator's question about data center telemetry for a READ-ONLY analytics "
                    "copilot. Pick intents, the asset ids it refers to (only from known_assets), whether it concerns "
                    "all assets, and the window length if stated. Use previous_turn for references like 'it'. If the "
                    "asset or time is ambiguous, ask a clarification question. Answer with the tool."),
                    json.dumps({"question": req["message"], "known_assets": known[:60],
                                "previous_turn": prev.__dict__ if prev else None,
                                "deterministic_reading": {"intents": u.intents, "asset_ids": u.asset_ids,
                                                          "all_assets": u.all_assets}}, ensure_ascii=False))
                calls.append(call.to_dict())
                source = "llm+deterministic"
                u.intents = list(dict.fromkeys([*u.intents, *[i for i in plan.intents if i in INTENTS]]))
                llm_assets = [a for a in plan.asset_ids if a in known]
                if not u.asset_ids and llm_assets:
                    u.asset_ids, u.from_context = llm_assets, [*u.from_context, "assets (main LLM)"]
                    if u.clarification and "Which asset" in u.clarification:
                        u.clarification = None
                u.all_assets = u.all_assets or (plan.all_assets and not u.asset_ids)
                if u.window_minutes is None and plan.window_minutes and 0 < plan.window_minutes <= self.max_window:
                    u.window_minutes = plan.window_minutes
                if plan.needs_clarification and plan.clarification_question and not u.asset_ids and not u.all_assets:
                    u.clarification = u.clarification or plan.clarification_question
            except LLMError as exc:
                planner_warnings.append(f"question planner unavailable ({exc.kind}); deterministic "
                                        "interpretation used")
        data_end = self.data_end(run_id)
        if data_end is None:
            return {"clarification": "No telemetry is available for this run.", "trace": _t("understand", "NO_DATA", t)}
        if req["end_time"]:
            u.explicit_end = req["end_time"]
        if req["start_time"]:
            u.explicit_start = req["start_time"]
        start, end, basis = resolve_window(u, prev, data_end, self.default_window, self.max_window)
        if start >= end:
            u.clarification = "The requested time window is empty or outside the available telemetry."
        interp = {"intents": u.intents, "asset_ids": u.asset_ids, "all_assets": u.all_assets or not u.asset_ids,
                  "start_time": start.isoformat(), "end_time": end.isoformat(), "time_basis": basis,
                  "resolved_from_context": u.from_context, "source": source,
                  "candidate_settings": u.candidate_settings, "simulation_run_id": run_id}
        return {"understanding": u, "interpretation": interp, "clarification": u.clarification, "plan_source": source,
                "warnings": [*u.notes, *planner_warnings], "trace": _t("understand", "OK", t, calls=calls)}

    def clarify(self, state):
        t = time.monotonic()
        q = state["clarification"]
        return {"response": self._response(state, answer=q, clarification=True, evidence=[]),
                "trace": _t("clarify", "OK", t)}

    def select_tools(self, state):
        t, i = time.monotonic(), state["interpretation"]
        intents = set(i["intents"])
        tools = []
        if intents & {"status", "thermal", "workload"}:
            tools.append("latest")
        if intents & {"history", "trend", "thermal", "workload", "comparison", "status"}:
            tools.append("window_summary")
        if "trend" in intents and i["all_assets"]:
            tools.append("trend_screen")
        if "forecast" in intents:
            tools.append("forecast")
        if "energy" in intents:
            tools.append("energy")
        if intents & {"recommendations", "maintenance_priority"}:
            tools.append("recommendations")
        if "maintenance_priority" in intents or ("anomaly" in intents and i["all_assets"]):
            tools.append("maintenance_priority")
        if "cooling_setting" in intents and i["asset_ids"]:
            tools.append("cooling_candidates")
        if intents & {"explanation", "anomaly"} and len(i["asset_ids"]) == 1:
            tools.append("specialists")
        if not tools:
            tools = ["latest"]
        return {"tools": list(dict.fromkeys(tools)), "trace": _t("select_tools", "OK", t, tools=tools)}

    def _specialist_agents(self, intents: set) -> list[str]:
        agents = []
        if intents & {"thermal", "explanation", "trend", "forecast"}:
            agents.append("thermal")
        if "workload" in intents or ("explanation" in intents and "thermal" in intents):
            agents.append("workload")
        if intents & {"anomaly", "maintenance_priority"}:
            agents.append("anomaly")
        if "energy" in intents:
            agents.append("energy")
        return agents[:2] or ["thermal"]

    def run_tool(self, state):
        t, tool, i, req = time.monotonic(), state["tool"], state["interpretation"], state["request"]
        tools = self.tools_factory(req["scope"])
        run_id, assets = i["simulation_run_id"], i["asset_ids"]
        start, end = datetime.fromisoformat(i["start_time"]), datetime.fromisoformat(i["end_time"])
        try:
            if tool == "latest":
                r = tools.latest(run_id, assets, end)
            elif tool == "window_summary":
                r = tools.window_summary(run_id, assets, start, end)
            elif tool == "trend_screen":
                r = tools.trend_screen(run_id, assets, start, end)
            elif tool == "forecast":
                r = tools.forecast(run_id, assets or [], end) if assets else \
                    {"tool": "xgboost_forecast", "status": "SKIPPED", "evidence": [], "error": None,
                     "warnings": ["forecasts need a specific asset"]}
            elif tool == "energy":
                r = tools.energy(run_id, assets, start, end)
            elif tool == "recommendations":
                r = tools.recommendations(run_id, assets)
            elif tool == "maintenance_priority":
                r = tools.maintenance_priority(run_id, assets, end)
            elif tool == "cooling_candidates":
                r = tools.cooling_candidates(run_id, assets[0], i["candidate_settings"], end)
            else:
                r = tools.specialists(run_id, assets[0], end, self._specialist_agents(set(i["intents"])),
                                      req["message"])
        except Exception as exc:              # a failed tool is reported, never silently ignored
            log.error("copilot tool failed", exc_info=exc, extra={"context": {"tool": tool}})
            r = {"tool": tool, "status": "FAILED", "evidence": [], "warnings": [f"{tool} failed: "
                 f"{type(exc).__name__}"], "error": type(exc).__name__}
        return {"tool_results": {tool: r}, "trace": _t(f"tool:{tool}", r["status"], t, calls=r.get("calls", []))}

    def validate_results(self, state):
        t, evidence, warnings = time.monotonic(), [], []
        for name in state.get("tools", []):
            r = state["tool_results"].get(name)
            if r is None:
                continue
            warnings += [w for w in r.get("warnings", []) if w]
            for e in r["evidence"]:
                if e["kind"] == "measured" and isinstance(e["value"], (int, float)) and e["unit"] is None:
                    warnings.append(f"dropped {e['metric']}: measured value without a unit")
                    continue
                evidence.append({**e, "id": f"E{len(evidence) + 1}", "tool": r["tool"]})
        if not evidence:
            warnings.append("no evidence was found for this question in the requested scope and window")
        return {"evidence": evidence, "warnings": warnings, "trace": _t("validate_results", "OK", t,
                                                                         evidence=len(evidence))}

    def compose_answer(self, state):
        t, ev, i = time.monotonic(), state["evidence"], state["interpretation"]
        fallback = self._deterministic_answer(state)
        if self.main_llm is None or not ev:
            return {"answer": fallback, "trace": _t("compose_answer", "DETERMINISTIC", t)}
        specialist = state["tool_results"].get("specialists", {}).get("findings", [])
        payload = {"question": state["request"]["message"], "interpretation": i,
                   "evidence": [{k: e[k] for k in ("id", "kind", "asset_id", "metric", "value", "unit", "window", "note")}
                                for e in ev[:120]],
                   "specialist_findings": [{k: f.get(k) for k in ("agent", "summary", "hypotheses")}
                                           for f in specialist if f.get("summary")],
                   "warnings": state.get("warnings", [])[:15]}
        system = ("You answer an operator's question about data center telemetry using ONLY the evidence items. Cite "
                  "ids like [E3]. Quote numbers exactly as given; add no new numbers (no arithmetic beyond what is "
                  "given). Separate measured values, calculated metrics, model predictions and hypotheses. For 'why' "
                  "questions say that co-occurrence is correlation, not proven causation. Never claim any equipment "
                  "change was made; this system is read-only and recommendation-only. If evidence is missing, say "
                  "what is missing. State the time window used. Answer with the tool.")
        calls, user = [], json.dumps(payload, ensure_ascii=False, default=str)
        extra = [float(i.get("window_minutes") or 0)] + [float(x) for x in i.get("candidate_settings", [])]
        for attempt in (1, 2):
            try:
                a, call = self.main_llm.invoke(CopilotAnswer, system, user)
            except LLMError as exc:
                return {"answer": fallback, "warnings": [f"answer model unavailable ({exc.kind}); deterministic "
                                                         "summary returned"],
                        "trace": _t("compose_answer", "FALLBACK", t, calls=calls, error=exc.kind)}
            calls.append(call.to_dict())
            texts = [a.answer, *a.measured, *a.calculated, *a.predictions, *a.hypotheses, *a.recommendations,
                     *a.limitations]
            bad = check_numbers(texts, ev, extra)
            claims = [x for x in texts if re.search(r"\b(have|has|was|were)\s+(set|changed|adjusted|reduced|increased)"
                                                    r"\b", x, re.I) and not re.search(r"\b(not|no|never|nothing)\b", x, re.I)]
            if not bad and not claims:
                answer = a.model_dump()
                if "explanation" in i["intents"] and CORRELATION_NOTE not in answer["limitations"]:
                    answer["limitations"].append(CORRELATION_NOTE)
                answer["source"] = "llm"
                return {"answer": answer, "trace": _t("compose_answer", "OK", t, calls=calls)}
            user += f"\n\nRejected: numbers not in the evidence {bad[:10]}; action claims {claims[:2]}. Fix them."
        return {"answer": fallback, "warnings": ["the generated answer failed validation (unsupported numbers or "
                                                 "claims); deterministic summary returned"],
                "trace": _t("compose_answer", "FALLBACK", t, calls=calls, rejected=bad[:10])}

    def _deterministic_answer(self, state) -> dict:
        ev, i = state["evidence"], state["interpretation"]
        groups = {k: [e for e in ev if e["kind"] == k] for k in ("measured", "calculated", "prediction", "hypothesis",
                                                                 "recommendation", "unavailable")}

        def line(e):
            v = e["value"]
            val = f"{v:g}" if isinstance(v, float) else v
            return (f"[{e['id']}] {e.get('asset_id') or 'all'} {e['metric']} = {val}"
                    + (f" {e['unit']}" if e.get("unit") else "") + (f" ({e['note']})" if e.get("note") else ""))
        assets = ", ".join(i["asset_ids"]) if i["asset_ids"] else "all assets in scope"
        highlights = self._highlights(state)
        prefix = CONTROL_REFUSAL if any(w.startswith(CONTROL_REFUSAL) for w in state.get("warnings", [])) else ""
        summary = (prefix + f"For {assets} over {i['time_basis']}: " + (" ".join(highlights) if highlights else
                   "no specific finding.") + f" ({len(groups['measured'])} measured, {len(groups['calculated'])} "
                   f"calculated, {len(groups['prediction'])} predicted evidence items; deterministic summary.)")
        limitations = [f"unavailable: {e['metric']}: {e.get('note')}" for e in groups["unavailable"]][:8]
        if "explanation" in i["intents"]:
            limitations.append(CORRELATION_NOTE)
        return {"answer": summary, "measured": [line(e) for e in groups["measured"][:10]],
                "calculated": [line(e) for e in groups["calculated"][:10]],
                "predictions": [line(e) for e in groups["prediction"][:6]],
                "hypotheses": [f"[{e['id']}] {e['note']}" for e in groups["hypothesis"][:6]],
                "recommendations": [line(e) for e in groups["recommendation"][:6]], "limitations": limitations,
                "source": "deterministic"}

    @staticmethod
    def _highlights(state) -> list[str]:
        """One sentence per tool result, built only from tool outputs (values already in the evidence)."""
        r, out = state["tool_results"], []
        if (ts := r.get("trend_screen")) and ts.get("table"):
            flagged = [x["asset_id"] for x in ts["table"] if x["rising_inlet_with_stable_workload"]]
            out.append(f"Calculated: {len(flagged)} of {len(ts['table'])} assets show a rising inlet trend with a "
                       f"stable workload{': ' + ', '.join(flagged) if flagged else ''}.")
        if (mp := r.get("maintenance_priority")) and not mp.get("table") and mp.get("status") == "OK":
            out.append("Calculated: no asset currently has warning findings, stored alerts or open recommendations, "
                       "so there is no evidence-based investigation priority.")
        if (mp := r.get("maintenance_priority")) and mp.get("table"):
            top = [f"{x['asset_id']} ({x['severity']}; {', '.join(x['findings']) or 'no warning findings'})"
                   for x in mp["table"][:3]]
            out.append("Triage order (calculated): " + "; ".join(top) + ".")
        if (fc := r.get("forecast")) and fc.get("predictions"):
            out.append("XGBoost predictions under observed conditions: " + "; ".join(
                f"{p['asset_id']} {p['predicted_inlet_temperature_c']:g} degC at +{p['horizon_minutes']} min"
                for p in fc["predictions"]) + ".")
        elif fc and fc.get("status") == "UNAVAILABLE":
            out.append(f"Forecast unavailable: {fc.get('error')}.")
        if (ws := r.get("window_summary")) and len(ws.get("table") or []) > 3:
            rows = [x for x in ws["table"] if x.get("inlet_temperature_c")]
            if rows:
                hi = max(rows, key=lambda x: x["inlet_temperature_c"]["mean"])
                lo = min(rows, key=lambda x: x["inlet_temperature_c"]["mean"])
                out.append(f"Calculated over {len(rows)} assets: highest mean inlet {hi['asset_id']} "
                           f"({hi['inlet_temperature_c']['mean']:g} degC), lowest {lo['asset_id']} "
                           f"({lo['inlet_temperature_c']['mean']:g} degC).")
        if (ws := r.get("window_summary")) and ws.get("table") and len(ws["table"]) <= 3:
            for row in ws["table"]:
                inlet, work = row.get("inlet_temperature_c"), row.get("workload_intensity")
                if inlet:
                    out.append(f"Calculated for {row['asset_id']}: mean inlet {inlet['mean']:g} degC (trend "
                               f"{inlet['trend_per_minute']} degC/min)"
                               + (f", mean workload {work['mean']:g}" if work else "") + ".")
        if (cc := r.get("cooling_candidates")) and cc.get("optimizer"):
            o = cc["optimizer"]
            out.append(f"Cooling-setting evaluation: {o['status']} ({', '.join(x['code'] for x in o['reasons'])}).")
        if (en := r.get("energy")):
            peer = en.get("fan_peer") or []
            elevated = [x for x in peer if x["status"] == "ELEVATED"]
            screened = [x for x in peer if x["status"] not in ("INSUFFICIENT_PEERS", "INSUFFICIENT_DATA")]
            if elevated:
                out.append("Possible fan-energy waste (calculated, relative only): " + "; ".join(
                    f"{x['asset_id']} fan {x['excess_fraction']:+.1%} vs comparable peers (affinity-law power ratio "
                    f"{x['relative_fan_power']:.2f})" for x in elevated) + ".")
            elif peer:
                out.append(f"Fan peer comparison (calculated): no elevated fan speed among {len(screened)} screenable "
                           f"asset(s); {len(peer) - len(screened)} could not be compared (too few comparable peers).")
            out.append("Cooling energy cannot be quantified in kW: no measured cooling power exists (server IT power "
                       "is not cooling power).")
        if (rc := r.get("recommendations")) is not None and "recommendations" in r:
            out.append(f"{len(rc.get('recommendations', []))} stored recommendation(s) found.")
        if (sp := r.get("specialists")):
            done = [f["summary"] for f in sp.get("findings", []) if f.get("summary")]
            out.append("Specialist interpretation: " + " ".join(done)[:600] if done else
                       "Specialist interpretation not available (deterministic evidence only).")
        return out

    def remember(self, state):
        t, req = time.monotonic(), state["request"]
        response = state.get("response") or self._response(state, answer=state["answer"]["answer"])
        i = state.get("interpretation") or {}
        cid = self.memory.append(req["conversation_id"], Turn(
            question=req["message"][:500], asset_ids=i.get("asset_ids", []), all_assets=i.get("all_assets", False),
            start_time=i.get("start_time"), end_time=i.get("end_time"), intents=i.get("intents", []),
            answer_summary=str(response["answer"])[:300]))
        response["conversation_id"] = cid
        response["agent_execution_summary"]["nodes"] = [*state.get("trace", []), *_t("remember", "OK", t)]
        return {"response": response}

    def _response(self, state, *, answer, clarification=False, evidence=None) -> dict:
        results = state.get("tool_results", {})
        ans = state.get("answer") or {}
        tokens = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        for n in state.get("trace", []):
            for c in n.get("calls", []):
                for k in tokens:
                    tokens[k] += int(c.get(k) or 0)
        tables = {name: r["table"] for name, r in results.items() if r.get("table")}
        return {
            "conversation_id": None, "answer": answer, "clarification_needed": clarification,
            "interpretation": state.get("interpretation"),
            "sections": {k: ans.get(k, []) for k in ("measured", "calculated", "predictions", "hypotheses",
                                                     "recommendations", "limitations")},
            "evidence": state.get("evidence", []) if evidence is None else evidence,
            "metrics": tables,
            "predictions": [p for r in results.values() for p in r.get("predictions", [])],
            "recommendations": [x for r in results.values() for x in r.get("recommendations", [])],
            "optimizer": results.get("cooling_candidates", {}).get("optimizer"),
            "warnings": list(dict.fromkeys(state.get("warnings", []))),
            "agent_execution_summary": {
                "mode": self.mode, "answer_source": ans.get("source", "none" if clarification else "deterministic"),
                "tools": {n: r["status"] for n, r in results.items()},
                "specialists": [{k: f.get(k) for k in ("agent", "status", "error")}
                                for f in results.get("specialists", {}).get("findings", [])],
                "models": {"main": getattr(self.main_llm, "model", None), "subagent": getattr(self.sub_llm, "model",
                                                                                             None)},
                "tokens": tokens},
            "read_only": True, "automatic_control_executed": False,
            "units_note": "units per evidence item; telemetry_query.UNITS documents all metrics",
            "unavailable_measurements": tq.UNAVAILABLE_MEASUREMENTS}
