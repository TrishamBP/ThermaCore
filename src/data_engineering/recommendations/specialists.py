"""Specialist subagents shared by the recommendation workflow and the operations copilot.

One implementation: a specialist receives deterministic evidence (with ids) and returns a structured
AgentFinding from the subagent model. It never queries data, computes numbers or touches equipment.
"""

import json

from data_engineering.recommendations.llm import LLMCall, StructuredLLM
from data_engineering.recommendations.schemas import AgentFinding, Evidence

TYPE_REQUIREMENTS = {
    "COOLING_EFFICIENCY": "stable workload, inlet headroom >= the configured approach margin, no thermal or airflow "
                          "warnings, an intervention-validated response model and measured cooling power",
    "PREEMPTIVE_COOLING": "a rising-inlet or approaching-limit finding AND a forecast at or above (inlet limit minus "
                          "margin) with forecast inputs inside the training range",
    "AIRFLOW_INVESTIGATION": "an airflow/fan finding (AF_*) with usable fan, airflow and inlet readings",
    "ENERGY_WASTE": "fan speed ELEVATED vs comparable peers (same cooling/asset type, similar workload and inlet) "
                    "in at least the required number of windows, with no thermal or airflow warnings; impact is "
                    "relative only (no kW: no measured cooling power)",
    "MAINTENANCE_INSPECTION": "the same anomaly rule persisting across the required number of consecutive windows "
                              "with good data quality",
    "SENSOR_DATA_QUALITY": "stale, missing, irregular, conflicting-duplicate or physically contradictory telemetry",
}
AGENT_ROLES = {
    "thermal": "Thermal Analysis Agent: inlet/exhaust temperature trends, thermal headroom against CONFIGURED "
               "limits, workload stability, and the XGBoost forecast results provided.",
    "energy": "Energy Analysis Agent: fan-energy waste indicators from the fan-speed peer comparison (relative "
              "only; no kW because no cooling power is measured) and energy impact of candidate settings when "
              "measured cooling power exists.",
    "anomaly": "Anomaly and Maintenance Agent: airflow and fan anomalies, persistent abnormal patterns, and stale, "
               "missing, duplicate or contradictory telemetry, compared with recent history.",
    "workload": "Workload Analysis Agent: CPU/GPU/memory utilization and workload intensity levels and trends, and "
                "how they co-vary with temperature (co-variation is correlation, not proven causation).",
}
SUBAGENT_RULES = (
    "Rules: use ONLY the evidence provided and cite evidence_ids (e.g. TH-3). Do not invent numbers, limits, "
    "forecasts, energy figures or settings. Distinguish observations from hypotheses; never state that a component "
    "has failed. You cannot change or command any equipment. A recommendation type is supported only if the evidence "
    "satisfies ALL of its requirements.")


def evidence_json(items: list[Evidence]) -> list[dict]:
    return [{"evidence_id": e.evidence_id, "kind": e.kind, "metric": e.metric, "observed_value": e.observed_value,
             "unit": e.unit, "interpretation": e.interpretation} for e in items]


def run_specialist(llm: StructuredLLM, agent: str, header: dict, items: list[Evidence],
                   eligible: list[str] = (), feedback: list[str] | None = None,
                   question: str | None = None) -> tuple[AgentFinding, LLMCall]:
    """Raises LLMError on provider/parse failure (callers record it; never a silent success)."""
    payload = {**header, "evidence": evidence_json(items),
               "eligible_recommendation_types": {k: TYPE_REQUIREMENTS[k] for k in eligible}}
    if question:
        payload["operator_question"] = question
    if feedback:
        payload["previous_attempt_problems"] = feedback
    system = f"You are the {AGENT_ROLES[agent]} {SUBAGENT_RULES} Answer with the tool."
    return llm.invoke(AgentFinding, system, json.dumps(payload, ensure_ascii=False))
