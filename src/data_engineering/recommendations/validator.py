"""Deterministic safety and feasibility validator (plain Python, no LLM judgement).

Checks agent findings (citations, eligibility, agreement with the rules), applies constraints to
candidates (limits configured, telemetry fresh and complete, model outputs valid and in range, no
unsupported counterfactual settings, failed agents), assigns uncertainty by a fixed rule, ranks,
and checks LLM explanations (no numbers that are not in the evidence, no claims of execution).
"""

import math
import re

from data_engineering.recommendations.rules import Candidate
from data_engineering.recommendations.schemas import (
    ALLOWED_UNITS,
    TYPE_OWNER,
    AgentFinding,
    Evidence,
    Explanation,
    Uncertainty,
)

SEVERITY_RANK = {"CRITICAL": 2, "WARNING": 1, "INFO": 0}
TYPE_PRIORITY = ["PREEMPTIVE_COOLING", "AIRFLOW_INVESTIGATION", "SENSOR_DATA_QUALITY", "MAINTENANCE_INSPECTION",
                 "ENERGY_WASTE", "COOLING_EFFICIENCY"]
THERMAL_TYPES = {"PREEMPTIVE_COOLING", "COOLING_EFFICIENCY"}

UNCERTAINTY_METHOD = (
    "Categorical rule, not a probability: UNKNOWN if inconclusive; HIGH if telemetry quality is degraded or the "
    "recommendation relies on a model prediction that does not beat persistence offline; MEDIUM if it relies on "
    "trend/projection inferences or model predictions; LOW if it relies only on direct observations with good "
    "data quality.")


# ------------------------------------------------------------------ agent findings

def check_finding(finding: AgentFinding, evidence: list[Evidence], eligible: set[str],
                  rule_supported: set[str]) -> tuple[list[str], list[str]]:
    """-> (issues: invalid output, worth a re-analysis; conflicts: disagreement with the rules)."""
    ids = {e.evidence_id for e in evidence}
    issues = [f"cites unknown evidence_id {i.evidence_id!r}" for i in finding.interpretations
              if i.evidence_id not in ids]
    cited = set(re.findall(r"(?<![\w-])(?:TH|EN|AN)-\d+\b", finding.summary))
    issues += [f"summary cites unknown evidence_id {c!r}" for c in sorted(cited - ids)]
    issues += [f"supports {t}, which this agent cannot recommend" for t in finding.supported_types
               if t not in eligible]
    conflicts = []
    for t in sorted(set(finding.supported_types) & eligible - rule_supported):
        conflicts.append(f"agent supports {t} but its deterministic evidence requirements are not met")
    for t in sorted(rule_supported - set(finding.supported_types)):
        conflicts.append(f"deterministic rules support {t} but the agent does not")
    return issues, conflicts


# ------------------------------------------------------------------ candidates

def _stale(ctx) -> list[str]:
    s, reasons = ctx.snapshot, []
    rule_ids = {f.rule_id for f in s.findings}
    if s.data_quality.status == "INSUFFICIENT" or s.as_of is None:
        reasons.append("telemetry is insufficient for this window")
    if "DQ_STALE_ASSET" in rule_ids:
        reasons.append("this asset's telemetry is stale")
    if "DQ_PIPELINE_STALE" in rule_ids and ctx.as_of_requested is None:
        reasons.append("live analysis, but the telemetry pipeline has stopped delivering data")
    return reasons


def _model_output_problems(ctx, c: Candidate) -> list[str]:
    f = ctx.forecast
    if not c.uses_model_prediction or f is None:
        return []
    problems = []
    horizons = set((ctx.model_summary or {}).get("horizons_minutes") or [])
    for fc in f["forecasts"]:
        v = fc["predicted_inlet_temperature_c"]
        if not (isinstance(v, (int, float)) and math.isfinite(v)):
            problems.append(f"non-finite forecast at {fc['horizon_minutes']} min")
        if fc["horizon_minutes"] not in horizons:
            problems.append(f"forecast horizon {fc['horizon_minutes']} min is not a trained horizon")
    if f["extrapolated_features"]:
        problems.append("forecast inputs are outside the training range (out-of-distribution)")
    return problems


def _uncertainty(ctx, c: Candidate, status: str, limitations: list[str]) -> Uncertainty:
    if status == "INCONCLUSIVE":
        return Uncertainty(level="UNKNOWN", basis="inconclusive: required evidence is missing", limitations=limitations)
    skill = (ctx.model_summary or {}).get("test_rmse_skill_vs_persistence") or {}
    weak_model = c.uses_model_prediction and any(v is not None and v <= 0 for v in skill.values())
    if ctx.snapshot.data_quality.status != "OK":
        return Uncertainty(level="HIGH", basis=f"telemetry data quality is {ctx.snapshot.data_quality.status}",
                           limitations=limitations)
    if weak_model:
        return Uncertainty(level="HIGH", basis="relies on a forecast that does not beat persistence offline",
                           limitations=limitations)
    if c.inference_based or c.uses_model_prediction:
        return Uncertainty(level="MEDIUM", basis="relies on trend/projection inferences or model predictions",
                           limitations=limitations)
    return Uncertainty(level="LOW", basis="direct observations with good data quality", limitations=limitations)


def validate_candidates(ctx, candidates: list[Candidate], evidence: dict[str, list[Evidence]],
                        agent_results: dict[str, dict]) -> list[dict]:
    """Apply every constraint; returns recommendation dicts (without ids/explanations)."""
    all_evidence = {e.evidence_id: e for items in evidence.values() for e in items}
    lim = ctx.cooling_config.limits
    stale = _stale(ctx)
    out = []
    for c in candidates:
        checked, warnings, missing = [], list(c.warnings), list(c.missing_evidence)
        status = c.status
        owner = TYPE_OWNER[c.type]
        result = agent_results.get(owner) or {"status": "NOT_RUN"}

        checked.append("telemetry freshness and completeness")
        if stale and c.type != "SENSOR_DATA_QUALITY":
            status = "INCONCLUSIVE"
            missing += stale
        if ctx.as_of_requested is not None and any(f.rule_id == "DQ_PIPELINE_STALE" for f in ctx.snapshot.findings):
            warnings.append("historical analysis: the pipeline is not delivering new data (wall clock)")

        if c.type in THERMAL_TYPES:
            checked.append("configured inlet limit present")
            if lim.inlet_temperature_high_c is None:
                status = "INCONCLUSIVE"
                missing.append("configured inlet temperature limit")
        checked.append("model outputs finite, at trained horizons and in the training range")
        problems = _model_output_problems(ctx, c)
        if problems:
            status = "INCONCLUSIVE"
            missing += problems

        checked.append("candidate setting from a validated, feasible, in-support optimizer result only")
        if c.candidate_setting is not None:
            o = ctx.optimizer or {}
            feasible = [x for x in o.get("candidates", []) if x["setting"] == c.candidate_setting and x["feasible"]
                        and x["within_bounds"] and x["within_supported_range"]]
            if o.get("status") != "RECOMMENDATION_AVAILABLE" or not feasible or \
                    (ctx.model_summary or {}).get("label") != "intervention_validated":
                warnings.append("candidate setting rejected: not a validated, feasible, in-support optimizer result")
                c.candidate_setting = None
                status = "INCONCLUSIVE"

        checked.append("evidence units valid and evidence present")
        ev = [all_evidence[i] for i in c.evidence_ids if i in all_evidence]
        bad_units = [e.evidence_id for e in ev if e.unit not in ALLOWED_UNITS]
        if bad_units:
            status = "INCONCLUSIVE"
            missing.append(f"evidence with invalid units: {bad_units}")
        if status == "REQUIRES_REVIEW" and not any(e.kind in ("measured", "rule_finding", "derived") for e in ev):
            status = "INCONCLUSIVE"
            missing.append("at least one measured observation")

        checked.append(f"{owner} agent completed and agrees with the rules")
        if result["status"] == "FAILED":
            status = "INCONCLUSIVE"
            missing.append(f"{owner} analysis (agent failed: {result.get('error')})")
        elif result["status"] == "NOT_RUN":
            status = "INCONCLUSIVE"
            missing.append(f"{owner} analysis (not run)")
        type_conflicts = [x for x in result.get("conflicts", []) if c.type in x]
        if type_conflicts:
            status = "INCONCLUSIVE"
            missing += [f"agreement between the {owner} agent and the deterministic rules ({x})"
                        for x in type_conflicts]
        if result.get("issues"):
            warnings.append(f"{owner} agent output failed validation: " + "; ".join(result["issues"]))

        limitations = ["Telemetry is simulated; no physical validation.",
                       "No automatic control: an operator must approve any real-world test."]
        if c.relative_energy:
            limitations += c.relative_energy["limitations"]
        if c.uses_model_prediction:
            limitations.append("Forecasts are under observed conditions; the response to cooling settings is not "
                               "validated." if (ctx.model_summary or {}).get("label") != "intervention_validated"
                               else "Counterfactual estimates come from an observationally validated model.")
        finding = result.get("finding")
        out.append({
            "type": c.type, "status": status, "severity": c.severity, "title": c.title,
            "recommended_action": c.recommended_action, "evidence": ev,
            "current_setting": c.current_setting, "candidate_setting": c.candidate_setting,
            "setting_name": c.setting_name, "predicted_temperature": c.predicted_temperature,
            "temperature_horizon_minutes": c.temperature_horizon_minutes,
            "estimated_energy_impact": None, "energy_unit": None,
            "energy_impact_status": (
                f"RELATIVE ONLY: fan {c.relative_energy['excess_fraction']:+.1%} vs peer median; affinity-law fan "
                f"power ratio {c.relative_energy['relative_fan_power']:.2f} (no kW: no measured cooling power)")
            if c.relative_energy else "UNAVAILABLE: no measured cooling power or validated energy model",
            "relative_energy_indicator": c.relative_energy,
            "uncertainty": _uncertainty(ctx, c, status, limitations),
            "conditions_that_invalidate_recommendation": c.invalidation,
            "constraints_checked": checked, "validation_warnings": list(dict.fromkeys(warnings)),
            "missing_evidence": list(dict.fromkeys(missing)),
            "hypotheses": list(dict.fromkeys([*c.hypotheses, *((finding.hypotheses if finding else []))])),
            "agent_assessment": finding.summary if finding and not result.get("issues") else None,
        })
    return out


def rank(recommendations: list[dict]) -> list[dict]:
    return sorted(recommendations, key=lambda r: (r["status"] != "REQUIRES_REVIEW", -SEVERITY_RANK[r["severity"]],
                                                  TYPE_PRIORITY.index(r["type"])))


# ------------------------------------------------------------------ explanations

_NUMBER = re.compile(r"(?<![\w.])-?\d+(?:\.\d+)?")
# Dates and clock times (ISO timestamps from the evidence) and evidence ids are not quantities.
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?)?"
                        r"|\b\d{1,2}:\d{2}(?::\d{2})?\b|\b(?:TH|EN|AN)-\d+\b|\b[A-Z]+-[A-Z]+-\d+\b"
                        r"|\bsim-[0-9a-f]+\b")
_EXECUTION_CLAIMS = re.compile(
    r"\b(has been|have been|was|were|is being|we have|i have|automatically)\s+(executed|applied|adjusted|changed|"
    r"reduced|increased|lowered|raised|set)\b", re.IGNORECASE)


def _allowed_numbers(recommendations: list[dict], ctx) -> list[float]:
    vals = [float(ctx.snapshot.window_minutes), float(ctx.cooling_config.recent_minutes)]
    for r in recommendations:
        for e in r["evidence"]:
            if e.observed_value is not None:
                vals.append(e.observed_value)
            for v in e.details.values():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    vals.append(float(v))
        # numbers that appear in the textual facts the LLM was given are not invented
        texts = [r.get("title", ""), r.get("recommended_action", ""), *r.get("missing_evidence", []),
                 *r.get("validation_warnings", []), *r.get("conditions_that_invalidate_recommendation", []),
                 *(e.interpretation for e in r["evidence"])]
        vals += [float(x) for t in texts for x in _NUMBER.findall(_TIMESTAMP.sub(" ", t))]
        for k in ("current_setting", "candidate_setting", "predicted_temperature", "temperature_horizon_minutes"):
            if r.get(k) is not None:
                vals.append(float(r[k]))
    for v in vars(ctx.cooling_config.limits).values():
        if v is not None:
            vals.append(float(v))
    return vals


def _number_ok(x: float, allowed: list[float]) -> bool:
    if abs(x) <= 10 and float(x).is_integer():          # counts and ordinals ("two windows", "step 1")
        return True
    for a in allowed:
        for candidate in (a, a * 100):                     # fractions quoted as percentages
            if abs(x - candidate) <= max(0.051, abs(candidate) * 0.005):
                return True
    return False


def check_explanation(explanation: Explanation, recommendations: list[dict], ctx) -> list[str]:
    issues = []
    ids = {r["recommendation_id"] for r in recommendations}
    got = {i.recommendation_id for i in explanation.items}
    if got != ids:
        issues.append(f"explains {sorted(got)} but the validated recommendations are {sorted(ids)}")
    allowed = _allowed_numbers(recommendations, ctx)
    texts = [explanation.overall_summary]
    for item in explanation.items:
        texts += [item.what_to_evaluate, item.why, item.energy_impact, item.uncertainty, item.why_human_review,
                  *item.measured_observations, *item.model_predictions, *item.invalidation_conditions,
                  *item.missing_evidence]
    for text in texts:
        for m in _EXECUTION_CLAIMS.finditer(text):
            before = text[max(0, m.start() - 25):m.start()].lower()
            if not re.search(r"\b(not|no|never|nothing)\b", before + m.group(0).lower()):
                issues.append(f"claims an action was carried out: {m.group(0)!r}")
        for raw in _NUMBER.findall(_TIMESTAMP.sub(" ", text)):
            if not _number_ok(float(raw), allowed):
                issues.append(f"number {raw} does not appear in the evidence")
    return list(dict.fromkeys(issues))
