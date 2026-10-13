"""Recommendation service: runs the LangGraph workflow and reads persisted results.

The engine is built once by the lifespan (LLM clients included); each analysis opens its own
short-lived store connections through the StoreFactory.
"""

import uuid
from datetime import datetime, timezone

from app.common.logging import get_logger
from app.exceptions import DatabaseUnavailableError, ServiceUnavailableError
from app.recommendations.exceptions import AnalysisNotFoundError, RecommendationNotFoundError
from app.common.pagination import decode_cursor, encode_cursor
from app.common.security.auth import AssetScope
from app.recommendations.exceptions import AlertNotFoundError
from app.recommendations.schemas import AlertQuery, AnalyzeRequest, DecisionRequest, ListQuery
from app.telemetry.exceptions import AssetNotFoundError, RunNotFoundError
from data_engineering.cooling.estimator import UnknownScopeError
from data_engineering.recommendations.config import LLMSettings
from data_engineering.recommendations.graph import RecommendationEngine
from data_engineering.storage.base import SqlTelemetryStore

log = get_logger("recommendations")


def _iso(value) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value)


class RecommendationService:
    def __init__(self, store: SqlTelemetryStore | None, engine: RecommendationEngine, llm_settings: LLMSettings,
                 model_registry, scope: AssetScope | None = None) -> None:
        self.store, self.engine, self.llm_settings, self.registry = store, engine, llm_settings, model_registry
        self.scope = scope or AssetScope([])

    def analyze(self, request: AnalyzeRequest) -> dict:
        self.scope.check(request.asset_id)
        try:
            return self.engine.analyze(request.asset_id, request.run_id, request.as_of)
        except UnknownScopeError as exc:
            raise (AssetNotFoundError if exc.kind == "asset" else RunNotFoundError)(str(exc)) from exc
        except (DatabaseUnavailableError, ServiceUnavailableError):
            raise
        except Exception as exc:          # driver errors; details are logged, not returned
            log.error("recommendation analysis failed", exc_info=exc)
            raise DatabaseUnavailableError("Recommendation analysis failed while reading or writing the store") \
                from exc

    def _review_status(self, recommendation_id: str) -> tuple[str, list[dict]]:
        decisions = self.store.decisions(recommendation_id)
        return (decisions[-1]["decision"] if decisions else "PENDING_REVIEW"), decisions

    def _page(self, fetch, query, id_key):
        """Keyset pagination with the operator scope applied (rows outside the scope are skipped)."""
        self.scope.check(query.asset_id)
        cursor, out = decode_cursor(query.cursor), []
        while len(out) < query.limit:
            rows = fetch(cursor, query.limit + 1)
            out += [row for row in rows if self.scope.permits(row["asset_id"])]
            if len(rows) < query.limit + 1:
                return out[:query.limit], None
            cursor = (rows[-1]["created_at"], rows[-1][id_key])
        page = out[:query.limit]
        return page, encode_cursor(page[-1]["created_at"], page[-1][id_key])

    def list(self, query: ListQuery) -> dict:
        rows, next_cursor = self._page(lambda cur, n: self.store.list_recommendations(
            simulation_run_id=query.run_id, asset_id=query.asset_id, recommendation_type=query.type,
            status=query.status, severity=query.severity, start=query.start_time, end=query.end_time,
            cursor=cur, limit=n), query, "recommendation_id")
        items = []
        for row in rows:
            p = row["payload_json"]
            items.append({"recommendation_id": row["recommendation_id"], "analysis_id": row["analysis_id"],
                          "asset_id": row["asset_id"], "simulation_run_id": row["simulation_run_id"],
                          "type": p["type"], "status": p["status"], "severity": p["severity"], "title": p["title"],
                          "observation_window_end": p["observation_window_end"], "created_at": _iso(row["created_at"]),
                          "review_status": self._review_status(row["recommendation_id"])[0]})
        return {"items": items, "count": len(items), "next_cursor": next_cursor}

    def alerts(self, query: AlertQuery) -> dict:
        rows, next_cursor = self._page(lambda cur, n: self.store.list_alerts(
            simulation_run_id=query.run_id, asset_id=query.asset_id, rule_id=query.rule_id, severity=query.severity,
            start=query.start_time, end=query.end_time, cursor=cur, limit=n), query, "alert_id")
        return {"items": [{**r["payload_json"], "created_at": _iso(r["created_at"])} for r in rows],
                "count": len(rows), "next_cursor": next_cursor}

    def alert(self, alert_id: str) -> dict:
        row = self.store.get_alert(alert_id)
        if row is None or not self.scope.permits(row["asset_id"]):
            raise AlertNotFoundError(f"alert {alert_id!r} not found")
        return {**row["payload_json"], "created_at": _iso(row["created_at"])}

    def detail(self, recommendation_id: str) -> dict:
        row = self.store.get_recommendation(recommendation_id)
        if row is None or not self.scope.permits(row["asset_id"]):
            raise RecommendationNotFoundError(f"recommendation {recommendation_id!r} not found")
        status, decisions = self._review_status(recommendation_id)
        return {"recommendation": row["payload_json"], "review_status": status, "created_at": _iso(row["created_at"]),
                "decisions": [{**{k: d[k] for k in ("decision_id", "decision", "reviewer", "note")},
                               "decided_at": _iso(d["decided_at"])} for d in decisions]}

    def analysis(self, analysis_id: str) -> dict:
        row = self.store.get_analysis(analysis_id)
        if row is None or not self.scope.permits(row["asset_id"]):
            raise AnalysisNotFoundError(f"analysis {analysis_id!r} not found")
        return {"analysis_id": analysis_id, "asset_id": row["asset_id"], "simulation_run_id": row["simulation_run_id"],
                "status": row["status"], "created_at": _iso(row["created_at"]), "execution": row["trace_json"]}

    def decide(self, recommendation_id: str, request: DecisionRequest) -> dict:
        row = self.store.get_recommendation(recommendation_id)
        if row is None or not self.scope.permits(row["asset_id"]):
            raise RecommendationNotFoundError(f"recommendation {recommendation_id!r} not found")
        decision = {"decision_id": f"dec-{uuid.uuid4().hex[:20]}", "recommendation_id": recommendation_id,
                    "decision": request.decision, "reviewer": request.reviewer, "note": request.note,
                    "decided_at": datetime.now(timezone.utc)}
        self.store.add_decision(decision)
        log.info("recommendation decision recorded", extra={"context": {
            "recommendation_id": recommendation_id, "decision": request.decision}})
        return {**decision, "decided_at": _iso(decision["decided_at"])}

    def status(self) -> dict:
        s, model = self.llm_settings, self.registry.model
        return {"mode": self.engine.mode, "llm_enabled": s.agents_llm_enabled,
                "api_key_configured": s.fireworks_api_key is not None,
                "main_model": s.fireworks_main_model, "subagent_model": s.fireworks_subagent_model,
                "reasoning_effort": s.fireworks_reasoning_effort, "timeout_s": s.fireworks_timeout_s,
                "cooling_model": {"version": model.version, "label": model.label} if model else None}
