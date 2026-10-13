"""Controlled, read-only copilot tools (predefined query functions; no SQL from the LLM).

Each tool opens its own READ-ONLY store connection (SQLite mode=ro + query_only, PostgreSQL
default_transaction_read_only) with the configured statement timeout, enforces the operator's asset
scope, bounds assets and time ranges, and returns typed evidence:
  measured     - a telemetry value as stored
  calculated   - computed here (mean, trend, direction, ranking) from measured values
  prediction   - XGBoost output (under observed conditions)
  hypothesis   - a specialist's suggested cause (not established)
  recommendation / configuration / unavailable
There is no tool that writes data or commands equipment.
"""

from collections import Counter
from datetime import datetime

from data_engineering import telemetry_query as tq
from data_engineering.cooling.estimator import CoolingStateEstimator
from data_engineering.cooling_model.optimizer import recommend
from data_engineering.cooling_model.predictor import InsufficientInferenceDataError
from data_engineering.energy import fan_peer
from data_engineering.recommendations import evidence as rec_evidence
from data_engineering.recommendations.llm import LLMError
from data_engineering.recommendations.specialists import run_specialist

KIND = {"measured": "measured", "derived": "calculated", "model_prediction": "prediction",
        "configuration": "configuration", "rule_finding": "calculated", "unavailable": "unavailable"}
SUMMARY_METRICS = ("inlet_temperature_c", "exhaust_temperature_c", "workload_intensity", "cpu_utilization",
                   "gpu_utilization", "fan_speed_rpm", "airflow_cfm", "power_consumption_kw", "pue")
SEVERITY_RANK = {"CRITICAL": 3, "WARNING": 2, "INFO": 1}


def item(kind, metric, value=None, unit=None, asset_id=None, window=None, source="telemetry", note=None) -> dict:
    if isinstance(value, float):
        value = round(value, 4)
    return {"kind": kind, "metric": metric, "value": value, "unit": unit, "asset_id": asset_id, "window": window,
            "source": source, "note": note}


def result(tool, status="OK", evidence=None, warnings=None, error=None, **extra) -> dict:
    return {"tool": tool, "status": status, "evidence": evidence or [], "warnings": warnings or [], "error": error,
            **extra}


class CopilotTools:
    def __init__(self, *, store_factory, scope, cooling_config, model_provider, model_availability, model_settings,
                 workflow, sub_llm=None, max_assets: int = 25, max_points: int = 5000) -> None:
        self.store_factory, self.scope, self.cooling_config = store_factory, scope, cooling_config
        self.model_provider, self.model_availability = model_provider, model_availability
        self.model_settings, self.workflow, self.sub_llm = model_settings, workflow, sub_llm
        self.max_assets, self.max_points = max_assets, max_points

    def _open(self):
        return self.store_factory()       # read-only connection (see module docstring)

    def _assets(self, store, run_id, asset_ids):
        known = {a["asset_id"]: a for a in store.run_assets(run_id) if self.scope.permits(a["asset_id"])}
        chosen = [a for a in asset_ids if a in known] if asset_ids else sorted(known)
        return {a: known[a] for a in chosen[:self.max_assets]}, len(chosen) > self.max_assets

    @staticmethod
    def _win(start, end):
        return f"({tq.iso(start)}, {tq.iso(end)}]"

    # ------------------------------------------------------------------ telemetry
    def latest(self, run_id, asset_ids, end: datetime) -> dict:
        store = self._open()
        try:
            assets, truncated = self._assets(store, run_id, asset_ids)
            latest = tq.latest_per_asset(store, run_id, end, assets)
        finally:
            store.close()
        ev, warnings = [], []
        for aid, a in assets.items():
            r = latest.get(aid)
            if r is None:
                ev.append(item("unavailable", "latest_reading", asset_id=aid, note="no observed reading"))
                continue
            age = (end - r["event_timestamp"]).total_seconds()
            if age > self.cooling_config.stale_after_seconds:
                warnings.append(f"{aid}: latest reading is {age:.0f} s older than the window end (stale)")
            for m in SUMMARY_METRICS:
                v = tq.value(r, m, a.get("asset_type"))
                if v is None and m == "gpu_utilization":
                    continue
                ev.append(item("measured" if v is not None else "unavailable", m, v, tq.UNITS[m], aid,
                               f"reading at {tq.iso(r['event_timestamp'])}", note=tq.METRIC_NOTES.get(m)))
        if truncated:
            warnings.append(f"only the first {self.max_assets} assets were read")
        return result("latest_telemetry", evidence=ev, warnings=warnings)

    def window_summary(self, run_id, asset_ids, start, end, metrics=SUMMARY_METRICS) -> dict:
        store = self._open()
        try:
            assets, truncated = self._assets(store, run_id, asset_ids)
            per_asset = {aid: tq.asset_window(store, run_id, a, start, end, list(metrics), "raw", self.max_points)
                         for aid, a in assets.items()}
        finally:
            store.close()
        ev, table, warnings = [], [], []
        win = self._win(start, end)
        for aid, s in per_asset.items():
            if s["readings"] == 0:
                ev.append(item("unavailable", "telemetry", asset_id=aid, window=win, note="no readings in the window"))
                continue
            if s["missing_readings"]:
                warnings.append(f"{aid}: {s['missing_readings']} missing reading(s) in the window")
            row = {"asset_id": aid, "readings": s["readings"]}
            for m in metrics:
                pts = [(datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00")), p[m]) for p in s["points"]
                       if p[m] is not None]
                if not pts:
                    continue
                vals = [v for _, v in pts]
                mean, slope = sum(vals) / len(vals), tq.trend_per_minute(pts)
                row[m] = {"mean": round(mean, 4), "min": round(min(vals), 4), "max": round(max(vals), 4),
                          "trend_per_minute": slope, "unit": tq.UNITS[m]}
                ev.append(item("calculated", f"{m}.mean", mean, tq.UNITS[m], aid, win, "telemetry_query",
                               f"mean of {len(vals)} observed readings"))
                if slope is not None and m in ("inlet_temperature_c", "workload_intensity", "fan_speed_rpm",
                                               "power_consumption_kw", "exhaust_temperature_c"):
                    ev.append(item("calculated", f"{m}.trend_per_minute", slope, f"{tq.UNITS[m]}/min", aid, win,
                                   "telemetry_query", "least-squares slope"))
            table.append(row)
        if truncated:
            warnings.append(f"only the first {self.max_assets} assets were read")
        return result("window_summary", evidence=ev, warnings=warnings, table=table)

    def trend_screen(self, run_id, asset_ids, start, end) -> dict:
        """Per asset: inlet direction and workload direction from the estimator's trend rules over the window."""
        minutes = max(1, min(1440, int((end - start).total_seconds() // 60)))
        store = self._open()
        try:
            assets, _ = self._assets(store, run_id, asset_ids)
            est = CoolingStateEstimator(store, self.cooling_config)
            snaps = {aid: est.evaluate(aid, run_id, minutes, end) for aid in assets}
        finally:
            store.close()
        ev, table, win = [], [], self._win(start, end)
        for aid, s in snaps.items():
            inlet = s.metrics.get("inlet_temperature_c")
            row = {"asset_id": aid, "inlet_direction": inlet.direction if inlet else None,
                   "inlet_trend_per_minute": inlet.trend_per_minute if inlet else None,
                   "workload_direction": s.workload_direction, "state": s.state,
                   "rising_inlet_with_stable_workload": bool(inlet and inlet.direction == "increasing"
                                                             and s.workload_direction == "stable")}
            table.append(row)
            if inlet and inlet.trend_per_minute is not None:
                ev.append(item("calculated", "inlet_temperature_c.trend_per_minute", inlet.trend_per_minute,
                               "degC/min", aid, win, "cooling_state_estimator", f"direction: {inlet.direction}"))
            ev.append(item("calculated", "workload_direction", s.workload_direction, None, aid, win,
                           "cooling_state_estimator"))
        flagged = [r["asset_id"] for r in table if r["rising_inlet_with_stable_workload"]]
        ev.append(item("calculated", "assets_rising_inlet_with_stable_workload", len(flagged), "count", None, win,
                       "cooling_state_estimator", ", ".join(flagged) or "none"))
        return result("trend_screen", evidence=ev, table=table,
                      warnings=["direction uses the estimator's trend sensitivity (config/cooling_state.yaml "
                                "trend_min_change), an analysis heuristic"])

    def energy(self, run_id, asset_ids, start, end) -> dict:
        r = self.window_summary(run_id, asset_ids, start, end, ("power_consumption_kw", "pue"))
        r["tool"] = "energy_analysis"
        column = self.model_settings.optimizer.measured_cooling_power_column
        r["evidence"].append(item("unavailable", "cooling_power_kw", None, "kW", None, None, "configuration",
                                  tq.UNAVAILABLE_MEASUREMENTS["cooling_power_kw"] if column is None
                                  else f"configured column {column!r} without a validated energy model"))
        r["warnings"].append("power_consumption_kw is per-server IT power: it is not cooling power, so cooling-energy "
                             "waste or savings cannot be quantified in kW from this telemetry")
        store = self._open()
        try:
            assets, _ = self._assets(store, run_id, asset_ids)
            peer = fan_peer.analyze(store, run_id, end, self.workflow.fan_peer, sorted(assets))
        finally:
            store.close()
        r["fan_peer"] = peer["results"]
        r["table"] = [{k: x.get(k) for k in ("asset_id", "status", "asset_fan_rpm", "peer_median_fan_rpm",
                                             "excess_fraction", "relative_fan_power", "reference_windows")}
                      for x in peer["results"]]
        for x in peer["results"]:
            win = f"window ending {x['window']['end']}" if x.get("window") else None
            r["evidence"].append(item("calculated", "fan_peer_comparison.status", x["status"], None, x["asset_id"], win,
                                      "energy.fan_peer", "; ".join(x["reasons"]) or None))
            if x.get("excess_fraction") is not None:
                r["evidence"].append(item("calculated", "fan_speed_rpm.excess_vs_peers", x["excess_fraction"],
                                          "fraction", x["asset_id"], win, "energy.fan_peer",
                                          f"fan {x['asset_fan_rpm']:.0f} rpm vs peer median "
                                          f"{x['peer_median_fan_rpm']:.0f} rpm"))
                r["evidence"].append(item("calculated", "fan_power.relative_to_peers", x["relative_fan_power"],
                                          "ratio", x["asset_id"], win, "energy.fan_peer",
                                          "affinity-law ratio (rpm/peer)^3; relative only, not kW"))
        r["warnings"] += peer["limitations"][:2]
        return r

    # ------------------------------------------------------------------ models
    def forecast(self, run_id, asset_ids, end) -> dict:
        model, blocked = self.model_provider(), self.model_availability()
        if blocked:
            return result("xgboost_forecast", "UNAVAILABLE", [item("unavailable", "inlet_forecast",
                          source="cooling_response_model", note="; ".join(r["code"] for r in blocked))],
                          error=blocked[0]["detail"])
        ev, preds, warnings = [], [], []
        store = self._open()
        try:
            assets, _ = self._assets(store, run_id, asset_ids[:3])
            for aid in assets:
                try:
                    p = model.predict_for_asset(store, run_id, aid, end)
                except InsufficientInferenceDataError as exc:
                    ev.append(item("unavailable", "inlet_forecast", asset_id=aid, source="cooling_response_model",
                                   note=str(exc)))
                    continue
                skill = {h: e["test"]["rmse_skill_vs_persistence"] for h, e in model.metadata["evaluation"].items()}
                for f in p["forecasts"]:
                    h = f["horizon_minutes"]
                    ev.append(item("prediction", f"inlet_forecast_{h}min", f["predicted_inlet_temperature_c"], "degC",
                                   aid, f"for {f['forecast_timestamp']}", "xgboost",
                                   f"model {p['model_version']} ({p['model_label']}); offline test RMSE "
                                   f"{f['offline_test_rmse_c']:.3f} degC; skill vs persistence {skill[str(h)]:+.1%}"))
                    preds.append({"asset_id": aid, **{k: f[k] for k in ("horizon_minutes", "forecast_timestamp",
                                                                       "predicted_inlet_temperature_c",
                                                                       "persistence_inlet_temperature_c",
                                                                       "offline_test_rmse_c")},
                                  "model_version": p["model_version"], "model_label": p["model_label"]})
                warnings += [f"{aid}: {w}" for w in p["model_validity_warnings"][-1:]]
                if p["data_quality"]["status"] != "OK":
                    warnings.append(f"{aid}: forecast inputs degraded ({'; '.join(p['data_quality']['notes'])})")
        finally:
            store.close()
        return result("xgboost_forecast", evidence=ev, predictions=preds, warnings=warnings)

    def cooling_candidates(self, run_id, asset_id, candidates, end) -> dict:
        model, blocked = self.model_provider(), self.model_availability()
        store = self._open()
        try:
            out = recommend(None if blocked else model, store, run_id, asset_id, self.model_settings.optimizer, end,
                            candidates=candidates or None)
        finally:
            store.close()
        ev = [item("unavailable" if out["status"] == "UNAVAILABLE" else "calculated", "cooling_optimizer.status",
                   out["status"], None, asset_id, None, "cooling_optimizer",
                   "; ".join(r["code"] for r in out["reasons"]) or None)]
        warnings = [] if out["status"] == "RECOMMENDATION_AVAILABLE" else [
            "counterfactual fan-speed effects and cooling energy are not validated: no setting can be recommended"]
        return result("cooling_candidate_evaluation", evidence=ev, optimizer=out, warnings=warnings)

    # ------------------------------------------------------------------ recommendations / priorities
    def recommendations(self, run_id, asset_ids, limit=10) -> dict:
        store = self._open()
        try:
            rows = []
            for aid in (asset_ids or [None]):
                rows += store.list_recommendations(simulation_run_id=run_id, asset_id=aid, limit=limit)
            alerts = []
            for aid in (asset_ids or [None]):
                alerts += store.list_alerts(simulation_run_id=run_id, asset_id=aid, limit=limit)
        finally:
            store.close()
        rows = [r for r in rows if self.scope.permits(r["asset_id"])][:limit]
        alerts = [a for a in alerts if self.scope.permits(a["asset_id"])][:limit]
        recs, ev = [], []
        for r in rows:
            p = r["payload_json"]
            recs.append({"recommendation_id": r["recommendation_id"], "asset_id": r["asset_id"], "type": p["type"],
                         "status": p["status"], "severity": p["severity"], "title": p["title"],
                         "observation_window_end": p["observation_window_end"],
                         "missing_evidence": p["missing_evidence"][:3],
                         "link": f"/api/v1/recommendations/{r['recommendation_id']}"})
            ev.append(item("recommendation", p["type"], p["status"], None, r["asset_id"],
                           f"window ending {p['observation_window_end']}", "recommendations",
                           f"{p['title']} [{r['recommendation_id']}]"))
        for a in alerts:
            p = a["payload_json"]
            ev.append(item("calculated", f"alert.{p['rule_id']}", p["severity"], None, a["asset_id"],
                           f"window ending {p['observation_window_end']}", "alerts", p["explanation"]))
        warnings = [] if rows else ["no stored recommendations for this scope; generate them with "
                                    "POST /api/v1/recommendations/generate"]
        return result("recommendations", evidence=ev, recommendations=recs, warnings=warnings)

    def maintenance_priority(self, run_id, asset_ids, end) -> dict:
        """Deterministic ranking: current estimator severity, then stored alerts/recommendations."""
        store = self._open()
        try:
            assets, _ = self._assets(store, run_id, asset_ids)
            est = CoolingStateEstimator(store, self.cooling_config)
            snaps = {aid: est.evaluate(aid, run_id, None, end) for aid in assets}
            alerts = Counter(a["asset_id"] for a in store.list_alerts(simulation_run_id=run_id, limit=1000))
            recs = Counter(r["asset_id"] for r in store.list_recommendations(simulation_run_id=run_id,
                                                                             status="REQUIRES_REVIEW", limit=1000))
        finally:
            store.close()
        def asset_severity(snap):        # asset-level findings only (not the pipeline-wide wall-clock check)
            levels = [f.severity for f in snap.findings if f.rule_id != "DQ_PIPELINE_STALE"]
            return max(levels, key=SEVERITY_RANK.get) if levels else "INFO"
        table = sorted(({"asset_id": aid, "state": s.state, "severity": asset_severity(s),
                         # pipeline-wide wall-clock staleness is not a property of one asset
                         "findings": [f.rule_id for f in s.findings if f.severity != "INFO"
                                      and f.rule_id != "DQ_PIPELINE_STALE"],
                         "stored_alerts": alerts.get(aid, 0), "open_recommendations": recs.get(aid, 0)}
                        for aid, s in snaps.items()),
                       key=lambda r: (-SEVERITY_RANK[r["severity"]], -len(r["findings"]), -r["stored_alerts"],
                                      -r["open_recommendations"], r["asset_id"]))
        actionable = [r for r in table if r["severity"] != "INFO" or r["findings"] or r["stored_alerts"]
                      or r["open_recommendations"]]
        ev = [item("calculated", "priority_rank", i + 1, None, r["asset_id"], f"window ending {tq.iso(end)}",
                   "cooling_state_estimator", f"{r['severity']} {r['state']}; findings: {', '.join(r['findings']) or 'none'}")
              for i, r in enumerate(actionable[:10])]
        ev.append(item("calculated", "assets_needing_investigation", len(actionable), "count", None,
                       f"window ending {tq.iso(end)}", "cooling_state_estimator",
                       "assets with warning findings, stored alerts or open recommendations" if actionable else
                       f"none of the {len(table)} assets has warning findings, stored alerts or open recommendations"))
        return result("maintenance_priority", evidence=ev, table=actionable[:10],
                      warnings=["ranking = current rule severity, then number of warning findings, stored alerts and "
                                "open recommendations; it is a triage order, not a fault diagnosis"])

    # ------------------------------------------------------------------ specialists (subagent LLM)
    def specialists(self, run_id, asset_id, end, agents, question) -> dict:
        store = self._open()
        try:
            ctx = rec_evidence.gather_context(store, run_id, asset_id, end, self.cooling_config,
                                              None if self.model_availability() else self.model_provider(),
                                              self.model_settings, self.workflow)
        finally:
            store.close()
        builders = {"thermal": rec_evidence.thermal_evidence, "energy": rec_evidence.energy_evidence,
                    "anomaly": rec_evidence.anomaly_evidence, "workload": rec_evidence.workload_evidence}
        ev, findings, calls = [], [], []
        for agent in agents:
            items = builders[agent](ctx)
            ev += [item(KIND[e.kind], e.metric, e.observed_value if e.observed_value is not None else
                        (e.details.get("status") or None), e.unit, asset_id, e.observation_window,
                        f"{agent}_specialist_evidence", e.interpretation[:200]) for e in items]
            if self.sub_llm is None:
                findings.append({"agent": agent, "status": "DETERMINISTIC", "summary": None, "hypotheses": []})
                continue
            try:
                f, call = run_specialist(self.sub_llm, agent, {"asset_id": asset_id, "window": ctx.window}, items,
                                         question=question)
                calls.append(call.to_dict())
                findings.append({"agent": agent, "status": "OK", "summary": f.summary, "hypotheses": f.hypotheses,
                                 "missing_evidence": f.missing_evidence, "conclusion": f.conclusion})
                ev += [item("hypothesis", f"{agent}.hypothesis", None, None, asset_id, ctx.window,
                            f"{agent}_specialist", h) for h in f.hypotheses]
            except LLMError as exc:
                findings.append({"agent": agent, "status": "FAILED", "error": exc.kind, "summary": None,
                                 "hypotheses": []})
        warnings = [f"{x['agent']} specialist failed ({x['error']}); only deterministic evidence is used"
                    for x in findings if x["status"] == "FAILED"]
        return result("specialists", evidence=ev, findings=findings, calls=calls, warnings=warnings)
