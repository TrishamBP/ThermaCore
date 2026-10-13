"""Digital twin service: joins the layout with the existing telemetry, cooling-model, recommendation and
copilot services (service-layer calls, no HTTP between modules, no second model or agent implementation).

Value kinds in responses: "measured" (stored telemetry), "calculated" (derived here, e.g. delta-T or a
status), "aggregated" (from child servers, with the method), "predicted" (XGBoost, under observed conditions).
"""

from collections import Counter
from datetime import datetime, timedelta

from app.assets.schemas import TelemetryQuery
from app.assets.scope import resolve_run
from app.assets.service import AssetService
from app.common.security.auth import AssetScope
from app.cooling.schemas import CoolingPredictRequest
from app.cooling.service import CoolingModelService
from app.exceptions import AppError, InvalidRequestError, NotFoundError
from data_engineering import telemetry_query as tq
from data_engineering.cooling.estimator import CoolingStateEstimator
from data_engineering.digital_twin.layout import Layout
from data_engineering.energy import fan_peer

STATE_METRICS = ("inlet_temperature_c", "exhaust_temperature_c", "temperature_delta_c", "workload_intensity",
                 "cpu_utilization", "gpu_utilization", "fan_speed_rpm", "airflow_cfm", "power_consumption_kw", "pue")
OVERLAY_TYPES = ("temperature", "predicted_temperature", "workload", "thermal_risk", "energy", "fan_excess",
                 "airflow", "cooling_efficiency", "data_quality")
_OVERLAY_METRIC = {"temperature": "inlet_temperature_c", "workload": "workload_intensity",
                   "energy": "power_consumption_kw", "airflow": "airflow_cfm"}


class LayoutObjectNotFoundError(NotFoundError):
    code = "unknown_layout_object"


class LayoutUnavailableError(AppError):
    status_code, code, message = 503, "layout_not_configured", "No digital twin layout is configured"


def band(value, bands: list[dict] | None) -> str:
    if value is None:
        return "UNKNOWN"
    if not bands:
        return "UNBANDED"
    for b in bands:
        if b["max"] is None or value <= b["max"]:
            return b["status"]
    return "UNKNOWN"


class TwinService:
    def __init__(self, store, layout: Layout, settings, scope: AssetScope, cooling_config, registry=None,
                 copilot=None, fan_peer_config=None) -> None:
        self.store, self.layout, self.settings, self.scope = store, layout, settings, scope
        self.cooling_config, self.registry, self.copilot = cooling_config, registry, copilot
        self.fan_peer_config = fan_peer_config or fan_peer.FanPeerConfig()

    # ------------------------------------------------------------------ layout
    def _objects(self, room_id=None, include_servers=True, include_cooling=True, asset_type=None):
        if room_id is not None and (room_id not in self.layout.objects or self.layout.objects[room_id].type != "room"):
            raise LayoutObjectNotFoundError(f"room {room_id!r} not found in the layout")
        out = []
        for o in self.layout.objects.values():
            if room_id and o.type not in ("facility",) and self.layout.room_of(o.id) != room_id:
                continue
            if (o.type == "server" and not include_servers) or (o.type == "cooling_unit" and not include_cooling):
                continue
            if asset_type and o.type != asset_type:
                continue
            out.append(o)
        return out

    def _meta(self) -> dict:
        lay = self.layout
        return {"layout_version": lay.version, "revision": lay.revision, "demonstration": lay.demonstration,
                "units": lay.units, "source": lay.source, "generated_at": tq.iso(lay.loaded_at),
                "notice": ("DEMONSTRATION layout: rack membership, coordinates and dimensions are illustrative and do "
                           "not represent a verified physical facility.") if lay.demonstration else None}

    def layout_view(self, room_id, include_servers, include_cooling, known_version) -> dict:
        if known_version == self.layout.version:
            return {**self._meta(), "unchanged": True, "objects": []}
        run_assets = self._run_asset_ids(None)
        objs = self._objects(room_id, include_servers, include_cooling)
        return {**self._meta(), "unchanged": False, "count": len(objs),
                "objects": [o.to_dict(self.layout.version, bool(o.asset_id and o.asset_id in run_assets))
                            for o in objs]}

    def layout_object(self, object_id: str) -> dict:
        o = self.layout.objects.get(object_id)
        if o is None:
            raise LayoutObjectNotFoundError(f"layout object {object_id!r} not found")
        run_assets = self._run_asset_ids(None)
        return {**self._meta(), "object": o.to_dict(self.layout.version, bool(o.asset_id in run_assets)),
                "parents": [p.to_dict(self.layout.version, False) for p in self.layout.ancestors(object_id)],
                "children": [c.id for c in self.layout.children(object_id)]}

    # ------------------------------------------------------------------ live state
    def _run_asset_ids(self, run_id) -> dict[str, dict]:
        try:
            run_id = resolve_run(self.store, run_id)
        except AppError:
            return {}
        return {a["asset_id"]: a for a in self.store.run_assets(run_id)}

    def _thermal_status(self, inlet):
        lim = self.cooling_config.limits
        if inlet is None or lim.inlet_temperature_high_c is None:
            return "UNKNOWN"
        if inlet >= lim.inlet_temperature_high_c:
            return "ABOVE_LIMIT"
        if lim.inlet_approach_margin_c is not None and inlet >= lim.inlet_temperature_high_c - lim.inlet_approach_margin_c:
            return "APPROACHING_LIMIT"
        return "WITHIN_LIMIT"

    def _states(self, run_id, as_of=None, objects=None):
        run_id = resolve_run(self.store, run_id)
        assets = {a["asset_id"]: a for a in self.store.run_assets(run_id)}
        objects = objects if objects is not None else list(self.layout.objects.values())
        mapped = {o.asset_id: o for o in objects if o.asset_id}
        visible = {aid: assets[aid] for aid in mapped if aid in assets and self.scope.permits(aid)}
        reference = as_of or max((a["last_event_timestamp"] for a in assets.values()), default=None)
        latest = tq.latest_per_asset(self.store, run_id, as_of, visible)          # one query for all assets
        alerts = Counter(a["asset_id"] for a in self.store.list_alerts(simulation_run_id=run_id, limit=2000)
                         if reference is None or a["window_end"] >= reference - timedelta(
                             minutes=self.cooling_config.window_minutes))
        recs = Counter(r["asset_id"] for r in self.store.list_recommendations(simulation_run_id=run_id,
                                                                             status="REQUIRES_REVIEW", limit=2000))
        states = {}
        for aid, o in mapped.items():
            if aid not in assets:
                states[aid] = {"object_id": o.id, "asset_id": aid, "status": "NO_TELEMETRY", "values": {},
                               "last_telemetry_timestamp": None, "freshness": "NO_DATA", "thermal_status": "UNKNOWN",
                               "active_alerts": 0, "open_recommendations": 0}
                continue
            if aid not in visible:
                states[aid] = {"object_id": o.id, "asset_id": aid, "status": "RESTRICTED", "values": {},
                               "last_telemetry_timestamp": None, "freshness": "UNKNOWN", "thermal_status": "UNKNOWN",
                               "active_alerts": None, "open_recommendations": None}
                continue
            r, a = latest.get(aid), assets[aid]
            values = {}
            if r is not None:
                for m in STATE_METRICS:
                    v = tq.value(r, m, a.get("asset_type"))
                    values[m] = {"value": v, "unit": tq.UNITS[m],
                                 "kind": "calculated" if m == "temperature_delta_c" else "measured",
                                 **({"note": tq.METRIC_NOTES[m]} if m in tq.METRIC_NOTES else {})}
                values["cooling_efficiency"] = {"value": None, "unit": None, "kind": "unavailable",
                                                "note": tq.UNAVAILABLE_MEASUREMENTS["cooling_efficiency"]}
            age = (reference - r["event_timestamp"]).total_seconds() if r is not None and reference else None
            fresh = "NO_DATA" if r is None else ("FRESH" if age <= self.cooling_config.stale_after_seconds
                                                  else "STALE")
            inlet = values.get("inlet_temperature_c", {}).get("value")
            states[aid] = {"object_id": o.id, "asset_id": aid, "asset_type": a.get("asset_type"),
                           "status": "OK" if fresh == "FRESH" else fresh,
                           "last_telemetry_timestamp": tq.iso(r["event_timestamp"]) if r else None,
                           "age_seconds": age, "freshness": fresh, "values": values,
                           "thermal_status": self._thermal_status(inlet) if fresh == "FRESH" else "UNKNOWN",
                           "active_alerts": alerts.get(aid, 0), "open_recommendations": recs.get(aid, 0)}
        return run_id, reference, states

    def _aggregates(self, states):
        """Racks/aisles/rooms: aggregated from child servers that have fresh data (method stated)."""
        out = {}
        for o in self.layout.objects.values():
            if o.type not in ("rack", "aisle", "room", "facility"):
                continue
            kids = [states[c.asset_id] for c in self.layout.descendants(o.id) if c.asset_id in states
                    and states[c.asset_id]["freshness"] == "FRESH"]
            inlets = [k["values"]["inlet_temperature_c"]["value"] for k in kids
                      if k["values"].get("inlet_temperature_c", {}).get("value") is not None]
            work = [k["values"]["workload_intensity"]["value"] for k in kids
                    if k["values"].get("workload_intensity", {}).get("value") is not None]
            power = [k["values"]["power_consumption_kw"]["value"] for k in kids
                     if k["values"].get("power_consumption_kw", {}).get("value") is not None]
            out[o.id] = {"object_id": o.id, "type": o.type, "kind": "aggregated", "servers_reporting": len(kids),
                         "max_inlet_temperature_c": max(inlets) if inlets else None,
                         "mean_workload_intensity": round(sum(work) / len(work), 4) if work else None,
                         "sum_server_power_kw": round(sum(power), 3) if power else None,
                         "worst_thermal_status": max((k["thermal_status"] for k in kids), default="UNKNOWN",
                                                     key=["UNKNOWN", "WITHIN_LIMIT", "APPROACHING_LIMIT",
                                                          "ABOVE_LIMIT"].index),
                         "method": "max inlet / mean workload / sum of server IT power over child servers with "
                                   "fresh telemetry"}
        return out

    def state(self, run_id, room_id, asset_type, asset_ids, since, include_derived) -> dict:
        objs = self._objects(room_id, asset_type=asset_type)
        if asset_ids:
            wanted = set(asset_ids)
            objs = [o for o in objs if o.asset_id in wanted or o.id in wanted]
        run_id, reference, states = self._states(run_id, None, objs)
        items = list(states.values())
        if since is not None:
            items = [s for s in items if s["last_telemetry_timestamp"] and
                     datetime.fromisoformat(s["last_telemetry_timestamp"].replace("Z", "+00:00")) > since]
        newest = max((s["last_telemetry_timestamp"] for s in states.values() if s["last_telemetry_timestamp"]),
                     default=None)
        return {**self._meta(), "simulation_run_id": run_id, "reference_time": tq.iso(reference),
                "state_cursor": newest, "assets": items,
                "aggregates": list(self._aggregates(states).values()) if include_derived else [],
                "thermal_status_rule": "inlet vs config/cooling_state.yaml limits.inlet_temperature_high_c and "
                                       "inlet_approach_margin_c; UNKNOWN without a configured limit or fresh data",
                "freshness_rule": f"FRESH = latest observed reading within {self.cooling_config.stale_after_seconds} s "
                                  "(event time) of the run's newest reading"}

    # ------------------------------------------------------------------ overlays
    def overlays(self, overlay_type, run_id, room_id, asset_type, timestamp) -> dict:
        if overlay_type not in OVERLAY_TYPES:
            raise InvalidRequestError(f"unknown overlay type {overlay_type!r}; supported: {', '.join(OVERLAY_TYPES)}")
        objs = [o for o in self._objects(room_id, asset_type=asset_type) if o.asset_id]
        run_id, reference, states = self._states(run_id, timestamp, objs)
        bands = self.layout.overlay_bands.get(overlay_type if overlay_type != "predicted_temperature" else "temperature")
        risk, preds, warnings = {}, {}, []
        if overlay_type == "thermal_risk":
            est = CoolingStateEstimator(self.store, self.cooling_config)
            snap = est.evaluate(None, run_id, None, reference)
            risk = {a.asset_id: a for a in snap.assets}
        if overlay_type == "predicted_temperature":
            service = CoolingModelService(self.store, self.registry)
            for aid, s in states.items():
                if s["freshness"] != "FRESH":
                    continue
                p = service.predict_v2(CoolingPredictRequest(asset_id=aid, run_id=run_id, as_of=reference,
                                                             horizons_minutes=[max(self.registry.model.metadata[
                                                                 "horizons_minutes"])] if self.registry.model else None),
                                       self.settings.model_max_age_days)
                preds[aid] = p
            if preds and all(p["status"] == "UNAVAILABLE" for p in preds.values()):
                warnings.append("predictions unavailable: " + ", ".join(r["code"] for r in
                                                                       next(iter(preds.values()))["reasons"]))
        if overlay_type == "energy":
            warnings.append("energy overlay shows per-server IT power; it is not cooling power")
        peers = {}
        if overlay_type == "fan_excess" and reference is not None:
            peers = {x["asset_id"]: x for x in fan_peer.analyze(self.store, run_id, reference, self.fan_peer_config,
                                                                 [a for a in states if self.scope.permits(a)])["results"]}
            warnings.append("fan speed relative to comparable peers (energy-waste indicator); relative only, no kW")
        if overlay_type == "airflow":
            warnings.append("airflow overlay shows measured airflow magnitude per server; airflow direction and paths "
                            "are not in the telemetry and are not inferred")
        if overlay_type == "cooling_efficiency":
            warnings.append(tq.UNAVAILABLE_MEASUREMENTS["cooling_efficiency"])
        items = []
        for aid, s in states.items():
            o, value, unit, status, source, predicted, item_warnings = self.layout.by_asset()[aid], None, None, \
                "UNKNOWN", "telemetry", overlay_type == "predicted_temperature", []
            observed = s["last_telemetry_timestamp"]
            if s["status"] in ("RESTRICTED", "NO_TELEMETRY"):
                status = s["status"]
            elif overlay_type in _OVERLAY_METRIC:
                m = _OVERLAY_METRIC[overlay_type]
                value, unit = s["values"].get(m, {}).get("value"), tq.UNITS[m]
                status = band(value, bands) if s["freshness"] == "FRESH" else s["freshness"]
            elif overlay_type == "thermal_risk":
                a = risk.get(aid)
                value, status, source = (a.state if a else None), (a.severity if a else "UNKNOWN"), \
                    "cooling_state_estimator"
            elif overlay_type == "predicted_temperature":
                p = preds.get(aid)
                predicted, source = True, "xgboost"
                if p and p["status"] == "AVAILABLE" and p["forecasts"]:
                    f = p["forecasts"][-1]
                    value, unit, observed = f["predicted_inlet_temperature_c"], "degC", f["forecast_timestamp"]
                    status = band(value, bands)
                    item_warnings.append(f"model {p['model']['label']}; +{f['horizon_minutes']} min; offline RMSE "
                                         f"{f['evaluation']['offline_test_rmse_c']:.3f} degC")
                else:
                    status = "UNAVAILABLE"
            elif overlay_type == "fan_excess":
                x, source = peers.get(aid), "energy.fan_peer"
                value, unit = (x or {}).get("excess_fraction"), "fraction"
                status = x["status"] if x else "UNKNOWN"
            elif overlay_type == "cooling_efficiency":
                status, source = "UNAVAILABLE", "none"
            elif overlay_type == "data_quality":
                value, unit, status = s.get("age_seconds"), "seconds", s["freshness"]
            items.append({"object_id": o.id, "asset_id": aid, "overlay_type": overlay_type, "value": value,
                          "unit": unit, "status": status, "observed_at": observed, "data_source": source,
                          "is_predicted": predicted, "warnings": item_warnings})
        return {**self._meta(), "simulation_run_id": run_id, "overlay_type": overlay_type,
                "reference_time": tq.iso(reference), "bands": bands,
                "bands_note": "visual bands from the layout file; not equipment safety limits", "items": items,
                "warnings": warnings}

    # ------------------------------------------------------------------ inspection
    def resolve(self, object_or_asset_id: str):
        o = self.layout.objects.get(object_or_asset_id) or self.layout.by_asset().get(object_or_asset_id)
        if o is None:
            raise LayoutObjectNotFoundError(f"{object_or_asset_id!r} is neither a layout object nor a mapped asset")
        return o

    def inspect(self, object_or_asset_id, run_id, start, end, metrics, include_forecast, include_recommendations,
                rec_service) -> dict:
        o = self.resolve(object_or_asset_id)
        base = {**self._meta(), "object": o.to_dict(self.layout.version, bool(o.asset_id)),
                "parents": [{"id": p.id, "type": p.type, "name": p.name} for p in self.layout.ancestors(o.id)]}
        if o.asset_id is None:
            run_id, reference, states = self._states(run_id, None, self.layout.descendants(o.id))
            agg = self._aggregates(states).get(o.id)
            return {**base, "kind": "aggregated", "aggregate": agg, "children": [
                {"object_id": s["object_id"], "asset_id": s["asset_id"], "freshness": s["freshness"],
                 "thermal_status": s["thermal_status"]} for s in states.values()],
                    "warnings": [] if agg else ["this object has no telemetry and no child servers with telemetry"]}
        self.scope.check(o.asset_id)
        assets = AssetService(self.store, self.settings, self.scope, self.cooling_config)
        detail = assets.detail(o.asset_id, run_id)
        history = assets.telemetry(TelemetryQuery(asset_id=o.asset_id, run_id=detail["simulation_run_id"],
                                                  start_time=start, end_time=end, interval="5m", metrics=metrics,
                                                  limit=self.settings.max_telemetry_points), o.asset_id)
        out = {**base, "kind": "asset", "asset_id": o.asset_id, "detail": detail, "history": history}
        if include_forecast and self.registry is not None:
            out["forecast"] = CoolingModelService(self.store, self.registry).predict_v2(
                CoolingPredictRequest(asset_id=o.asset_id, run_id=detail["simulation_run_id"]),
                self.settings.model_max_age_days)
        if include_recommendations and rec_service is not None:
            from app.recommendations.schemas import AlertQuery, ListQuery
            out["recommendations"] = rec_service.list(ListQuery(asset_id=o.asset_id, limit=10))["items"]
            out["alerts"] = rec_service.alerts(AlertQuery(asset_id=o.asset_id, limit=10))["items"]
        return out

    # ------------------------------------------------------------------ analysis (copilot graph)
    def analyze(self, object_or_asset_id, question, analysis_type, start, end, run_id) -> dict:
        o = self.resolve(object_or_asset_id)
        hint = {"thermal": "temperature trend", "cooling_efficiency": "cooling efficiency energy",
                "workload": "workload utilization", "energy": "energy power", "maintenance": "investigate first",
                "forecast": "forecast", "recommendations": "recommendations", None: ""}[analysis_type]
        asset_ids = [o.asset_id] if o.asset_id else [c.asset_id for c in self.layout.descendants(o.id) if c.asset_id]
        asset_ids = [a for a in asset_ids if self.scope.permits(a)]
        if not asset_ids:
            raise InvalidRequestError("the selected object has no telemetry-linked assets in your scope")
        message = question if not hint else f"{question} ({hint})"
        if len(asset_ids) > 1:
            message += " for assets " + ", ".join(asset_ids)
        res = self.copilot.chat(message, scope=self.scope, asset_id=asset_ids[0] if len(asset_ids) == 1 else None,
                                run_id=run_id, start_time=start, end_time=end)
        return {"analysis_id": f"twa-{res['conversation_id'][5:]}", "object_id": o.id, "asset_ids": asset_ids,
                "question": question, "analysis_type": analysis_type, "summary": res["answer"],
                "clarification_needed": res["clarification_needed"], "sections": res["sections"],
                "evidence": res["evidence"], "metrics": res["metrics"], "predictions": res["predictions"],
                "recommendations": res["recommendations"], "warnings": res["warnings"],
                "execution": res["agent_execution_summary"], "conversation_id": res["conversation_id"],
                "recommendation_only": True, "automatic_control_executed": False}

