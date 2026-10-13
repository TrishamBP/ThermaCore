"""Asset and telemetry service (read-only queries through the store's parameterized functions)."""

from datetime import datetime, timedelta, timezone

from app.assets.schemas import AssetListQuery, TelemetryQuery
from app.assets.scope import run_assets
from app.common.pagination import decode_cursor, encode_cursor
from app.common.security.auth import AssetScope
from app.config import AppSettings
from app.exceptions import InvalidRequestError
from data_engineering import telemetry_query as tq
from data_engineering.cooling.config import CoolingConfig
from data_engineering.cooling.estimator import CoolingStateEstimator

AGGREGATION = ("Readings with the same (asset, timestamp) collapse to the most recently ingested one. Only observed "
               "readings contribute values; missing readings are counted, never treated as 0. Buckets are "
               "[bucket_start, bucket_start + interval) aligned to the UTC epoch, per asset (assets are never mixed); "
               "each metric gives mean/min/max/count of observed values (null when count is 0). Range: "
               "start_time exclusive, end_time inclusive. GPU utilization is null for non-GPU assets.")
PUBLIC_ASSET_FIELDS = ("asset_id", "asset_type", "cooling_type", "data_center_id", "region", "region_code",
                       "server_age_years", "rack_u_position", "energy_source_primary", "source_type",
                       "source_failure_risk_level", "source_energy_efficiency_class", "source_maintenance_status",
                       "first_event_timestamp", "last_event_timestamp", "generator", "generator_version", "seed")


def _metric_info(names):
    return [{"name": m, "unit": tq.UNITS[m], "derived_from": tq.DERIVED.get(m), "note": tq.METRIC_NOTES.get(m)}
            for m in names]


class AssetService:
    def __init__(self, store, settings: AppSettings, scope: AssetScope, cooling_config: CoolingConfig) -> None:
        self.store, self.settings, self.scope, self.cooling_config = store, settings, scope, cooling_config

    def _fresh(self, latest: datetime | None, reference: datetime | None) -> bool | None:
        if latest is None or reference is None:
            return None
        return (reference - latest).total_seconds() <= self.cooling_config.stale_after_seconds

    def telemetry(self, query: TelemetryQuery, asset_id: str | None = None) -> dict:
        asset_id = asset_id or query.asset_id
        if query.cursor and asset_id is None:
            raise InvalidRequestError("cursor pagination needs asset_id")
        run_id, assets = run_assets(self.store, query.run_id, self.scope, asset_id)
        default_end = max((a["last_event_timestamp"] for a in assets.values()), default=None)
        try:
            start, end = tq.window_bounds(query.start_time, query.end_time, default_end, 60,
                                          self.settings.max_telemetry_range_hours)
        except ValueError as exc:
            raise InvalidRequestError(str(exc)) from exc
        cursor = decode_cursor(query.cursor)
        if cursor is not None:
            if not start < cursor[0] < end or cursor[1] != asset_id:
                raise InvalidRequestError("cursor does not belong to this query")
            start = cursor[0]
        limit = min(query.limit, self.settings.max_telemetry_points)
        budget, series, warnings = self.settings.max_telemetry_points, [], []
        for a in sorted(assets.values(), key=lambda x: x["asset_id"]):
            if budget <= 0:
                warnings.append("response point budget reached; narrow the asset set or time range")
                break
            s = tq.asset_window(self.store, run_id, a, start, end, query.metric_list(), query.interval,
                                min(limit, budget))
            budget -= len(s["points"])
            series.append(s)
        next_cursor = None
        if asset_id is not None and series and series[0]["truncated"] and series[0]["points"]:
            last = series[0]["points"][-1]
            last_ts = datetime.fromisoformat((last.get("timestamp") or last["bucket_start"]).replace("Z", "+00:00"))
            if query.interval != "raw":                      # continue after the last whole bucket
                last_ts = last_ts + timedelta(seconds=tq.INTERVALS[query.interval]) - timedelta(microseconds=1)
            next_cursor = encode_cursor(last_ts, asset_id)
        for s in series:
            if s["readings"] == 0:
                warnings.append(f"{s['asset_id']}: no readings in the requested range")
            if s["duplicates_collapsed"]:
                warnings.append(f"{s['asset_id']}: {s['duplicates_collapsed']} duplicate reading(s) collapsed")
            if s["gaps_over_tolerance"]:
                warnings.append(f"{s['asset_id']}: {s['gaps_over_tolerance']} gap(s) between readings")
        return {"simulation_run_id": run_id, "start_time": tq.iso(start), "end_time": tq.iso(end),
                "interval": query.interval, "metrics": _metric_info(query.metric_list()), "aggregation": AGGREGATION,
                "series": series, "next_cursor": next_cursor, "unavailable_measurements": tq.UNAVAILABLE_MEASUREMENTS,
                "warnings": warnings}

    def list(self, query: AssetListQuery) -> dict:
        run_id, assets = run_assets(self.store, query.run_id, self.scope)
        reference = max((a["last_event_timestamp"] for a in assets.values()), default=None)
        latest = tq.latest_per_asset(self.store, run_id, None, assets)
        items = []
        for aid in sorted(assets)[:query.limit]:
            a = assets[aid]
            ts = latest.get(aid, {}).get("event_timestamp")
            items.append({"asset_id": aid, "asset_type": a.get("asset_type"), "cooling_type": a.get("cooling_type"),
                          "data_center_id": a.get("data_center_id"), "region_code": a.get("region_code"),
                          "first_event_timestamp": tq.iso(a["first_event_timestamp"]),
                          "last_event_timestamp": tq.iso(a["last_event_timestamp"]),
                          "latest_observed_timestamp": tq.iso(ts), "fresh": self._fresh(ts, reference),
                          "source_type": a.get("source_type")})
        return {"simulation_run_id": run_id, "items": items, "count": len(items),
                "freshness_rule": f"fresh = latest observed reading within {self.cooling_config.stale_after_seconds} s "
                                  "(event time) of the run's newest reading"}

    def detail(self, asset_id: str, run_id: str | None) -> dict:
        run_id, assets = run_assets(self.store, run_id, self.scope, asset_id)
        a = assets[asset_id]
        latest = tq.latest_per_asset(self.store, run_id, None, assets, asset_id).get(asset_id)
        warnings, reading = [], None
        if latest is None:
            warnings.append("no observed reading for this asset")
        else:
            reading = {"timestamp": tq.iso(latest["event_timestamp"]),
                       "values": {m: {"value": tq.value(latest, m, a.get("asset_type")), "unit": tq.UNITS[m],
                                      **({"note": tq.METRIC_NOTES[m]} if m in tq.METRIC_NOTES else {})}
                                  for m in tq.SUPPORTED_METRICS}}
        state = None
        try:
            snap = CoolingStateEstimator(self.store, self.cooling_config).evaluate(asset_id, run_id)
            state = {"state": snap.state, "severity": snap.severity, "as_of": tq.iso(snap.as_of),
                     "window_minutes": snap.window_minutes,
                     "findings": [{"rule_id": f.rule_id, "severity": f.severity, "kind": f.kind,
                                   "explanation": f.explanation} for f in snap.findings],
                     "rules_not_evaluated": len(snap.rules_not_evaluated)}
        except ValueError as exc:
            warnings.append(f"cooling state unavailable: {exc}")
        asset = {k: (tq.iso(a[k]) if isinstance(a.get(k), datetime) else a.get(k)) for k in PUBLIC_ASSET_FIELDS}
        if reading and (datetime.now(timezone.utc) - a["updated_at"]).total_seconds() > \
                self.cooling_config.max_ingest_age_seconds:
            warnings.append("the telemetry pipeline has not stored data recently (wall clock)")
        return {"simulation_run_id": run_id, "asset": asset, "latest_reading": reading, "cooling_state": state,
                "unavailable_measurements": tq.UNAVAILABLE_MEASUREMENTS, "warnings": warnings}
