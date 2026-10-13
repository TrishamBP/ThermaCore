"""Dashboard overview.

Semantics (also returned in the response):
- scope: the assets of one simulation run (default: most recently updated run) within the operator's scope,
  optionally one asset;
- "latest" = each asset's latest OBSERVED reading at or before end_time (one query; same-timestamp duplicates
  collapse to the latest ingested); missing values stay null and are excluded from statistics;
- KPI statistics are across assets of their latest values (each asset counted once): mean/min/max and the
  number of assets reporting; power is additionally summed (server IT power, not cooling or facility power);
- freshness: an asset is fresh when its latest observed reading is within stale_after_seconds (event time)
  of end_time; the pipeline is stale when nothing was stored for max_ingest_age_seconds (wall clock);
- thermal risk: the cooling state estimator's data-center snapshot of the window ending at end_time;
- trends: per bucket, the mean across assets of each asset's bucket mean (equal weight per asset), plus a
  least-squares slope per minute of those bucket values.
"""

from collections import Counter
from datetime import datetime, timezone

from app.assets.scope import run_assets
from app.common.security.auth import AssetScope
from app.config import AppSettings
from app.exceptions import InvalidRequestError
from data_engineering import telemetry_query as tq
from data_engineering.cooling.config import CoolingConfig
from data_engineering.cooling.estimator import CoolingStateEstimator

KPI_METRICS = ("inlet_temperature_c", "exhaust_temperature_c", "temperature_delta_c", "workload_intensity",
               "cpu_utilization", "gpu_utilization", "fan_speed_rpm", "airflow_cfm", "power_consumption_kw", "pue")
TREND_METRICS = ("inlet_temperature_c", "exhaust_temperature_c", "workload_intensity", "cpu_utilization",
                 "fan_speed_rpm", "airflow_cfm", "power_consumption_kw")
SEMANTICS = __doc__.split("Semantics (also returned in the response):")[1].strip()


def _kpi(latest: dict, assets: dict, metric: str) -> dict:
    values = {aid: v for aid, r in latest.items()
              if (v := tq.value(r, metric, assets[aid].get("asset_type"))) is not None}
    out = {"unit": tq.UNITS[metric], "assets_reporting": len(values), "mean": None, "min": None, "max": None,
           "max_asset_id": None}
    if metric in tq.METRIC_NOTES:
        out["note"] = tq.METRIC_NOTES[metric]
    if values:
        out.update(mean=round(sum(values.values()) / len(values), 4), min=round(min(values.values()), 4),
                   max=round(max(values.values()), 4), max_asset_id=max(values, key=values.get))
        if metric == "power_consumption_kw":
            out["sum"] = round(sum(values.values()), 3)
    return out


class DashboardService:
    def __init__(self, store, settings: AppSettings, scope: AssetScope, cooling_config: CoolingConfig, clock=None):
        self.store, self.settings, self.scope, self.cooling_config = store, settings, scope, cooling_config
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def overview(self, asset_id, run_id, start_time, end_time) -> dict:
        run_id, assets = run_assets(self.store, run_id, self.scope, asset_id)
        warnings = []
        if not assets:
            raise InvalidRequestError("no assets of this run are within the operator's scope")
        default_end = max(a["last_event_timestamp"] for a in assets.values())
        try:
            start, end = tq.window_bounds(start_time, end_time, default_end, self.settings.dashboard_trend_minutes,
                                          self.settings.max_telemetry_range_hours)
        except ValueError as exc:
            raise InvalidRequestError(str(exc)) from exc

        latest = tq.latest_per_asset(self.store, run_id, end, assets, asset_id)
        stale_s = self.cooling_config.stale_after_seconds
        fresh = sorted(a for a, r in latest.items() if (end - r["event_timestamp"]).total_seconds() <= stale_s)
        no_reading = sorted(set(assets) - set(latest))
        stale = sorted(set(latest) - set(fresh))
        if no_reading:
            warnings.append(f"{len(no_reading)} asset(s) have no observed reading at or before end_time")
        if stale:
            warnings.append(f"{len(stale)} asset(s) have stale telemetry (> {stale_s} s before end_time)")
        ingest_age = (self.clock() - max(a["updated_at"] for a in assets.values())).total_seconds()
        pipeline_stale = ingest_age > self.cooling_config.max_ingest_age_seconds
        if pipeline_stale:
            warnings.append(f"the pipeline has not stored data for {ingest_age:.0f} s (wall clock)")

        # thermal risk from the estimator (same rules as /telemetry/cooling-state)
        estimator = CoolingStateEstimator(self.store, self.cooling_config, clock=self.clock)
        snap = estimator.evaluate(asset_id, run_id, None, end)
        if asset_id is None:
            states = Counter(a.state for a in snap.assets if self.scope.permits(a.asset_id))
            severities = Counter(a.severity for a in snap.assets if self.scope.permits(a.asset_id))
            findings = [f for f in snap.findings if f.asset_id is None or self.scope.permits(f.asset_id)]
        else:
            states, severities, findings = Counter([snap.state]), Counter([snap.severity]), snap.findings
        findings_by_rule = Counter(f.rule_id for f in findings)

        # trends over (start, end]
        rows, duplicates = tq.dedupe(self.store.run_readings_window(run_id, start, end,
                                                                    self.settings.max_telemetry_points + 1))
        if len(rows) > self.settings.max_telemetry_points:
            warnings.append("trend window truncated at the point limit; narrow the window or the asset scope")
        rows = [r for r in rows if r["asset_id"] in assets]
        interval = self.settings.dashboard_trend_interval_minutes * 60
        per_asset = {}
        for r in rows:
            per_asset.setdefault(r["asset_id"], []).append(r)
        buckets: dict[int, dict[str, list[float]]] = {}
        for aid, ar in per_asset.items():
            s = tq.series(ar, list(TREND_METRICS), assets[aid].get("asset_type"), interval)
            for p in s["points"]:
                key = int(datetime.fromisoformat(p["bucket_start"].replace("Z", "+00:00")).timestamp())
                for m in TREND_METRICS:
                    if p[m]["mean"] is not None:
                        buckets.setdefault(key, {}).setdefault(m, []).append(p[m]["mean"])
        trend_points = [{"bucket_start": tq.iso(datetime.fromtimestamp(k, timezone.utc)),
                         **{m: (round(sum(v[m]) / len(v[m]), 4) if v.get(m) else None) for m in TREND_METRICS},
                         "assets_reporting": max((len(x) for x in v.values()), default=0)}
                        for k, v in sorted(buckets.items())]
        slopes = {}
        for m in TREND_METRICS:
            pts = [(datetime.fromisoformat(p["bucket_start"].replace("Z", "+00:00")), p[m]) for p in trend_points
                   if p[m] is not None]
            slopes[m] = {"per_minute": tq.trend_per_minute(pts), "unit": f"{tq.UNITS[m]}/min"}
        missing = sum(r["reading_status"] != "observed" for r in rows)
        anomalies = [a for a in self.store.anomaly_labels_between(run_id, start, end) if a["asset_id"] in assets]
        alerts = [a for a in self.store.list_alerts(simulation_run_id=run_id, asset_id=asset_id, start=start, end=end,
                                                    limit=1000) if self.scope.permits(a["asset_id"])]
        latest_ts = max((r["event_timestamp"] for r in latest.values()), default=None)
        return {
            "simulation_run_id": run_id, "scope": {"asset_id": asset_id, "assets": len(assets)},
            "window": {"start_time": tq.iso(start), "end_time": tq.iso(end),
                       "trend_interval_minutes": self.settings.dashboard_trend_interval_minutes},
            "latest_telemetry_timestamp": tq.iso(latest_ts),
            "assets": {"known": len(assets), "with_observed_reading": len(latest), "fresh": len(fresh),
                       "stale": stale, "without_reading": no_reading},
            "kpis": {m: _kpi(latest, assets, m) for m in KPI_METRICS},
            "thermal_risk": {"evaluated_window_minutes": snap.window_minutes, "as_of": tq.iso(snap.as_of),
                             "overall_state": snap.state, "overall_severity": snap.severity,
                             "assets_by_state": dict(states), "assets_by_severity": dict(severities),
                             "findings_by_rule": dict(findings_by_rule),
                             "approaching_limit_assets": [a for a in snap.approaching_limit_assets
                                                          if self.scope.permits(a)],
                             "rules_not_evaluated": len(snap.rules_not_evaluated)},
            "anomalies": {"injected_anomaly_labels": [{**a, "first_ts": tq.iso(a["first_ts"]),
                                                       "last_ts": tq.iso(a["last_ts"])} for a in anomalies],
                          "injected_label_note": "labels deliberately injected by the simulator (ground truth "
                                                 "for testing), not detections",
                          "persisted_alerts_in_window": len(alerts),
                          "alerts_by_severity": dict(Counter(a["severity"] for a in alerts))},
            "trends": {"points": trend_points, "slopes": slopes},
            "data_quality": {"readings_in_window": len(rows), "missing_readings": missing,
                             "duplicates_collapsed": duplicates, "pipeline_stale": pipeline_stale,
                             "ingest_age_seconds": round(ingest_age, 1)},
            "unavailable_measurements": tq.UNAVAILABLE_MEASUREMENTS,
            "semantics": SEMANTICS, "warnings": warnings}
