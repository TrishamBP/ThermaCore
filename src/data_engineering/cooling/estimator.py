"""Cooling State Estimator: read telemetry, compute window metrics, classify, return a snapshot.

    uv run python -m data_engineering.cooling.estimator                      # whole data center, newest run
    uv run python -m data_engineering.cooling.estimator --asset SIM-NA-013 --window-minutes 30
    uv run python -m data_engineering.cooling.estimator --set-limit inlet_temperature_high_c=24

Read-only: it never writes to the database and never changes cooling settings.

Time model. Telemetry carries two clocks: `event_timestamp` (when the reading was measured; simulated
time in this project) and `ingested_at` (wall clock when the consumer stored it). The window ends at
`as_of` = the newest event_timestamp of the run (or an explicit as_of) and covers
(as_of - window, as_of]. Freshness is checked on both clocks:
  - DQ_STALE_ASSET: an asset's newest reading lags as_of by more than stale_after_seconds;
  - DQ_PIPELINE_STALE: nothing was stored for more than max_ingest_age_seconds of wall-clock time.

Scope. Simulated asset IDs repeat across simulation runs, so every evaluation is scoped to one run
(default: the run that most recently received readings). Without an asset, the data-center snapshot
evaluates every asset separately and then aggregates each asset's own statistics; readings of
different assets are never combined into one series.
"""

import argparse
import json
import sys
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from data_engineering.cooling import rules
from data_engineering.cooling.config import (
    CoolingConfig,
    CoolingConfigError,
    load_cooling_config,
)
from data_engineering.cooling.metrics import (
    Reading,
    derived_delta,
    prepare_series,
    series_stats,
)
from data_engineering.cooling.models import (
    AggregateMetric,
    AssetSummary,
    CoolingStateSnapshot,
    DataQuality,
    Finding,
    GapInfo,
    MetricSeries,
    RuleNotEvaluated,
    Scope,
)
from data_engineering.logging_utils import get_logger

log = get_logger("cooling_state")

# Units as documented in the telemetry event schema (inferred from source column names).
UNITS = {
    "inlet_temperature_c": "degC", "exhaust_temperature_c": "degC", "temperature_delta_c": "degC",
    "cpu_utilization": "percent", "gpu_utilization": "percent", "workload_intensity": "ratio_0_1",
    "fan_speed_rpm": "rpm", "airflow_cfm": "cfm", "power_consumption_kw": "kW", "pue": "ratio",
}
TELEMETRY_METRICS = ("inlet_temperature_c", "exhaust_temperature_c", "cpu_utilization", "gpu_utilization",
                     "workload_intensity", "fan_speed_rpm", "airflow_cfm", "power_consumption_kw", "pue")
# Requested indicators that the telemetry schema (v1.0) does not contain.
UNAVAILABLE = {
    "cooling_efficiency": ("ratio", "not in the telemetry schema v1.0: the Kaggle source has a static per-row "
                                    "cooling_efficiency, but the simulator does not produce it per minute"),
}
GPU_ASSET_TYPES = frozenset({"GPU"})
NOTES = [
    "All telemetry is simulated (source_type=simulated) from an engineered synthetic Kaggle dataset; "
    "this is not real operational telemetry.",
    "power_consumption_kw is per-asset power as measured in the telemetry; the schema does not identify "
    "cooling power, so no cooling-energy figures are derived.",
    "Findings of kind 'inference' are rule-based suggestions, not confirmed faults.",
]


class UnknownScopeError(LookupError):
    """The requested simulation run or asset does not exist in the telemetry store."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind          # "run" | "asset"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class CoolingStateEstimator:
    def __init__(self, store, config: CoolingConfig, clock: Callable[[], datetime] = utcnow) -> None:
        self.store, self.config, self.clock = store, config, clock

    # ----- public -----
    def evaluate(self, asset_id: str | None = None, run_id: str | None = None,
                 window_minutes: int | None = None, as_of: datetime | None = None) -> CoolingStateSnapshot:
        started = time.monotonic()
        config = self.config.with_overrides(window_minutes=window_minutes) if window_minutes else self.config
        if as_of is not None and as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware (UTC)")
        evaluated_at = self.clock()

        run_id = run_id or self.store.latest_run_id()
        if run_id is None:
            return self._no_telemetry(evaluated_at, config, asset_id, None, "the telemetry store has no readings")
        assets = self.store.run_assets(run_id)
        if not assets:
            raise UnknownScopeError("run", f"simulation run {run_id!r} not found")
        if asset_id is not None and asset_id not in {a["asset_id"] for a in assets}:
            raise UnknownScopeError("asset", f"asset {asset_id!r} not found in simulation run {run_id!r}")

        as_of = as_of or max(a["last_event_timestamp"] for a in assets)
        window_start = as_of - timedelta(minutes=config.window_minutes)
        last_ingest = max(a["updated_at"] for a in assets)
        ingest_age = (evaluated_at - last_ingest).total_seconds()
        pipeline_finding = rules.pipeline_staleness_finding(ingest_age, config)

        selected = [a for a in assets if asset_id is None or a["asset_id"] == asset_id]
        results = [self._evaluate_asset(a, run_id, window_start, as_of, config) for a in selected]

        if asset_id is not None:
            snapshot = self._asset_snapshot(results[0], evaluated_at, as_of, window_start, config, run_id,
                                            ingest_age, pipeline_finding)
        else:
            snapshot = self._data_center_snapshot(results, evaluated_at, as_of, window_start, config, run_id,
                                                  ingest_age, pipeline_finding)
        log.info("cooling state evaluated", extra={"context": {
            "scope": snapshot.scope.level, "asset_id": asset_id, "run_id": run_id, "state": snapshot.state,
            "severity": snapshot.severity, "findings": len(snapshot.findings),
            "rules_not_evaluated": len(snapshot.rules_not_evaluated), "as_of": as_of.isoformat(),
            "ms": round((time.monotonic() - started) * 1000, 1)}})
        return snapshot

    # ----- per asset -----
    def _evaluate_asset(self, asset: dict, run_id: str, start: datetime, end: datetime, config: CoolingConfig) -> dict:
        rows = self.store.readings_window(run_id, asset["asset_id"], start, end)
        readings = [Reading(r["event_timestamp"], r["ingested_at"], r["reading_status"],
                            {m: r[m] for m in TELEMETRY_METRICS}) for r in rows]
        series = prepare_series(readings, start, end, config.expected_interval_seconds, config.gap_tolerance_factor)
        is_gpu_asset = asset.get("asset_type") in GPU_ASSET_TYPES

        def stats(name, s=series) -> MetricSeries:
            st = series_stats(s, name, end, recent_minutes=config.recent_minutes, min_points=config.min_points,
                              min_change=config.trend_min_change.get(name))
            return MetricSeries(unit=UNITS[name], observed_points=st.observed_points, null_points=st.null_points,
                                sufficient_points=st.sufficient, latest=st.latest,
                                latest_timestamp=st.latest_timestamp, latest_age_seconds=st.latest_age_seconds,
                                mean=st.mean, minimum=st.minimum, maximum=st.maximum, std_dev=st.std_dev,
                                first=st.first, change=st.change, change_elapsed_minutes=st.elapsed_minutes,
                                trend_per_minute=st.trend_per_minute, recent_rate_per_minute=st.recent_rate_per_minute,
                                direction=st.direction)

        metrics: dict[str, MetricSeries] = {}
        for name in TELEMETRY_METRICS:
            if name == "gpu_utilization" and not is_gpu_asset:
                metrics[name] = MetricSeries(
                    unit=UNITS[name], applicable=False,
                    reason=f"asset_type {asset.get('asset_type')!r} has no GPU; the telemetry value (0) is "
                           "structural, not a measured utilization")
            else:
                metrics[name] = stats(name)
        delta_series = derived_delta(series, "exhaust_temperature_c", "inlet_temperature_c", "temperature_delta_c")
        metrics["temperature_delta_c"] = stats("temperature_delta_c", delta_series).model_copy(update={
            "source": "derived", "reason": "exhaust_temperature_c - inlet_temperature_c of the same reading"})
        for name, (unit, reason) in UNAVAILABLE.items():
            metrics[name] = MetricSeries(unit=unit, available=False, reason=reason)

        newest = series.readings[-1].timestamp if series.readings else asset.get("last_event_timestamp")
        staleness = (end - newest).total_seconds() if newest is not None else None
        aid = asset["asset_id"]
        findings = rules.data_quality_findings(series, metrics, config, aid, staleness)
        insufficient = any(f.rule_id == "DQ_NO_READINGS" for f in findings)
        skipped: list[RuleNotEvaluated] = []
        direction = "unknown"
        if not insufficient:
            condition, skipped = rules.condition_findings(metrics, config, aid)
            findings += condition
            direction = rules.workload_direction(metrics)
            wl = rules.workload_finding(direction, metrics, aid)
            if wl is not None:
                findings.append(wl)
        state, severity = rules.classify(findings, skipped, insufficient=insufficient)
        return {"asset": asset, "series": series, "metrics": metrics, "findings": findings, "skipped": skipped,
                "state": state, "severity": severity, "workload_direction": direction,
                "insufficient": insufficient, "staleness": staleness}

    @staticmethod
    def _data_quality(results: list[dict], findings: list[Finding], ingest_age: float) -> DataQuality:
        dq_findings = [f for f in findings if f.kind == "data_quality"]
        series = [r["series"] for r in results]
        if results and all(r["insufficient"] for r in results):
            status = "INSUFFICIENT"
        elif any(f.severity != "INFO" for f in dq_findings):
            status = "DEGRADED"
        else:
            status = "OK"
        expected = sum(s.expected_points for s in series)
        observed = sum(s.observed_points for s in series)
        missing_fields = sorted({name for r in results for name, m in r["metrics"].items()
                                 if m.available and m.applicable and m.observed_points == 0})
        gaps = [GapInfo(start=g.start, end=g.end, seconds=g.seconds, position=g.position)
                for s in series for g in s.gaps] if len(results) == 1 else []
        intervals = [s.median_interval_seconds for s in series if s.median_interval_seconds is not None]
        return DataQuality(
            status=status, stale=any(f.rule_id in ("DQ_STALE_ASSET", "DQ_PIPELINE_STALE") for f in dq_findings),
            ingest_age_seconds=round(ingest_age, 1), expected_points=expected,
            present_points=sum(s.present_points for s in series), observed_points=observed,
            missing_readings=sum(s.missing_readings for s in series),
            duplicate_timestamps=sum(s.duplicate_timestamps for s in series),
            conflicting_duplicates=sum(s.conflicting_duplicates for s in series),
            irregular_intervals=sum(s.irregular_intervals for s in series),
            median_interval_seconds=sorted(intervals)[len(intervals) // 2] if intervals else None,
            coverage=round(min(1.0, observed / expected), 4) if expected else None, gaps=gaps,
            missing_fields=missing_fields, unavailable_fields=sorted(UNAVAILABLE),
            warnings=[f"{f.rule_id}" + (f" ({f.asset_id})" if f.asset_id else "") + f": {f.explanation}"
                      for f in dq_findings if f.severity != "INFO"])

    def _asset_snapshot(self, r, evaluated_at, as_of, window_start, config, run_id, ingest_age, pipeline_finding):
        findings = list(r["findings"]) + ([pipeline_finding] if pipeline_finding else [])
        state, severity = rules.classify(findings, r["skipped"], insufficient=r["insufficient"])
        asset = r["asset"]
        return CoolingStateSnapshot(
            evaluated_at=evaluated_at, as_of=as_of, window_minutes=config.window_minutes, window_start=window_start,
            scope=Scope(level="asset", asset_id=asset["asset_id"], asset_type=asset.get("asset_type"),
                        simulation_run_id=run_id, data_center_id=asset.get("data_center_id")),
            state=state, severity=severity, workload_direction=r["workload_direction"], metrics=r["metrics"],
            findings=findings, rules_not_evaluated=r["skipped"],
            data_quality=self._data_quality([r], findings, ingest_age),
            source_type=asset.get("source_type"), notes=NOTES)

    def _data_center_snapshot(self, results, evaluated_at, as_of, window_start, config, run_id, ingest_age,
                              pipeline_finding):
        findings = [f for r in results for f in r["findings"]] + ([pipeline_finding] if pipeline_finding else [])
        skipped = [s for r in results for s in r["skipped"]]
        all_insufficient = bool(results) and all(r["insufficient"] for r in results)
        state, severity = rules.classify(findings, skipped, insufficient=all_insufficient)

        aggregates: dict[str, AggregateMetric] = {}
        for name in (*TELEMETRY_METRICS, "temperature_delta_c"):
            usable = [(r["asset"]["asset_id"], r["metrics"][name]) for r in results
                      if r["metrics"][name].applicable and r["metrics"][name].latest is not None]
            agg = AggregateMetric(unit=UNITS[name], source="derived" if name == "temperature_delta_c" else "telemetry",
                                  assets_reporting=len(usable))
            if name == "gpu_utilization":
                agg.reason = "aggregated over GPU assets only"
            if usable:
                latest = [(aid, m.latest) for aid, m in usable]
                agg.mean_of_latest = sum(v for _, v in latest) / len(latest)
                agg.min_of_latest = min(v for _, v in latest)
                agg.max_asset_id, agg.max_of_latest = max(latest, key=lambda x: (x[1], x[0]))
                if name == "power_consumption_kw":
                    agg.sum_of_latest = sum(v for _, v in latest)
                trends = [(aid, m.trend_per_minute) for aid, m in usable if m.trend_per_minute is not None]
                if trends:
                    agg.mean_trend_per_minute = sum(t for _, t in trends) / len(trends)
                    agg.max_trend_asset_id, agg.max_trend_per_minute = max(trends, key=lambda x: (x[1], x[0]))
                for _, m in usable:
                    key = m.direction or "unknown"
                    agg.directions[key] = agg.directions.get(key, 0) + 1
            aggregates[name] = agg
        for name, (unit, reason) in UNAVAILABLE.items():
            aggregates[name] = AggregateMetric(unit=unit, available=False, reason=reason)

        dirs = [r["workload_direction"] for r in results if r["workload_direction"] not in ("unknown",)]
        moving = {d for d in dirs if d != "stable"}
        workload = ("unknown" if not dirs else "stable" if not moving
                    else moving.pop() if len(moving) == 1 and "mixed" not in dirs else "mixed")
        summaries = [AssetSummary(asset_id=r["asset"]["asset_id"], asset_type=r["asset"].get("asset_type"),
                                  state=r["state"], severity=r["severity"],
                                  rule_ids=sorted({f.rule_id for f in r["findings"]}),
                                  data_quality_status=("INSUFFICIENT" if r["insufficient"] else "DEGRADED" if any(
                                      f.kind == "data_quality" and f.severity != "INFO" for f in r["findings"]) else "OK"),
                                  latest_timestamp=r["series"].readings[-1].timestamp if r["series"].readings else None)
                     for r in results]
        first = results[0]["asset"] if results else {}
        return CoolingStateSnapshot(
            evaluated_at=evaluated_at, as_of=as_of, window_minutes=config.window_minutes, window_start=window_start,
            scope=Scope(level="data_center", simulation_run_id=run_id, data_center_id=first.get("data_center_id")),
            state=state, severity=severity, workload_direction=workload, metrics=aggregates, findings=findings,
            rules_not_evaluated=_dedupe_skipped(skipped),
            data_quality=self._data_quality(results, findings, ingest_age), assets=summaries,
            source_type=first.get("source_type"),
            notes=NOTES + ["Data-center metrics are statistics over each asset's own latest values and trends; "
                           "sum_of_latest is reported only for power (additive)."])

    def _no_telemetry(self, evaluated_at, config, asset_id, run_id, reason) -> CoolingStateSnapshot:
        finding = Finding(rule_id="DQ_NO_TELEMETRY", state="INSUFFICIENT_DATA", severity="WARNING",
                          kind="data_quality", explanation=f"No telemetry available: {reason}.", evidence={})
        return CoolingStateSnapshot(
            evaluated_at=evaluated_at, as_of=None, window_minutes=config.window_minutes, window_start=None,
            scope=Scope(level="asset" if asset_id else "data_center", asset_id=asset_id, simulation_run_id=run_id),
            state="INSUFFICIENT_DATA", severity="WARNING", workload_direction="unknown", metrics={},
            findings=[finding], rules_not_evaluated=[],
            data_quality=DataQuality(status="INSUFFICIENT", stale=False, unavailable_fields=sorted(UNAVAILABLE),
                                     warnings=[finding.explanation]), notes=NOTES)


def _dedupe_skipped(skipped: list[RuleNotEvaluated]) -> list[RuleNotEvaluated]:
    """At data-center scope, a rule skipped for the same reason on every asset is listed once."""
    by_rule: dict[tuple[str, str], list[str]] = {}
    for s in skipped:
        by_rule.setdefault((s.rule_id, s.reason), []).append(s.asset_id)
    out = []
    for (rule_id, reason), assets in by_rule.items():
        out.append(RuleNotEvaluated(rule_id=rule_id, reason=reason,
                                    asset_id=assets[0] if len(assets) == 1 else None))
    return out


def _parse_limit(text: str) -> tuple[str, float]:
    name, sep, value = text.partition("=")
    if not sep:
        raise argparse.ArgumentTypeError("use NAME=VALUE, e.g. inlet_temperature_high_c=27")
    try:
        return name.strip(), float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not a number") from None


def main(argv: list[str] | None = None) -> int:
    from data_engineering.storage.base import open_store
    from data_engineering.streaming.settings import load_consumer_settings

    ap = argparse.ArgumentParser(prog="python -m data_engineering.cooling.estimator",
                                 description="Print a cooling state snapshot (read-only).")
    ap.add_argument("--asset", help="asset_id (default: whole data center)")
    ap.add_argument("--run", help="simulation_run_id (default: the run that most recently received readings)")
    ap.add_argument("--window-minutes", type=int)
    ap.add_argument("--as-of", type=lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")),
                    help="evaluate the window ending at this event time (UTC), e.g. 2026-01-01T13:50:00Z")
    ap.add_argument("--set-limit", action="append", type=_parse_limit, default=[], metavar="NAME=VALUE",
                    help="override one limit from config/cooling_state.yaml for this run (repeatable)")
    ap.add_argument("--config", help="cooling config file (default config/cooling_state.yaml)")
    args = ap.parse_args(argv)
    try:
        config = load_cooling_config(args.config)
        if args.set_limit:
            config = config.with_overrides(**dict(args.set_limit))
        store = open_store(load_consumer_settings())
    except (CoolingConfigError, TypeError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    try:
        store.check_initialized()
        snapshot = CoolingStateEstimator(store, config).evaluate(args.asset, args.run, args.window_minutes, args.as_of)
    except (UnknownScopeError, ValueError, CoolingConfigError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        store.close()
    print(json.dumps(snapshot.model_dump(mode="json"), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
