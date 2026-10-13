"""Command-line examples for the DuckDB analytics over the Parquet history.

    uv run python -m data_engineering.analytics.query_telemetry overview          # also lists simulation runs
    uv run python -m data_engineering.analytics.query_telemetry readings SIM-NA-001 \
        --start 2026-01-01T00:00:00Z --end 2026-01-01T00:10:00Z --metrics cpu_utilization power_consumption_kw
    uv run python -m data_engineering.analytics.query_telemetry stats --metrics cpu_utilization inlet_temperature_c
    uv run python -m data_engineering.analytics.query_telemetry counts --bucket hour --asset SIM-NA-001
    uv run python -m data_engineering.analytics.query_telemetry anomalies --bucket day
    uv run python -m data_engineering.analytics.query_telemetry completeness SIM-NA-001 \
        --start 2026-01-01T00:00:00Z --end 2026-01-02T00:00:00Z

Reads PARQUET_OUTPUT_DIR (default data/telemetry) unless --parquet-dir is given. Prints JSON.
"""

import argparse
import json
import sys
from datetime import date, datetime, timezone

from data_engineering.analytics.duckdb_queries import BUCKETS, TelemetryAnalytics
from data_engineering.simulation.generate_telemetry import METRICS
from data_engineering.streaming.settings import load_consumer_settings


def _utc(text: str) -> datetime:
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{text!r}: give a timezone, e.g. 2026-01-01T00:00:00Z")
    return value.astimezone(timezone.utc)


def _json_default(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m data_engineering.analytics.query_telemetry",
                                 description="DuckDB queries over the Parquet telemetry history.")
    ap.add_argument("--parquet-dir", help="override PARQUET_OUTPUT_DIR")
    sub = ap.add_subparsers(dest="query", required=True)
    sub.add_parser("overview", help="row/event/asset/file counts")

    window = argparse.ArgumentParser(add_help=False)
    window.add_argument("--start", type=_utc, help="inclusive, UTC")
    window.add_argument("--end", type=_utc, help="exclusive, UTC")
    window.add_argument("--run", help="simulation_run_id (asset IDs repeat across runs; see `overview`)")

    r = sub.add_parser("readings", parents=[window], help="telemetry of one asset in a time window")
    r.add_argument("asset_id")
    r.add_argument("--metrics", nargs="+", choices=METRICS)
    r.add_argument("--limit", type=int, default=20)

    s = sub.add_parser("stats", parents=[window], help="avg/min/max per metric and asset")
    s.add_argument("--metrics", nargs="+", choices=METRICS)
    s.add_argument("--asset")
    s.add_argument("--all-assets", action="store_true", help="one row per metric instead of per asset")

    c = sub.add_parser("counts", parents=[window], help="events per asset and time bucket")
    c.add_argument("--bucket", choices=BUCKETS, default="hour")
    c.add_argument("--asset")

    a = sub.add_parser("anomalies", parents=[window], help="injected-anomaly labels per asset and period")
    a.add_argument("--bucket", choices=BUCKETS, default="day")
    a.add_argument("--asset")

    k = sub.add_parser("completeness", parents=[window], help="expected vs received slots and gaps")
    k.add_argument("asset_id")
    k.add_argument("--interval-seconds", type=int, default=60)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = load_consumer_settings(parquet_dir=args.parquet_dir)
    analytics = TelemetryAnalytics(root.resolve_path(root.parquet_dir))
    try:
        if args.query == "overview":
            out = analytics.overview()
        elif args.query == "readings":
            if args.start is None or args.end is None:
                raise ValueError("readings needs --start and --end")
            out = analytics.readings(args.asset_id, args.start, args.end, args.metrics, args.limit, args.run)
        elif args.query == "stats":
            out = analytics.metric_stats(args.metrics, args.start, args.end, args.asset, not args.all_assets, args.run)
        elif args.query == "counts":
            out = analytics.event_counts(args.start, args.end, args.bucket, args.asset, args.run)
        elif args.query == "anomalies":
            out = analytics.anomaly_summary(args.start, args.end, args.bucket, args.asset, args.run)
        else:
            if args.start is None or args.end is None:
                raise ValueError("completeness needs --start and --end")
            out = analytics.completeness(args.asset_id, args.start, args.end, args.interval_seconds, run_id=args.run)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(out, indent=2, default=_json_default))
    if not analytics.has_data():
        print(f"note: no Parquet files under {analytics.root}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
