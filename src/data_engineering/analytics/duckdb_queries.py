"""DuckDB queries over the Parquet history written by the consumer.

DuckDB reads the Parquet files in place (`read_parquet` over a glob). Nothing is loaded
into pandas or into a database first; only the columns and partitions a query needs are
read. Time filters also filter on the `event_date` partition directory, so DuckDB skips
whole days that are outside the window.

Safety: every value (asset ID, timestamps, bucket unit, file glob) is a bound parameter.
Metric names cannot be parameters (they are column names), so they are checked against
the allowlist METRICS, the 16 metrics of the event schema, before being placed in SQL.

Asset IDs (SIM-NA-001, ...) are only unique within one simulation run, and the topic can
hold several runs, so every query accepts `run_id` (simulation_run_id). Without it, rows
of all runs are combined.

All timestamps are UTC. Results contain timezone-aware datetimes (DuckDB returns naive
UTC values here because the session time zone is UTC and conversion to aware values
would require pytz).
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from data_engineering.simulation.generate_telemetry import METRICS

BUCKETS = ("minute", "hour", "day")
MAX_LIMIT = 100_000


def _check_metrics(metrics) -> list[str]:
    metrics = list(metrics) if metrics else list(METRICS)
    unknown = [m for m in metrics if m not in METRICS]
    if unknown:
        raise ValueError(f"unknown metric(s) {unknown}; allowed: {list(METRICS)}")
    return metrics


def _check_window(start: datetime | None, end: datetime | None) -> None:
    for name, value in (("start", start), ("end", end)):
        if value is not None and value.tzinfo is None:
            raise ValueError(f"{name} must be timezone-aware (UTC)")
    if start is not None and end is not None and start >= end:
        raise ValueError("start must be before end")


def _aware(value):
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


class TelemetryAnalytics:
    def __init__(self, parquet_dir: Path | str, connection: duckdb.DuckDBPyConnection | None = None) -> None:
        self.root = Path(parquet_dir)
        self.con = connection or duckdb.connect()
        self.con.execute("SET TimeZone = 'UTC'")

    # ----- plumbing -----
    @property
    def glob(self) -> str:
        return (self.root / "**" / "*.parquet").as_posix()

    def has_data(self) -> bool:
        return self.root.is_dir() and next(self.root.rglob("*.parquet"), None) is not None

    def _source(self) -> str:
        return "read_parquet(?, hive_partitioning = true, union_by_name = true)"

    def _rows(self, sql: str, params: list) -> list[dict]:
        cur = self.con.execute(sql, params)
        names = [d[0] for d in cur.description]
        return [{k: _aware(v) for k, v in zip(names, row)} for row in cur.fetchall()]

    @staticmethod
    def _where(start, end, asset_id, run_id=None) -> tuple[str, list]:
        clauses, params = [], []
        if run_id is not None:
            clauses.append("simulation_run_id = ?")
            params.append(run_id)
        if start is not None:
            clauses += ["event_timestamp >= ?", "event_date >= ?"]
            params += [start, start.astimezone(timezone.utc).date()]
        if end is not None:
            clauses += ["event_timestamp < ?", "event_date <= ?"]
            params += [end, end.astimezone(timezone.utc).date()]
        if asset_id is not None:
            clauses.append("asset_id = ?")
            params.append(asset_id)
        return (" WHERE " + " AND ".join(clauses)) if clauses else "", params

    # ----- queries -----
    def overview(self) -> dict:
        """Row and identity counts, for checking the sink against the database and Kafka."""
        if not self.has_data():
            return {"rows": 0, "distinct_event_ids": 0, "assets": 0, "files": 0,
                    "first_event": None, "last_event": None, "missing_readings": 0, "runs": {}}
        row = self._rows(
            "SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_event_ids, "
            "count(DISTINCT asset_id) AS assets, count(DISTINCT filename) AS files, "
            "min(event_timestamp) AT TIME ZONE 'UTC' AS first_event, "
            "max(event_timestamp) AT TIME ZONE 'UTC' AS last_event, "
            "count(*) FILTER (WHERE reading_status = 'missing') AS missing_readings "
            "FROM read_parquet(?, hive_partitioning = true, union_by_name = true, filename = true)",
            [self.glob])[0]
        row["runs"] = {r["simulation_run_id"]: r["rows"] for r in self._rows(
            f"SELECT simulation_run_id, count(*) AS rows FROM {self._source()} GROUP BY 1 ORDER BY 2 DESC",
            [self.glob])}
        return row

    def readings(self, asset_id: str, start: datetime, end: datetime, metrics=None, limit: int = 1_000,
                 run_id: str | None = None) -> list[dict]:
        """Telemetry of one asset with start <= event_timestamp < end, oldest first."""
        _check_window(start, end)
        metrics = _check_metrics(metrics)
        if not 1 <= int(limit) <= MAX_LIMIT:
            raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
        if not self.has_data():
            return []
        where, params = self._where(start, end, asset_id, run_id)
        cols = ", ".join(metrics)
        return self._rows(
            f"SELECT event_id, simulation_run_id, asset_id, event_timestamp AT TIME ZONE 'UTC' AS event_timestamp, "
            f"reading_status, "
            f"{cols} FROM {self._source()}{where} ORDER BY event_timestamp, event_id LIMIT ?",
            [self.glob, *params, int(limit)])

    def metric_stats(self, metrics=None, start: datetime | None = None, end: datetime | None = None,
                     asset_id: str | None = None, per_asset: bool = True, run_id: str | None = None) -> list[dict]:
        """Average / min / max per metric (and per asset). Nulls (missing readings) are excluded
        from the statistics and counted separately, never treated as 0."""
        _check_window(start, end)
        metrics = _check_metrics(metrics)
        if not self.has_data():
            return []
        where, params = self._where(start, end, asset_id, run_id)
        group_col = "asset_id" if per_asset else "'all' AS asset_id"
        group_by = " GROUP BY asset_id" if per_asset else ""
        parts = [f"SELECT {group_col}, '{m}' AS metric, count({m}) AS readings, "
                 f"count(*) - count({m}) AS nulls, avg({m}) AS avg, min({m}) AS min, max({m}) AS max "
                 f"FROM filtered{group_by}" for m in metrics]
        sql = (f"WITH filtered AS (SELECT * FROM {self._source()}{where}) "
               + " UNION ALL ".join(parts) + " ORDER BY asset_id, metric")
        return self._rows(sql, [self.glob, *params])

    def event_counts(self, start: datetime | None = None, end: datetime | None = None, bucket: str = "hour",
                     asset_id: str | None = None, run_id: str | None = None) -> list[dict]:
        """Events per asset per time bucket, split into observed and missing readings."""
        if bucket not in BUCKETS:
            raise ValueError(f"bucket must be one of {BUCKETS}")
        _check_window(start, end)
        if not self.has_data():
            return []
        where, params = self._where(start, end, asset_id, run_id)
        return self._rows(
            f"SELECT asset_id, date_trunc(?, event_timestamp) AT TIME ZONE 'UTC' AS bucket_start, "
            f"count(*) AS events, count(*) FILTER (WHERE reading_status = 'observed') AS observed, "
            f"count(*) FILTER (WHERE reading_status = 'missing') AS missing "
            f"FROM {self._source()}{where} GROUP BY ALL ORDER BY asset_id, bucket_start",
            [bucket, self.glob, *params])

    def anomaly_summary(self, start: datetime | None = None, end: datetime | None = None, bucket: str = "day",
                        asset_id: str | None = None, run_id: str | None = None) -> list[dict]:
        """Explicit anomaly labels (simulator-injected) per asset, period and type."""
        if bucket not in BUCKETS:
            raise ValueError(f"bucket must be one of {BUCKETS}")
        _check_window(start, end)
        if not self.has_data():
            return []
        where, params = self._where(start, end, asset_id, run_id)
        return self._rows(
            f"WITH labels AS (SELECT asset_id, event_timestamp, unnest(injected_anomalies) AS a "
            f"FROM {self._source()}{where}) "
            f"SELECT asset_id, date_trunc(?, event_timestamp) AT TIME ZONE 'UTC' AS period, "
            f"a.anomaly_type AS anomaly_type, count(*) AS labeled_events, "
            f"count(DISTINCT a.anomaly_id) AS distinct_anomalies, "
            f"min(event_timestamp) AT TIME ZONE 'UTC' AS first_labeled, "
            f"max(event_timestamp) AT TIME ZONE 'UTC' AS last_labeled "
            f"FROM labels GROUP BY ALL ORDER BY asset_id, period, anomaly_type",
            [self.glob, *params, bucket])

    def completeness(self, asset_id: str, start: datetime, end: datetime, interval_seconds: int = 60,
                     max_gaps: int = 100, run_id: str | None = None) -> dict:
        """Compare one asset's events with the expected regular grid [start, end).

        - events: messages stored for the window (observed + missing readings)
        - missing_readings: slots the producer explicitly reported as missing
        - absent_slots: expected slots with no event at all (lost or never published)
        - duplicate_events > 0 usually means several runs were mixed: pass run_id
        """
        _check_window(start, end)
        if start is None or end is None or interval_seconds <= 0:
            raise ValueError("start, end and a positive interval_seconds are required")
        expected = int((end - start).total_seconds() // interval_seconds)
        result = {"asset_id": asset_id, "run_id": run_id, "expected_slots": expected, "events": 0, "observed_readings": 0,
                  "missing_readings": 0, "absent_slots": expected, "off_grid_events": 0,
                  "duplicate_events": 0, "absent_intervals": []}
        if expected == 0:
            result["absent_slots"] = 0
            return result
        last_slot = start + timedelta(seconds=interval_seconds * (expected - 1))
        if not self.has_data():
            result["absent_intervals"] = [[start, last_slot]]
            return result
        where, params = self._where(start, end, asset_id, run_id)
        grid = "generate_series(?::TIMESTAMPTZ, ?::TIMESTAMPTZ, to_seconds(?)) AS g(slot)"
        base = (f"WITH ev AS (SELECT event_timestamp, reading_status FROM {self._source()}{where}), "
                f"slots AS (SELECT slot FROM {grid}) ")
        base_params = [self.glob, *params, start, last_slot, interval_seconds]
        counts = self._rows(
            base + "SELECT count(*) AS events, count(*) FILTER (WHERE reading_status = 'observed') AS observed, "
                   "count(*) FILTER (WHERE reading_status = 'missing') AS missing, "
                   "count(*) FILTER (WHERE event_timestamp NOT IN (SELECT slot FROM slots)) AS off_grid, "
                   "count(*) - count(DISTINCT event_timestamp) AS duplicates FROM ev", base_params)[0]
        gaps = self._rows(
            base + "SELECT min(slot) AT TIME ZONE 'UTC' AS gap_start, max(slot) AT TIME ZONE 'UTC' AS gap_end, "
                   "count(*) AS slots FROM ("
                   "  SELECT slot, epoch(slot) - ? * row_number() OVER (ORDER BY slot) AS island"
                   "  FROM slots WHERE slot NOT IN (SELECT event_timestamp FROM ev)"
                   ") GROUP BY island ORDER BY gap_start LIMIT ?",
            [*base_params, interval_seconds, int(max_gaps)])
        result.update(events=counts["events"], observed_readings=counts["observed"],
                      missing_readings=counts["missing"], off_grid_events=counts["off_grid"],
                      duplicate_events=counts["duplicates"],
                      absent_slots=sum(g["slots"] for g in gaps) if len(gaps) < max_gaps else None,
                      absent_intervals=[[g["gap_start"], g["gap_end"]] for g in gaps])
        if result["absent_slots"] is None:   # more gaps than listed: count them exactly
            result["absent_slots"] = self._rows(
                base + "SELECT count(*) AS n FROM slots WHERE slot NOT IN (SELECT event_timestamp FROM ev)",
                base_params)[0]["n"]
        return result
