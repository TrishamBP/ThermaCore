"""Backend-independent telemetry store: batch persistence, idempotency, export ledger.

All SQL is written once with `?` placeholders and shared by both backends. SQLite and
PostgreSQL subclasses only supply connections, placeholder style, type conversion
(timestamps, JSON), transaction control and transient-error classification.

Every value reaches the database as a bound parameter. Identifiers (table and column
names) come only from the constants below, never from input.
"""

import time
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from data_engineering.paths import REPO_ROOT
from data_engineering.simulation.generate_telemetry import METRICS
from data_engineering.streaming.event_validation import ValidEvent

SCHEMA_VERSION = 2
SCHEMA_DESCRIPTION = ("telemetry store v2: assets, telemetry_readings, anomaly_records, ingestion_batches, "
                      "parquet_exports; recommendation_analyses, recommendations, recommendation_decisions, alerts")

READING_COLUMNS = (
    "event_id", "simulation_run_id", "asset_id", "data_center_id", "region", "region_code",
    "event_timestamp", "event_timestamp_raw", "slot_index", "reading_status", "schema_version",
    "source_type", "timestamp_source", "published_from", *METRICS, "payload_sha256",
    "kafka_topic", "kafka_partition", "kafka_offset", "ingested_at", "ingest_batch_id",
)
# Asset attributes that a newer event may update (keys and first/last timestamps are handled separately).
ASSET_ATTRIBUTES = (
    "data_center_id", "region", "region_code", "asset_type", "cooling_type", "server_age_years",
    "rack_u_position", "energy_source_primary", "source_failure_risk_level",
    "source_energy_efficiency_class", "source_maintenance_status", "source_type",
    "baseline_source_row_number", "dataset", "source_file", "source_sha256", "generator",
    "generator_version", "seed",
)
ASSET_COLUMNS = ("simulation_run_id", "asset_id", *ASSET_ATTRIBUTES,
                 "first_event_timestamp", "last_event_timestamp", "created_at", "updated_at")
ANOMALY_COLUMNS = ("anomaly_id", "event_id", "simulation_run_id", "asset_id", "event_timestamp",
                   "anomaly_type", "label_source", "affected_metrics_json", "ingest_batch_id", "created_at")
BATCH_COLUMNS = ("batch_id", "consumer_group", "source_topic", "started_at", "persisted_at", "status",
                 "messages_received", "events_valid", "rows_inserted", "rows_deduplicated",
                 "rows_conflicting", "anomaly_rows_inserted", "events_rejected", "offsets_json")
TIMESTAMP_COLUMNS = frozenset({
    "event_timestamp", "ingested_at", "first_event_timestamp", "last_event_timestamp", "created_at",
    "updated_at", "started_at", "persisted_at", "committed_at", "completed_at", "applied_at",
    "window_start", "window_end", "decided_at", "first_ts", "last_ts"})
JSON_COLUMNS = frozenset({"offsets_json", "files_json", "affected_metrics_json", "trace_json", "payload_json"})
ANALYSIS_COLUMNS = ("analysis_id", "simulation_run_id", "asset_id", "window_start", "window_end", "status",
                    "trace_json", "created_at")
RECOMMENDATION_COLUMNS = ("recommendation_id", "analysis_id", "simulation_run_id", "asset_id",
                          "recommendation_type", "status", "severity", "window_start", "window_end", "payload_json",
                          "created_at")
ALERT_COLUMNS = ("alert_id", "analysis_id", "simulation_run_id", "asset_id", "rule_id", "severity", "window_start",
                 "window_end", "payload_json", "created_at")
DECISION_COLUMNS = ("decision_id", "recommendation_id", "decision", "reviewer", "note", "decided_at")
_IN_CHUNK = 500


class StoreError(RuntimeError):
    pass


class StoreNotInitialized(StoreError):
    pass


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class KafkaPosition:
    topic: str
    partition: int
    offset: int


@dataclass
class PreparedEvent:
    """One validated event mapped onto the database tables."""
    event_id: str
    payload_sha256: str
    event_timestamp: datetime
    reading: dict
    asset: dict
    anomalies: list[dict]
    position: KafkaPosition


def prepare_event(valid: ValidEvent, position: KafkaPosition) -> PreparedEvent:
    """Map an event onto table rows. Batch IDs and ingest times are filled in by persist_batch."""
    e = valid.event
    attrs, labels, prov = e["asset_attributes"], e["labels"], e["provenance"]
    reading = {
        "event_id": e["event_id"], "simulation_run_id": e["simulation_run_id"], "asset_id": e["asset_id"],
        "data_center_id": e["data_center_id"], "region": e["region"], "region_code": e["region_code"],
        "event_timestamp": valid.event_timestamp, "event_timestamp_raw": e["event_timestamp"],
        "slot_index": e["slot_index"], "reading_status": e["reading_status"],
        "schema_version": e["schema_version"], "source_type": e["source_type"],
        "timestamp_source": e["timestamp_source"], "published_from": prov["published_from"],
        **{m: e["metrics"][m] for m in METRICS},     # None stays None: missing is never 0
        "payload_sha256": valid.payload_sha256,
        "kafka_topic": position.topic, "kafka_partition": position.partition, "kafka_offset": position.offset,
    }
    asset = {
        "simulation_run_id": e["simulation_run_id"], "asset_id": e["asset_id"],
        "data_center_id": e["data_center_id"], "region": e["region"], "region_code": e["region_code"],
        "asset_type": attrs["server_type"], "cooling_type": attrs["cooling_type"],
        "server_age_years": attrs["server_age_years"], "rack_u_position": attrs["rack_u_position"],
        "energy_source_primary": attrs["energy_source_primary"],
        "source_failure_risk_level": labels["source_failure_risk_level"],
        "source_energy_efficiency_class": labels["source_energy_efficiency_class"],
        "source_maintenance_status": labels["source_maintenance_status"],
        "source_type": e["source_type"], "baseline_source_row_number": prov["baseline_source_row_number"],
        "dataset": prov["dataset"], "source_file": prov["source_file"], "source_sha256": prov["source_sha256"],
        "generator": prov["generator"], "generator_version": prov["generator_version"], "seed": prov["seed"],
    }
    anomalies = [{
        "anomaly_id": a["anomaly_id"], "event_id": e["event_id"], "simulation_run_id": e["simulation_run_id"],
        "asset_id": e["asset_id"], "event_timestamp": valid.event_timestamp, "anomaly_type": a["anomaly_type"],
        "label_source": "simulator_injected", "affected_metrics_json": list(a["affected_metrics"]),
    } for a in labels["injected_anomalies"]]
    return PreparedEvent(e["event_id"], valid.payload_sha256, valid.event_timestamp, reading, asset,
                         anomalies, position)


@dataclass
class BatchRecord:
    batch_id: str
    consumer_group: str
    source_topic: str
    started_at: datetime
    messages_received: int
    events_rejected: int
    offsets: dict[int, list[int]]          # partition -> [first_offset, last_offset]


@dataclass
class PersistResult:
    inserted: int = 0
    deduplicated: int = 0
    conflicting: int = 0
    anomaly_rows: int = 0
    conflict_event_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ExportPlan:
    export_id: str
    batch_count: int
    row_count: int
    attempts: int


class SqlTelemetryStore(ABC):
    backend: str
    placeholder = "?"
    schema_file: Path

    read_only = False
    query_timeout_s: float | None = None

    def __init__(self) -> None:
        self._deadline: float | None = None
        self.conn = self._connect()

    # ----- backend hooks -----
    @abstractmethod
    def _connect(self): ...

    @abstractmethod
    def is_transient(self, exc: BaseException) -> bool: ...

    @abstractmethod
    def _begin(self) -> None: ...

    @abstractmethod
    def _commit(self) -> None: ...

    @abstractmethod
    def _rollback(self) -> None: ...

    @abstractmethod
    def _apply_schema(self, ddl: str) -> None: ...

    @abstractmethod
    def _greatest(self, a: str, b: str) -> str: ...

    @abstractmethod
    def _least(self, a: str, b: str) -> str: ...

    @abstractmethod
    def _ts_in(self, value: datetime): ...

    @abstractmethod
    def _ts_out(self, value) -> datetime: ...

    @abstractmethod
    def _json_in(self, value): ...

    @abstractmethod
    def _json_out(self, value): ...

    @abstractmethod
    def _table_exists(self, table: str) -> bool: ...

    @abstractmethod
    def _columns(self, table: str) -> list[str]: ...

    def describe(self) -> str:
        return self.backend

    # ----- plumbing -----
    def _sql(self, sql: str) -> str:
        return sql if self.placeholder == "?" else sql.replace("?", self.placeholder)

    def _param(self, column: str, value):
        if value is None:
            return None
        if column in TIMESTAMP_COLUMNS and isinstance(value, datetime):
            return self._ts_in(value)
        if column in JSON_COLUMNS:
            return self._json_in(value)
        return value

    def _row(self, columns, record: dict) -> tuple:
        return tuple(self._param(c, record.get(c)) for c in columns)

    def _arm_timeout(self) -> None:
        self._deadline = time.monotonic() + self.query_timeout_s if self.query_timeout_s else None

    def _fetchall(self, sql: str, params=()) -> list[dict]:
        self._arm_timeout()
        cur = self.conn.execute(self._sql(sql), params)
        names = [d[0] for d in cur.description]
        out = []
        for row in cur.fetchall():
            record = dict(zip(names, row))
            for k in record:
                if k in TIMESTAMP_COLUMNS and record[k] is not None:
                    record[k] = self._ts_out(record[k])
                elif k in JSON_COLUMNS and record[k] is not None:
                    record[k] = self._json_out(record[k])
            out.append(record)
        return out

    def _execute(self, sql: str, params=()):
        self._arm_timeout()
        return self.conn.execute(self._sql(sql), params)

    def _executemany(self, sql: str, rows: list[tuple]) -> None:
        if rows:
            self.conn.cursor().executemany(self._sql(sql), rows)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self._begin()
        try:
            yield
        except BaseException:
            self._rollback()
            raise
        else:
            self._commit()

    @staticmethod
    def _insert_sql(table: str, columns, on_conflict: str | None = None) -> str:
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})"
        return f"{sql} ON CONFLICT {on_conflict}" if on_conflict else sql

    # ----- schema -----
    def initialize(self) -> None:
        """Create tables if needed and record the schema version. Safe to run repeatedly."""
        if self._table_exists("telemetry_readings") and "event_id" not in self._columns("telemetry_readings"):
            raise StoreError("this database already has a different telemetry_readings table "
                             "(the Phase 1 database?). Point the consumer at its own database file.")
        self._apply_schema(self.schema_file.read_text(encoding="utf-8"))
        with self.transaction():
            self._execute(self._insert_sql("schema_migrations", ("version", "description", "applied_at"),
                                           "(version) DO NOTHING"),
                          (SCHEMA_VERSION, SCHEMA_DESCRIPTION, self._ts_in(utcnow())))

    def schema_version(self) -> int | None:
        if not self._table_exists("schema_migrations"):
            return None
        rows = self._fetchall("SELECT MAX(version) AS v FROM schema_migrations")
        return rows[0]["v"] if rows else None

    def check_initialized(self) -> None:
        version = self.schema_version()
        if version != SCHEMA_VERSION:
            raise StoreNotInitialized(
                f"telemetry store schema version is {version!r}, expected {SCHEMA_VERSION}. "
                "Run `uv run python db/init_telemetry_store.py` first.")

    # ----- batch persistence -----
    def _existing_hashes(self, event_ids: list[str]) -> dict[str, str]:
        found = {}
        for i in range(0, len(event_ids), _IN_CHUNK):
            chunk = event_ids[i:i + _IN_CHUNK]
            marks = ", ".join("?" for _ in chunk)
            for row in self._fetchall(
                    f"SELECT event_id, payload_sha256 FROM telemetry_readings WHERE event_id IN ({marks})", chunk):
                found[row["event_id"]] = row["payload_sha256"]
        return found

    def _upsert_assets_sql(self) -> str:
        newer = "excluded.last_event_timestamp >= assets.last_event_timestamp"
        sets = [f"{c} = CASE WHEN {newer} THEN excluded.{c} ELSE assets.{c} END" for c in ASSET_ATTRIBUTES]
        sets += [
            f"first_event_timestamp = {self._least('assets.first_event_timestamp', 'excluded.first_event_timestamp')}",
            f"last_event_timestamp = {self._greatest('assets.last_event_timestamp', 'excluded.last_event_timestamp')}",
            "updated_at = excluded.updated_at",
        ]
        return self._insert_sql("assets", ASSET_COLUMNS,
                                "(simulation_run_id, asset_id) DO UPDATE SET " + ", ".join(sets))

    def persist_batch(self, batch: BatchRecord, events: list[PreparedEvent]) -> PersistResult:
        """Write one consumer batch in a single transaction (all or nothing).

        Duplicates (same event_id) are never inserted twice. Same payload fingerprint ->
        counted as deduplicated. Different fingerprint -> counted as conflicting; the first
        stored version wins and is not overwritten.
        """
        result = PersistResult()
        now = utcnow()
        first_seen: dict[str, str] = {}
        unique: list[PreparedEvent] = []
        for ev in events:                                   # duplicates inside the batch
            if ev.event_id in first_seen:
                if first_seen[ev.event_id] == ev.payload_sha256:
                    result.deduplicated += 1
                else:
                    result.conflicting += 1
                    result.conflict_event_ids.append(ev.event_id)
                continue
            first_seen[ev.event_id] = ev.payload_sha256
            unique.append(ev)

        with self.transaction():
            existing = self._existing_hashes([e.event_id for e in unique])
            new = []
            for ev in unique:                               # duplicates of earlier batches
                stored = existing.get(ev.event_id)
                if stored is None:
                    new.append(ev)
                elif stored == ev.payload_sha256:
                    result.deduplicated += 1
                else:
                    result.conflicting += 1
                    result.conflict_event_ids.append(ev.event_id)
            result.inserted = len(new)
            result.anomaly_rows = sum(len(ev.anomalies) for ev in new)

            batch_row = {
                "batch_id": batch.batch_id, "consumer_group": batch.consumer_group,
                "source_topic": batch.source_topic, "started_at": batch.started_at, "persisted_at": now,
                "status": "persisted", "messages_received": batch.messages_received,
                "events_valid": len(events), "rows_inserted": result.inserted,
                "rows_deduplicated": result.deduplicated, "rows_conflicting": result.conflicting,
                "anomaly_rows_inserted": result.anomaly_rows, "events_rejected": batch.events_rejected,
                "offsets_json": {str(p): r for p, r in sorted(batch.offsets.items())},
            }
            self._execute(self._insert_sql("ingestion_batches", BATCH_COLUMNS), self._row(BATCH_COLUMNS, batch_row))

            assets: dict[tuple, dict] = {}
            for ev in new:
                key = (ev.asset["simulation_run_id"], ev.asset["asset_id"])
                current = assets.get(key)
                if current is None:
                    assets[key] = {**ev.asset, "first_event_timestamp": ev.event_timestamp,
                                   "last_event_timestamp": ev.event_timestamp, "created_at": now, "updated_at": now}
                else:
                    current["first_event_timestamp"] = min(current["first_event_timestamp"], ev.event_timestamp)
                    if ev.event_timestamp >= current["last_event_timestamp"]:
                        current.update(ev.asset)
                        current["last_event_timestamp"] = ev.event_timestamp
            self._executemany(self._upsert_assets_sql(), [self._row(ASSET_COLUMNS, a) for a in assets.values()])

            self._executemany(
                self._insert_sql("telemetry_readings", READING_COLUMNS, "(event_id) DO NOTHING"),
                [self._row(READING_COLUMNS, {**ev.reading, "ingested_at": now, "ingest_batch_id": batch.batch_id})
                 for ev in new])
            self._executemany(
                self._insert_sql("anomaly_records", ANOMALY_COLUMNS, "(anomaly_id, event_id) DO NOTHING"),
                [self._row(ANOMALY_COLUMNS, {**a, "ingest_batch_id": batch.batch_id, "created_at": now})
                 for ev in new for a in ev.anomalies])
        return result

    def mark_batch(self, batch_id: str, status: str, dlq_published: int) -> None:
        with self.transaction():
            self._execute("UPDATE ingestion_batches SET status = ?, committed_at = ?, dlq_published = ? "
                          "WHERE batch_id = ?",
                          (status, self._ts_in(utcnow()) if status == "committed" else None, dlq_published, batch_id))

    # ----- Parquet export ledger -----
    def unexported_summary(self) -> dict:
        row = self._fetchall(
            "SELECT COUNT(*) AS batches, COALESCE(SUM(rows_inserted), 0) AS row_count, "
            "MIN(persisted_at) AS persisted_at FROM ingestion_batches WHERE export_id IS NULL AND rows_inserted > 0")[0]
        return {"batches": int(row["batches"]), "rows": int(row["row_count"]), "oldest": row["persisted_at"]}

    def claim_export(self, max_rows: int) -> ExportPlan | None:
        """Assign unexported batches (oldest first, ~max_rows rows) to a new export job."""
        with self.transaction():
            candidates = self._fetchall(
                "SELECT batch_id, rows_inserted FROM ingestion_batches WHERE export_id IS NULL AND rows_inserted > 0 "
                "ORDER BY persisted_at, batch_id")
            chosen, rows = [], 0
            for c in candidates:
                if chosen and rows + c["rows_inserted"] > max_rows:
                    break
                chosen.append(c["batch_id"])
                rows += c["rows_inserted"]
            if not chosen:
                return None
            export_id = f"exp-{utcnow():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
            self._execute(self._insert_sql("parquet_exports", ("export_id", "status", "created_at",
                                                               "batch_count", "row_count", "attempts")),
                          (export_id, "writing", self._ts_in(utcnow()), len(chosen), rows, 0))
            for i in range(0, len(chosen), _IN_CHUNK):
                chunk = chosen[i:i + _IN_CHUNK]
                marks = ", ".join("?" for _ in chunk)
                self._execute(f"UPDATE ingestion_batches SET export_id = ? WHERE export_id IS NULL "
                              f"AND batch_id IN ({marks})", (export_id, *chunk))
        return ExportPlan(export_id, len(chosen), rows, 0)

    def pending_exports(self) -> list[ExportPlan]:
        return [ExportPlan(r["export_id"], r["batch_count"], r["row_count"], r["attempts"])
                for r in self._fetchall("SELECT export_id, batch_count, row_count, attempts FROM parquet_exports "
                                        "WHERE status = 'writing' ORDER BY created_at, export_id")]

    def export_rows(self, export_id: str) -> tuple[list[dict], dict[str, list[dict]]]:
        """Rows of one export job, in a deterministic order, plus anomaly labels by event_id."""
        rows = self._fetchall(
            "SELECT r.*, a.asset_type, a.source_failure_risk_level, a.source_energy_efficiency_class, "
            "a.source_maintenance_status "
            "FROM telemetry_readings r "
            "JOIN ingestion_batches b ON b.batch_id = r.ingest_batch_id "
            "JOIN assets a ON a.simulation_run_id = r.simulation_run_id AND a.asset_id = r.asset_id "
            "WHERE b.export_id = ? ORDER BY r.region_code, r.event_timestamp, r.asset_id, r.event_id", (export_id,))
        anomalies: dict[str, list[dict]] = {}
        for a in self._fetchall(
                "SELECT ar.event_id, ar.anomaly_id, ar.anomaly_type, ar.affected_metrics_json "
                "FROM anomaly_records ar JOIN ingestion_batches b ON b.batch_id = ar.ingest_batch_id "
                "WHERE b.export_id = ? ORDER BY ar.event_id, ar.anomaly_id", (export_id,)):
            anomalies.setdefault(a["event_id"], []).append(
                {"anomaly_id": a["anomaly_id"], "anomaly_type": a["anomaly_type"],
                 "affected_metrics": list(a["affected_metrics_json"])})
        return rows, anomalies

    def complete_export(self, export_id: str, files: list[dict]) -> None:
        with self.transaction():
            self._execute("UPDATE parquet_exports SET status = 'done', completed_at = ?, files_json = ?, "
                          "attempts = attempts + 1, last_error = NULL WHERE export_id = ?",
                          (self._ts_in(utcnow()), self._json_in(files), export_id))

    def record_export_failure(self, export_id: str, error: str) -> None:
        with self.transaction():
            self._execute("UPDATE parquet_exports SET attempts = attempts + 1, last_error = ? WHERE export_id = ?",
                          (error[:500], export_id))

    # ----- queries -----
    def readings_between(self, asset_id: str, start: datetime, end: datetime, limit: int = 10_000,
                         simulation_run_id: str | None = None) -> list[dict]:
        """Readings of one asset with start <= event_timestamp < end, oldest first (uses the
        (asset_id, event_timestamp) index). Asset IDs repeat across runs: pass simulation_run_id."""
        run_clause = " AND simulation_run_id = ?" if simulation_run_id is not None else ""
        run_param = (simulation_run_id,) if simulation_run_id is not None else ()
        return self._fetchall(
            "SELECT * FROM telemetry_readings WHERE asset_id = ? AND event_timestamp >= ? AND event_timestamp < ?"
            + run_clause + " ORDER BY event_timestamp, event_id LIMIT ?",
            (asset_id, self._ts_in(start), self._ts_in(end), *run_param, int(limit)))

    # ----- read-only queries used by the cooling state estimator -----
    def latest_run_id(self) -> str | None:
        """The simulation run that most recently received new readings (assets.updated_at)."""
        rows = self._fetchall("SELECT simulation_run_id FROM assets ORDER BY updated_at DESC, simulation_run_id DESC "
                              "LIMIT 1")
        return rows[0]["simulation_run_id"] if rows else None

    def runs_summary(self) -> list[dict]:
        """One row per simulation run: seed, asset count, event-time span (from the assets table)."""
        return [dict(r, first_event=self._ts_out(r["first_event"]), last_event=self._ts_out(r["last_event"]))
                for r in self._fetchall(
                    "SELECT simulation_run_id, seed, COUNT(*) AS assets, MIN(first_event_timestamp) AS first_event, "
                    "MAX(last_event_timestamp) AS last_event FROM assets GROUP BY simulation_run_id, seed "
                    "ORDER BY simulation_run_id")]

    def anomaly_event_times(self, simulation_run_id: str) -> list[dict]:
        """(asset_id, event_timestamp) of every injected-anomaly label of a run (for evaluation segments)."""
        return self._fetchall("SELECT DISTINCT asset_id, event_timestamp FROM anomaly_records "
                              "WHERE simulation_run_id = ?", (simulation_run_id,))

    def run_assets(self, simulation_run_id: str) -> list[dict]:
        return self._fetchall("SELECT * FROM assets WHERE simulation_run_id = ? ORDER BY asset_id",
                              (simulation_run_id,))

    def latest_observed(self, simulation_run_id: str, as_of: datetime | None = None,
                        asset_id: str | None = None) -> list[dict]:
        """Latest observed reading per asset (reading_status = 'observed') at or before as_of. One indexed
        aggregate query plus the matching rows; same-timestamp duplicates are returned and collapsed by the caller."""
        params: list = [simulation_run_id]
        clause = ""
        if as_of is not None:
            clause += " AND event_timestamp <= ?"
            params.append(self._ts_in(as_of))
        if asset_id is not None:
            clause += " AND asset_id = ?"
            params.append(asset_id)
        cols = ", ".join(f"r.{c}" for c in ("event_id", "asset_id", "event_timestamp", "ingested_at", "reading_status",
                                             *METRICS))
        return self._fetchall(
            f"SELECT {cols} FROM telemetry_readings r JOIN (SELECT asset_id, MAX(event_timestamp) AS ts "
            f"FROM telemetry_readings WHERE simulation_run_id = ? AND reading_status = 'observed'{clause} "
            "GROUP BY asset_id) m ON r.asset_id = m.asset_id AND r.event_timestamp = m.ts "
            "WHERE r.simulation_run_id = ? AND r.reading_status = 'observed' ORDER BY r.asset_id, r.ingested_at",
            (*params, simulation_run_id))

    def run_readings_window(self, simulation_run_id: str, start: datetime, end: datetime, limit: int) -> list[dict]:
        """All assets of one run with start < event_timestamp <= end, at most `limit` rows."""
        cols = ", ".join(("event_id", "asset_id", "event_timestamp", "ingested_at", "reading_status", *METRICS))
        return self._fetchall(
            f"SELECT {cols} FROM telemetry_readings WHERE simulation_run_id = ? AND event_timestamp > ? "
            "AND event_timestamp <= ? ORDER BY asset_id, event_timestamp, ingested_at LIMIT ?",
            (simulation_run_id, self._ts_in(start), self._ts_in(end), int(limit)))

    def anomaly_labels_between(self, simulation_run_id: str, start: datetime, end: datetime) -> list[dict]:
        return self._fetchall(
            "SELECT asset_id, anomaly_type, COUNT(*) AS readings, MIN(event_timestamp) AS first_ts, "
            "MAX(event_timestamp) AS last_ts FROM anomaly_records WHERE simulation_run_id = ? AND event_timestamp > ? "
            "AND event_timestamp <= ? GROUP BY asset_id, anomaly_type ORDER BY asset_id, anomaly_type",
            (simulation_run_id, self._ts_in(start), self._ts_in(end)))

    def readings_window(self, simulation_run_id: str, asset_id: str, start: datetime, end: datetime) -> list[dict]:
        """Readings of one asset of one run with start < event_timestamp <= end (index: asset_id, event_timestamp)."""
        cols = ", ".join(("event_id", "event_timestamp", "ingested_at", "reading_status", *METRICS))
        return self._fetchall(
            f"SELECT {cols} FROM telemetry_readings WHERE asset_id = ? AND simulation_run_id = ? "
            "AND event_timestamp > ? AND event_timestamp <= ? ORDER BY event_timestamp, ingested_at, event_id",
            (asset_id, simulation_run_id, self._ts_in(start), self._ts_in(end)))

    # ----- recommendation analyses (schema v2) -----
    def save_analysis(self, analysis: dict, recommendations: list[dict], alerts: list[dict] = ()) -> list[str]:
        """Insert an analysis and its recommendations in one transaction. A recommendation whose
        (run, asset, type, window_end) already exists is not inserted again; returns the ids of
        those already-existing recommendations."""
        existing = []
        with self.transaction():
            self._execute(self._insert_sql("recommendation_analyses", ANALYSIS_COLUMNS),
                          self._row(ANALYSIS_COLUMNS, analysis))
            for rec in recommendations:
                cur = self._execute(self._insert_sql("recommendations", RECOMMENDATION_COLUMNS,
                                                     "(simulation_run_id, asset_id, recommendation_type, window_end) "
                                                     "DO NOTHING"), self._row(RECOMMENDATION_COLUMNS, rec))
                if cur.rowcount != 1:
                    existing.append(rec["recommendation_id"])
            for alert in alerts:
                self._execute(self._insert_sql("alerts", ALERT_COLUMNS,
                                               "(simulation_run_id, asset_id, rule_id, window_end) DO NOTHING"),
                              self._row(ALERT_COLUMNS, alert))
        return existing

    def get_analysis(self, analysis_id: str) -> dict | None:
        rows = self._fetchall("SELECT * FROM recommendation_analyses WHERE analysis_id = ?", (analysis_id,))
        return rows[0] if rows else None

    def _filtered(self, table: str, id_column: str, filters: dict, start: datetime | None, end: datetime | None,
                  cursor: tuple | None, limit: int) -> list[dict]:
        """Newest first by (created_at, id); keyset pagination: cursor = (created_at, id) of the last row seen.
        start/end filter the observation window end (window_end). Column names come from constants only."""
        clauses, params = [], []
        for column, value in filters.items():
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if start is not None:
            clauses.append("window_end >= ?")
            params.append(self._ts_in(start))
        if end is not None:
            clauses.append("window_end <= ?")
            params.append(self._ts_in(end))
        if cursor is not None:
            clauses.append(f"(created_at < ? OR (created_at = ? AND {id_column} > ?))")
            ts = self._ts_in(cursor[0])
            params += [ts, ts, cursor[1]]
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._fetchall(f"SELECT * FROM {table}{where} ORDER BY created_at DESC, {id_column} LIMIT ?",
                              (*params, int(limit)))

    def list_recommendations(self, *, simulation_run_id: str | None = None, asset_id: str | None = None,
                             recommendation_type: str | None = None, status: str | None = None,
                             severity: str | None = None, start: datetime | None = None, end: datetime | None = None,
                             cursor: tuple | None = None, limit: int = 50) -> list[dict]:
        return self._filtered("recommendations", "recommendation_id",
                              {"simulation_run_id": simulation_run_id, "asset_id": asset_id,
                               "recommendation_type": recommendation_type, "status": status, "severity": severity},
                              start, end, cursor, limit)

    def list_alerts(self, *, simulation_run_id: str | None = None, asset_id: str | None = None,
                    rule_id: str | None = None, severity: str | None = None, start: datetime | None = None,
                    end: datetime | None = None, cursor: tuple | None = None, limit: int = 50) -> list[dict]:
        return self._filtered("alerts", "alert_id", {"simulation_run_id": simulation_run_id, "asset_id": asset_id,
                                                     "rule_id": rule_id, "severity": severity},
                              start, end, cursor, limit)

    def get_alert(self, alert_id: str) -> dict | None:
        rows = self._fetchall("SELECT * FROM alerts WHERE alert_id = ?", (alert_id,))
        return rows[0] if rows else None

    def get_recommendation(self, recommendation_id: str) -> dict | None:
        rows = self._fetchall("SELECT * FROM recommendations WHERE recommendation_id = ?", (recommendation_id,))
        return rows[0] if rows else None

    def add_decision(self, decision: dict) -> None:
        with self.transaction():
            self._execute(self._insert_sql("recommendation_decisions", DECISION_COLUMNS),
                          self._row(DECISION_COLUMNS, decision))

    def decisions(self, recommendation_id: str) -> list[dict]:
        return self._fetchall("SELECT * FROM recommendation_decisions WHERE recommendation_id = ? "
                              "ORDER BY decided_at, decision_id", (recommendation_id,))

    def counts(self) -> dict[str, int]:
        tables = ("assets", "telemetry_readings", "anomaly_records", "ingestion_batches", "parquet_exports")
        return {t: int(self._fetchall(f"SELECT COUNT(*) AS n FROM {t}")[0]["n"]) for t in tables}

    def batch(self, batch_id: str) -> dict | None:
        rows = self._fetchall("SELECT * FROM ingestion_batches WHERE batch_id = ?", (batch_id,))
        return rows[0] if rows else None

    def reconnect(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = self._connect()

    def close(self) -> None:
        self.conn.close()


def open_store(settings, *, search_path: str | None = None, connect_timeout_s: int = 10,
               sqlite_check_same_thread: bool = True, read_only: bool = False,
               query_timeout_s: float | None = None) -> SqlTelemetryStore:
    """Build the store selected by TELEMETRY_DB_BACKEND (does not initialize the schema).

    connect_timeout_s applies to PostgreSQL (SQLite opens a local file); sqlite_check_same_thread see
    SqliteTelemetryStore.
    """
    if settings.db_backend == "sqlite":
        from data_engineering.storage.sqlite_store import SqliteTelemetryStore
        return SqliteTelemetryStore(settings.resolve_path(settings.sqlite_path),
                                    check_same_thread=sqlite_check_same_thread, read_only=read_only,
                                    query_timeout_s=query_timeout_s)
    from data_engineering.storage.postgres_store import PostgresTelemetryStore
    return PostgresTelemetryStore(settings.postgres_url, search_path=search_path, connect_timeout_s=connect_timeout_s,
                                  read_only=read_only, query_timeout_s=query_timeout_s)


SCHEMA_DIR = REPO_ROOT / "db"
