"""Parquet history sink, fed from the database through the export ledger.

Why from the database and not directly from Kafka messages: the database commit is the
durability point (Kafka offsets are committed right after it). Parquet export happens
later, in groups of batches, and can always be redone from the stored rows. So:

- a crash after the DB commit but before the Parquet write loses nothing: the export
  row stays 'writing' (or the batches stay unassigned) and is redone on restart;
- a redone export writes the SAME file names (part-<export_id>-<n>.parquet) and simply
  replaces them, so retries never duplicate history;
- only rows *newly inserted* by a batch belong to it, so redelivered Kafka duplicates
  are never exported twice.

Files are written as <name>.parquet.tmp and atomically renamed, so readers (DuckDB) never
see a half-written file. Layout (hive-style, PARQUET_PARTITION_STRATEGY=region_date):

    data/telemetry/region_code=NA/event_date=2026-01-01/part-<export_id>-0000.parquet
"""

import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from data_engineering.logging_utils import get_logger
from data_engineering.simulation.generate_telemetry import METRICS

log = get_logger("parquet_sink")

PARQUET_SCHEMA_VERSION = "1"
# List children are named "element" (the Parquet standard), so the declared schema and the
# schema read back from a file are identical.
ANOMALY_TYPE = pa.struct([("anomaly_id", pa.string()), ("anomaly_type", pa.string()),
                          ("affected_metrics", pa.list_(pa.field("element", pa.string())))])
_TS = pa.timestamp("us", tz="UTC")
FILE_FIELDS = [
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("asset_id", pa.string(), nullable=False),
    pa.field("simulation_run_id", pa.string(), nullable=False),
    pa.field("data_center_id", pa.string(), nullable=False),
    pa.field("region", pa.string(), nullable=False),
    pa.field("region_code", pa.string(), nullable=False),
    pa.field("event_timestamp", _TS, nullable=False),
    pa.field("event_timestamp_raw", pa.string(), nullable=False),
    pa.field("slot_index", pa.int64(), nullable=False),
    pa.field("reading_status", pa.string(), nullable=False),
    pa.field("schema_version", pa.string(), nullable=False),
    pa.field("source_type", pa.string(), nullable=False),
    pa.field("timestamp_source", pa.string(), nullable=False),
    pa.field("published_from", pa.string(), nullable=False),
    pa.field("asset_type", pa.string()),
    *[pa.field(m, pa.float64()) for m in METRICS],          # null = missing reading, never 0
    pa.field("source_failure_risk_level", pa.string()),
    pa.field("source_energy_efficiency_class", pa.string()),
    pa.field("source_maintenance_status", pa.string()),
    pa.field("injected_anomalies", pa.list_(pa.field("element", ANOMALY_TYPE)), nullable=False),
    pa.field("kafka_topic", pa.string(), nullable=False),
    pa.field("kafka_partition", pa.int32(), nullable=False),
    pa.field("kafka_offset", pa.int64(), nullable=False),
    pa.field("ingested_at", _TS, nullable=False),
    pa.field("ingest_batch_id", pa.string(), nullable=False),
    pa.field("export_id", pa.string(), nullable=False),
]
PARTITION_KEYS = {"region_date": ("region_code", "event_date"), "date": ("event_date",)}
_SAFE = re.compile(r"[^A-Za-z0-9._-]")


def file_schema(partitioning: str) -> pa.Schema:
    """Columns stored inside each file; partition columns live only in the directory names."""
    keys = set(PARTITION_KEYS[partitioning])
    return pa.schema([f for f in FILE_FIELDS if f.name not in keys],
                     metadata={b"dco.parquet_schema_version": PARQUET_SCHEMA_VERSION.encode()})


def safe_partition_value(value) -> str:
    """Make a value safe for a directory name: no separators, no traversal, never empty."""
    text = _SAFE.sub("_", str(value)) if value is not None else ""
    return "unknown" if text.strip(".") == "" else text


def _partition_dir(root: Path, partitioning: str, row: dict) -> Path:
    values = {"region_code": row["region_code"], "event_date": row["event_timestamp"].date().isoformat()}
    path = root
    for key in PARTITION_KEYS[partitioning]:
        path = path / f"{key}={safe_partition_value(values[key])}"
    return path


def write_export(rows: list[dict], anomalies: dict[str, list[dict]], export_id: str, root: Path,
                 partitioning: str, max_rows_per_file: int, compression: str) -> list[dict]:
    """Write one export job. Deterministic: same rows + export_id -> same files."""
    if not re.fullmatch(r"[A-Za-z0-9._-]+", export_id):
        raise ValueError(f"unsafe export_id {export_id!r}")
    schema = file_schema(partitioning)
    columns = schema.names
    groups: dict[Path, list[dict]] = {}
    for row in rows:                                      # rows arrive ordered (region, time, asset, id)
        record = dict(row, export_id=export_id,
                      injected_anomalies=anomalies.get(row["event_id"], []))
        groups.setdefault(_partition_dir(root, partitioning, row), []).append(record)

    written = []
    for directory, group in sorted(groups.items()):
        directory.mkdir(parents=True, exist_ok=True)
        for stale in directory.glob(f"part-{export_id}-*.parquet.tmp"):
            stale.unlink()                                # leftovers of an interrupted attempt
        for part, start in enumerate(range(0, len(group), max_rows_per_file)):
            chunk = group[start:start + max_rows_per_file]
            table = pa.Table.from_pydict({c: [r.get(c) for r in chunk] for c in columns}, schema=schema)
            final = directory / f"part-{export_id}-{part:04d}.parquet"
            tmp = final.with_name(final.name + ".tmp")
            pq.write_table(table, tmp, compression=None if compression == "none" else compression)
            tmp.replace(final)                            # atomic on the same filesystem
            written.append({"path": final.relative_to(root).as_posix(), "rows": len(chunk)})
    return written


@dataclass
class ExportStats:
    exports_completed: int = 0
    rows_exported: int = 0
    export_failures: int = 0
    last_error: str | None = None
    files: list[str] = field(default_factory=list)


class ParquetExporter:
    """Moves persisted batches from the database ledger into Parquet files."""

    def __init__(self, store, root: Path, partitioning: str = "region_date", flush_rows: int = 50_000,
                 flush_interval_s: float = 60.0, max_rows_per_file: int = 250_000, compression: str = "zstd",
                 retry_backoff_s: float = 5.0, clock=time.monotonic) -> None:
        self.store, self.root = store, Path(root)
        self.partitioning, self.flush_rows, self.flush_interval_s = partitioning, flush_rows, flush_interval_s
        self.max_rows_per_file, self.compression = max_rows_per_file, compression
        self.retry_backoff_s, self.clock = retry_backoff_s, clock
        self.stats = ExportStats()
        self._next_retry_at = 0.0

    def _run(self, plan) -> bool:
        started = time.monotonic()
        try:
            rows, anomalies = self.store.export_rows(plan.export_id)
            files = write_export(rows, anomalies, plan.export_id, self.root, self.partitioning,
                                 self.max_rows_per_file, self.compression)
            self.store.complete_export(plan.export_id, files)
        except Exception as exc:   # sink failure must be visible, never silent
            self.stats.export_failures += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._next_retry_at = self.clock() + self.retry_backoff_s
            try:
                self.store.record_export_failure(plan.export_id, self.stats.last_error)
            except Exception:
                log.exception("could not record export failure in the ledger")
            log.error("parquet export failed; it stays pending and will be retried",
                      extra={"context": {"export_id": plan.export_id, "error": self.stats.last_error,
                                         "attempts": plan.attempts + 1}})
            return False
        self.stats.exports_completed += 1
        self.stats.rows_exported += len(rows)
        self.stats.files.extend(f["path"] for f in files)
        log.info("parquet export complete", extra={"context": {
            "export_id": plan.export_id, "rows": len(rows), "files": len(files),
            "seconds": round(time.monotonic() - started, 3)}})
        return True

    def _ledger_unavailable(self, exc: Exception) -> None:
        self.stats.export_failures += 1
        self.stats.last_error = f"{type(exc).__name__}: {exc}"
        self._next_retry_at = self.clock() + self.retry_backoff_s
        log.error("export ledger unavailable; parquet export postponed",
                  extra={"context": {"error": self.stats.last_error}})

    def recover(self) -> bool:
        """Redo exports left in 'writing' (crash or earlier failure). True if none remain.

        Never raises: if the database (which holds the ledger) is unavailable, the export is
        postponed and retried later; the consumer's own database writes handle the outage.
        """
        if self.clock() < self._next_retry_at:
            return False
        try:
            pending = self.store.pending_exports()
        except Exception as exc:
            self._ledger_unavailable(exc)
            return False
        for plan in pending:
            if not self._run(plan):
                return False
        return True

    def maybe_export(self, force: bool = False, deadline: float | None = None) -> None:
        """Export when enough rows are waiting, the oldest waits too long, or on shutdown (force)."""
        if not self.recover():
            return
        while deadline is None or time.monotonic() < deadline:
            try:
                pending = self.store.unexported_summary()
            except Exception as exc:
                self._ledger_unavailable(exc)
                return
            if pending["rows"] == 0:
                return
            age = (datetime.now(timezone.utc) - pending["oldest"]).total_seconds() if pending["oldest"] else 0
            if not (force or pending["rows"] >= self.flush_rows or age >= self.flush_interval_s):
                return
            try:
                plan = self.store.claim_export(self.flush_rows)
            except Exception as exc:
                self._ledger_unavailable(exc)
                return
            if plan is None or not self._run(plan):
                return
            if not force and pending["rows"] - plan.row_count < self.flush_rows:
                return

    def flush_all(self, deadline: float | None = None) -> None:
        """Shutdown: export everything persisted so far, ignoring the retry backoff once."""
        self._next_retry_at = 0.0
        self.maybe_export(force=True, deadline=deadline)

    def backlog(self) -> dict:
        """Rows waiting for export: unassigned batches plus exports still 'writing'."""
        pending = self.store.unexported_summary()
        writing = self.store.pending_exports()
        return {"unassigned_rows": pending["rows"], "unassigned_batches": pending["batches"],
                "exports_writing": len(writing), "rows_in_writing_exports": sum(p.row_count for p in writing)}
