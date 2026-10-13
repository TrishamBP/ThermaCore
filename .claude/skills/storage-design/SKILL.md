---
name: storage-design
description: Storage layout and write patterns for the data center operations platform — immutable source files, the append-only raw event store (Parquet landed from Kafka by the raw-sink consumer), and DuckDB curated/analytics tables with idempotent writes. Use when implementing the raw-sink consumer, choosing file layout or partitioning, writing to DuckDB, deduplicating, rebuilding curated tables, or considering a serving database.
---

# Storage Design

This merges the QuantBridge `storage-duckdb`, `storage-parquet` and `storage-supabase` skills. The source flow was DuckDB staging → Parquet export → Supabase UPSERT. Here the order changes, because data now arrives as a stream. Parquet is the **raw landing format**, and DuckDB is the **query and curated engine** over it. A serving database (Supabase/Postgres) is **not** part of the initial scope.

> **Phase 1 (implemented): SQLite.** The user chose SQLite for Phase 1. It lives at `data/datacenter_ops.db` with the schema in `db/schema.sql`. Tables: `source_files`, `source_records` (all 35 source columns, key `(source_file, source_row_number)`), `region_filtered_records` and view `filtered_source_records`, `simulation_runs`, `asset_metadata`, `telemetry_readings` (full 1,440-slot grid with explicit missing slots), `anomaly_events`, `anomaly_labels` (clean + injected values). The same rules apply as below: idempotent keyed inserts, one transaction per write, `ON CONFLICT`, and `PRAGMA foreign_keys = ON` on every connection. The DuckDB/Parquet layers below were the *proposal*.

> **Consumer store (implemented):** a separate database, SQLite `data/telemetry_store.db` or PostgreSQL, from `db/telemetry_store_{sqlite,postgres}.sql` and `src/data_engineering/storage/`.
> - Tables: `assets` keyed `(simulation_run_id, asset_id)`, `telemetry_readings` keyed `event_id` with 16 typed metrics, `anomaly_records`, `ingestion_batches`, `parquet_exports` (export ledger).
> - **Difference from the proposal:** the raw-sink consumer writing offset-named Parquet was *not* built. The database is the durability point. Parquet is exported *from the database* in ledger jobs (`part-<export_id>-NNNN.parquet`, tmp + rename, redone after crashes, only newly inserted rows), into `data/telemetry/region_code=…/event_date=…/`.
> - DuckDB reads those files in place (`src/data_engineering/analytics/duckdb_queries.py`).

## Layers

| Layer | Location | Written by | Mutability |
|---|---|---|---|
| Source | `data/raw/hyperscale-data-center-dataset/` | `dataset-ingestion` | Immutable. Checksummed |
| Raw events | `data/lake/raw/<topic>/event_date=YYYY-MM-DD/<topic>-p<partition>-<firstOffset>-<lastOffset>.parquet` | raw-sink consumer (`dco-raw-sink`) | Append-only. Never filtered |
| DLQ events | `data/lake/raw/dc.telemetry.dlq.v1/...` (same layout) | raw-sink also consumes the DLQ | Append-only |
| Curated | `data/warehouse/dco.duckdb`, schema `curated` | transformation jobs (`data-transformation`) | Rebuildable from raw |
| Analytics | same DuckDB, schema `analytics` (anomalies, incidents) | `anomaly-detection` | Idempotent by ID |

All `data/` and `logs/` paths are gitignored.

## Raw-sink consumer

1. Poll a batch (e.g. up to N events or T seconds) per partition.
2. Write the batch as a Parquet file named by **topic, partition and offset range**. Write to a temp file, then atomically rename it. The full envelope is stored. Nested `entity`/`payload` are stored as JSON strings or structs, chosen once and documented.
3. **Then** commit offsets (`kafka-streaming`: commit after the side effect is durable).
4. Reprocessing the same offsets produces the same filename and overwrites it with identical content. This is idempotent.
5. Partition directories by `event_date` derived from `event_time`, not from ingestion time.

Writing Parquet from Python needs `pyarrow`, or DuckDB's `COPY ... TO ... (FORMAT PARQUET)` from an in-memory table. Prefer whichever is already a dependency, and ask before adding one.

## DuckDB patterns

- Read raw: `read_parquet('data/lake/raw/<topic>/**/*.parquet', hive_partitioning = true)`.
- **Single writer.** A DuckDB file allows one read-write process at a time. Streaming consumers must not all write to the same `.duckdb` file concurrently. Either the raw sink writes only Parquet (preferred), or exactly one process owns the DuckDB file. Readers use `read_only=True`.
- Curated rebuild: `CREATE OR REPLACE TABLE`, or `BEGIN; DELETE FROM t WHERE event_date = ?; INSERT ...; COMMIT;`, wrapped in a transaction with `ROLLBACK` on error. This is the source's transaction discipline, kept.
- Idempotent insert by key: `INSERT INTO analytics.anomalies ... ON CONFLICT (anomaly_id) DO NOTHING` (or `DO UPDATE SET ...`). This is the source's UPSERT pattern, translated to DuckDB. Postgres-style UPSERT SQL is in `references/upsert-patterns.md` for a future serving layer.
- Typed DDL: declare tables explicitly (types, `PRIMARY KEY`). Do not rely on inference for curated tables.

## Retention and size

- Kafka retention is short (days). The raw store is the long-term record.
- If the raw store gets many small files, add a compaction job that rewrites a day's files into one, keeping all rows. Only when needed.

## When to add a serving database

Add Postgres/Supabase only when there is a concrete consumer: a multi-user dashboard, an API, or concurrent writers that DuckDB cannot serve. When that happens, reuse the staging-table + transactional UPSERT pattern in `references/upsert-patterns.md`.

## Failure modes

| Failure | Cause | Response |
|---|---|---|
| DuckDB file lock error | Two writers | Fix the ownership: one writer. Readers use read-only |
| Partial Parquet file | Crash mid-write | Prevented by temp file + rename. Offsets were not committed, so the batch is re-delivered |
| Raw count < produced count | Consumer lag or a crash | Check group lag. The gap closes after catch-up; if not, investigate |
| Disk full | Large replay | Write fails, consumer stops without committing. Free space and resume |
