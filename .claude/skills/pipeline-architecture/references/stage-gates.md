# Stage Gates

Procedural checklists for each pipeline stage. They are adapted from the QuantBridge `hooks/*.md` files. These are **process checklists for Claude and developers to follow**. They are not Claude Code hooks in `settings.json` and do not run automatically. Implement the checks in code where noted.

---

## Pre-Ingestion (before download / before producer starts)

- Required env vars are present (e.g. `KAFKA_BOOTSTRAP_SERVERS`, Kaggle credentials only if downloading). Fail with the missing variable's name. Never log values.
- Dataset files exist under `data/raw/<dataset-slug>/` and match the checksum manifest.
- Target paths (`data/lake/`, `data/warehouse/`, `logs/`) are writable.
- Log a run start: `run_id`, pipeline name, flags (`REPLAY_SPEEDUP`, `SAMPLE_MODE`, `SAMPLE_ROWS`, seed for simulator).
- Sample mode is ON by default in development.

## Pre-Produce (before events go to Kafka): was "pre-transform"

- Log the number of source rows read.
- For a **new source or changed mapping**, show 5–10 sample events (full envelope) and **wait for explicit user approval**.
- Check the payload shape: every field the contract requires exists in the source rows.
- Zero rows → log a visible warning and produce nothing. Never fail silently.

## Post-Land (after the raw-sink consumer writes a batch): was "post-ingestion"

- Log counts per topic-partition and the offset range written.
- Compare the source/produced count against the raw-landed count. A mismatch is a warning while consumers are lagging and an error at the end of the run.
- Detect schema drift (new, missing or retyped payload fields) → see On-Schema-Change.
- The DuckDB file or Parquet files open and can be queried.

## Pre-Write (before curated tables are rebuilt or upserted): was "pre-db-write"

- Input raw files exist and are non-empty for the requested range.
- Primary-key columns in the input match the target table definition.
- No leftover temp/staging tables from a previous failed run.
- This is the last safe point to abort before modifying curated tables.

## Post-Run

- Log duration, counts at each stage (source → produced → landed → validated → DLQ → anomalies), and flags.
- Mark the status explicitly: `SUCCESS`, `PARTIAL` (any stage skipped or any DLQ events), or `FAILED`. The absence of errors does not count as success.
- Report consumer-group lag at the end of the run.

## On-Failure

- Log the exception class, message, traceback, stage, `run_id`, and the topic/partition/offset if applicable.
- Batch writes: roll back the DuckDB transaction. Confirm no partial curated rows remain.
- Streaming: **do not commit the offset** of the failing event. After retries are exhausted, send it to the DLQ (see `kafka-streaming`).
- Keep the evidence. Do not delete raw Parquet, DuckDB files or logs from failed runs.
- Exit with a non-zero code. Never swallow exceptions.

## On-Schema-Change

- Log the field, the change type (added / removed / retyped), and before/after examples.
- **Added field:** non-breaking. It lands in the raw store automatically, because the payload is stored as-is. Flag it for a contract update.
- **Removed or retyped field:** breaking. Stop and flag for human review. Update the contract, the event `schema_version`, transformations and tests together.
- If a key/identity field is affected, this is critical. Redesign the keys and partitioning before continuing.
