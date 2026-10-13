---
name: data-transformation
description: Normalize and transform data center dataset rows and Kafka events — canonical snake_case field names, UTC timestamps, explicit units, null preservation, row-to-event mapping, and derived/rolling features built in DuckDB or Python. Use when mapping dataset columns into the event envelope, writing transformations from the raw store into curated tables, adding derived metrics, or debugging type, timestamp, or unit problems.
---

# Data Transformation

This merges the QuantBridge `normalization`, `transformation` and `feature-engineering` skills. In the source, "feature engineering" was also deterministic normalization, so all three now live here as two passes.

1. **Normalization**: structural consistency (names, types, time, units, nulls). This runs in the producer, when building the envelope, and again in curated SQL.
2. **Transformation and derivation**: the curated tables and derived metrics, built *after* raw load (the "T" of ELT), in DuckDB SQL by default.

## Normalization rules

| Concern | Rule |
|---|---|
| Field names | Map source column names to canonical `snake_case`. Keep the mapping in one place (`src/data_engineering/events/mapping.py` or the contract) with the original name next to it. Source column names are recorded in `docs/dataset/README.md` |
| Timestamps | Parse with the format the profile found. Convert to **UTC ISO-8601**. If the source timezone is not stated, do not guess silently: use the user-approved assumption and set `event_time_source: "dataset_tz_assumed"` |
| Numerics | Use `float` for telemetry measurements. `Decimal` (used in the source for financial quantities) is not needed unless exact decimal arithmetic matters. Never round at normalization time |
| Units | Carry units explicitly in `units` per metric. Convert only with an explicit conversion table, and keep the original unit in lineage. If a unit is unknown, use `unknown`. **Never infer units from what values "look like"**. Units implied by column names (e.g. `_kw`, `_c`) are recorded as inferred-from-name |
| Nulls | Preserve them. Never coerce null to `0`, `""`, or a fill value at normalization. Imputation, if ever, is a derived step and is labeled |
| Booleans/categoricals | Normalize case and whitespace only. Keep the original labels. Mapping to canonical categories is an explicit, reviewed table |
| Unknown columns | If the source contains columns not in the contract, keep them in the raw payload and log them as schema drift (see `pipeline-architecture/references/stage-gates.md`) |

Normalization failures (unparseable timestamp or number in a required field) are **contract violations**. In the producer, log and count them per the agreed policy. In a consumer, send the event to the DLQ. Never coerce bad values into plausible ones.

## Row → event mapping

- Build the envelope exactly as defined in `kafka-streaming/references/event-envelope.md`.
- Set `event_time` from the data, `ingestion_time` at send time, and `provenance` from the producer mode.
- Use only the columns listed in the data contract. Do not add fields "commonly found in data centers" that the dataset lacks.

## Transformations over the raw store

Default engine: **DuckDB SQL** reading raw Parquet (see `storage-design`). Pattern:

```sql
CREATE OR REPLACE TABLE curated.telemetry AS
SELECT
    event_id,
    provenance,
    CAST(event_time AS TIMESTAMPTZ)          AS event_time,
    CAST(ingestion_time AS TIMESTAMPTZ)      AS ingestion_time,
    entity ->> '<entity_col>'                AS <entity_col>,     -- placeholder
    CAST(payload ->> '<metric_col>' AS DOUBLE) AS <metric_col>    -- placeholder
FROM read_parquet('data/lake/raw/dc.telemetry.raw.v1/**/*.parquet', hive_partitioning = true)
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingestion_time) = 1;  -- dedup replays
```

- Every curated table is **rebuildable** from raw (`CREATE OR REPLACE`, or a delete+insert of a time partition). No hand edits.
- Keep `provenance` and `event_id` on every curated and derived row.

## Derived metrics

- Examples: rolling mean/std per entity and metric, rate of change, gaps between observations, ratios between metrics. Build a ratio **only if both inputs exist** in the dataset. Example: a PUE-style ratio needs both facility power and IT power columns.
- Compute them over `event_time` windows (`RANGE BETWEEN INTERVAL ... PRECEDING`), not over row counts, unless the data is strictly regular.
- Name derived columns descriptively (`<metric>_rolling_mean_1h`) and document the formula next to the SQL.
- Derived values are not observations. Downstream outputs (anomalies, reports) must cite the underlying observed values too.

## Before/after review

When creating or changing a mapping or transformation, show 5–10 input rows next to their output rows and wait for approval. This is required by the project rules in `CLAUDE.md`.

## Failure modes

| Failure | Cause | Response |
|---|---|---|
| Unparseable timestamp | Format not seen during profiling | Required field: DLQ/fail. Log the raw value. Re-profile |
| Non-numeric in a metric | Data quality issue | Null if the contract allows nulls, otherwise DLQ. Log the context |
| Unit mismatch across files | Different sources or units | Stop. Agree on a conversion table with the user |
| Duplicate `event_id` in raw | Replays, at-least-once delivery | Expected. Dedup in curated SQL as above |
