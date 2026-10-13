---
name: data-validation
description: Data contracts and data-quality gates for the data center operations platform — per-event validation in Kafka consumers (envelope + payload schema, DLQ routing) and batch checks in DuckDB (row counts, nulls, key uniqueness, timestamp ordering/gaps, ranges, provenance). Use when writing or changing a data contract, implementing the validator consumer, adding quality checks to curated tables, or investigating bad or missing data.
---

# Data Validation

This merges the QuantBridge `validation` skill and `contracts/data-contracts.md`. The source validated once, in batch, before load. This platform validates at two layers.

| Layer | Where | On failure |
|---|---|---|
| **Per-event** | Validator consumer (`dco-validator` group) on `dc.telemetry.raw.v1` | Event → `dc.telemetry.dlq.v1` with reason headers. Commit and continue |
| **Batch** | DuckDB checks over the raw store and curated tables | Fail the run (non-zero exit) before curated/analytics tables are published |

Validation never removes data from the raw store. It decides what flows to `validated` and curated outputs.

## Contracts

- Contracts live in `contracts/`. Start one with `references/data-contract-template.md`.
- Write a contract only **after** the dataset profile has been reviewed (`dataset-ingestion`). Every column in the contract must exist in the profile.
- Implement the contract in code once, in `src/data_engineering/events/`, and use that model both in the producer (to fail fast) and in the validator. Recommended: Pydantic v2 models. This is a new dependency, so ask first. Alternative: `jsonschema`.

## Per-event checks (validator consumer)

1. The bytes deserialize as JSON. If not → DLQ (`error_type=deserialization`).
2. Envelope: required fields are present. `provenance` is in the allowed set. `event_time`/`ingestion_time` parse as UTC. `schema_version` is supported.
3. Payload: required metrics are present, with the contract type. Nulls only where the contract allows them.
4. Ranges: only **physically impossible** values fail validation here (e.g. a negative value for a quantity the contract marks non-negative). Unusual-but-possible values are **anomalies**, not invalid. Leave them to `anomaly-detection`.
5. Synthetic events have `generator`. Replay/dataset events have `source`.
6. Pass → produce to `dc.telemetry.validated.v1` (same key), then commit the input offset after delivery is confirmed.

## Batch checks (DuckDB)

| Check | Rule | Action |
|---|---|---|
| Presence | The expected raw partitions/tables exist | Fail |
| Row count | > 0. Reconciles with producer summary counts per run (`run_id`) | Fail if 0; warn on mismatch, fail at the end of the run |
| Key nulls | `event_id`, `event_time`, entity key (if any) are never null | Fail |
| Uniqueness | `event_id` is unique in curated tables (raw may contain duplicates) | Fail |
| Time sanity | `event_time` within the dataset's profiled range (replay). Report per-entity monotonicity and gaps | Fail out-of-range; report gaps |
| Units | Every metric has a unit entry (`unknown` allowed) | Fail if missing |
| Provenance | Never null. Counts per provenance reported | Fail if null |
| DLQ ratio | DLQ count / produced count | Warn above the threshold agreed with the user |

Report every check (pass and fail) as a structured log entry (`observability-reliability`).

## Null handling (adapted)

| Scenario | Rule |
|---|---|
| Null in a key field | Invalid. Never in curated tables |
| Null in a metric | Allowed only if the contract says nullable. Kept as null, never zero-filled |
| Null in a descriptive attribute | Acceptable if the identifier is populated |
| Null unit | Not acceptable. Use the explicit `unknown` |
| Null provenance | Never acceptable |

## Adding or changing a contract

1. Show the profile evidence for each field (type, nulls, range).
2. Write the contract file plus the code model. Bump `schema_version` on a breaking change.
3. Update the mapping (`data-transformation`), validator, batch checks and tests (`pipeline-testing`) together.
4. Get user approval before the contract is used.
