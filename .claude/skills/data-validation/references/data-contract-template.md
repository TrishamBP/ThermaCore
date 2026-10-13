# Data Contract: `<contract name>` (schema_version `<n>`)

> This template replaces QuantBridge `contracts/data-contracts.md`. That file's tables were EIA-specific and were **not** carried over. Fill this in **only from the reviewed dataset profile** (`docs/dataset/profile.json`, `docs/dataset/README.md`).

## Scope

- Source file(s): `<file>` (sha256 from `MANIFEST.json`)
- Grain: one row/event = `<...>`
- Topic(s): `dc.telemetry.raw.v1` → `dc.telemetry.validated.v1`
- Curated table: `curated.<table>`
- Producer mode / provenance: `replay` | `synthetic` | `dataset`

## Keys

| Purpose | Field(s) | Evidence |
|---|---|---|
| Event identity | `event_id` = sha256(`dataset|file|row_number`) | deterministic |
| Kafka partition key | `<entity_col>`, or the documented fallback | profile: distinct=`<n>`, nulls=0 |
| Curated uniqueness | `event_id` | — |

## Fields

| Canonical name | Source column | Type | Nullable | Unit (+ source of unit) | Allowed range (physical limits only) | Evidence |
|---|---|---|---|---|---|---|
| `event_time` | `<timestamp_col>` | timestamptz (UTC) | no | — | profiled min..max | parse_rate=`<x>` |
| `<metric>` | `<Source Col>` | double | yes/no | `<unit>` (column name / docs / unknown) | `>= 0` if physically required | min/max/nulls |

## Not available in this source

List signals that are absent and must not be fabricated (e.g. failure labels, maintenance events, per-device IDs), as established by the profile.

## Validation policy

- Per-event failures → DLQ with `error_type`.
- Batch checks: see the `data-validation` skill. Note any thresholds agreed with the user here.

## Change log

| Version | Date | Change | Breaking? |
|---|---|---|---|
