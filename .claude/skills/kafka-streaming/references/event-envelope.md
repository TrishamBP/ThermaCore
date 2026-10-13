# Event Envelope (v1, proposed)

> **Superseded for telemetry** by the implemented schema `src/data_engineering/streaming/telemetry_event.v1.schema.json` (fields `event_timestamp`, `timestamp_source`, `source_type`, `reading_status`, `provenance.field_sources`, ...). Keep this file as design rationale; the schema file is the contract.

All Kafka events share this envelope. `entity` and `payload` hold **only fields confirmed by the dataset profile** (`docs/dataset/`). The angle-bracket names below are placeholders.

```json
{
  "event_id": "sha256:3f9a...",
  "schema_version": "1",
  "event_type": "telemetry.observation",
  "provenance": "replay",
  "event_time": "2024-03-01T12:00:00Z",
  "event_time_source": "dataset",
  "ingestion_time": "2026-10-13T09:15:02.123Z",
  "source": {
    "dataset": "deeplumiere/hyperscale-data-center-dataset",
    "file": "<file>.csv",
    "row_number": 1234,
    "dataset_sha256": "<from MANIFEST.json>"
  },
  "generator": null,
  "entity": { "<entity_col>": "<value>" },
  "payload": { "<metric_col>": 0.0 },
  "units": { "<metric_col>": "<unit or 'unknown'>" },
  "run": { "run_id": "replay-20261013-0915", "speedup": 60 }
}
```

## Field rules

| Field | Required | Rule |
|---|---|---|
| `event_id` | yes | Deterministic. `dataset`/`replay`: sha256 of `dataset|file|row_number`. `synthetic`: sha256 of `generator.name|generator.version|seed|sequence` |
| `schema_version` | yes | String. Bump the major version on breaking change, along with the topic `.vN` |
| `event_type` | yes | e.g. `telemetry.observation`, `anomaly.detected` |
| `provenance` | yes | `dataset` (batch-loaded actual value), `replay` (actual value re-emitted through Kafka), `synthetic` (generated) |
| `event_time` | yes | UTC ISO-8601. From the dataset timestamp for `replay`/`dataset` |
| `event_time_source` | yes | `dataset` / `synthetic` / `dataset_tz_assumed` (timezone not stated in the source; assumption documented in `docs/dataset/README.md`) |
| `ingestion_time` | yes | Set by the producer at send time. Never used for windowing |
| `source` | for `dataset`/`replay` | Enough to locate the exact source row |
| `generator` | for `synthetic` | `{ "name", "version", "seed", "based_on": "<profile/params ref>" }` |
| `synthetic_label` | optional, `synthetic` only | Ground truth for injected anomalies, e.g. `{ "injected_anomaly": "spike", "magnitude": 3.0 }` |
| `entity` | if the dataset has identifiers | Only real identifier columns |
| `payload` | yes | Metric values with names as in the contract (snake_case-normalized) |
| `units` | yes, per metric | `unknown` is allowed and must not be guessed |
| `run` | yes | `run_id` ties events to logs |

## Kafka record

- **Key**: the entity key (UTF-8 string), or the documented fallback grouping.
- **Value**: the envelope JSON (UTF-8).
- **Headers** (optional, for routing/debug): `schema_version`, `provenance`. DLQ records add `error_type`, `error_message`, `failed_stage`, `source_topic`, `source_partition`, `source_offset`.
- **Kafka timestamp**: leave it as the producer's default (CreateTime = publish wall-clock time). Do **not** set it to the event time: the broker's time-based retention uses record timestamps, so replayed or simulated events dated in the past would be deleted almost immediately. Always read event time from the payload.

## Anomaly event (`dc.anomalies.v1`)

Defined in the `anomaly-detection` skill. It reuses the same envelope fields (`event_id` = `anomaly_id`, `provenance` inherited from the input: an anomaly found on synthetic data is `synthetic`).
