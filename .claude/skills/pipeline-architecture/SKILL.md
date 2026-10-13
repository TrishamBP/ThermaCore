---
name: pipeline-architecture
description: End-to-end design and sequencing of the data center operations pipeline (dataset ingestion → replay/simulator producer → Kafka → validation/processing consumers → raw + curated storage → analytics/anomaly detection → future AI-agent investigation). Use when designing or changing pipeline stages, deciding where new code belongs, adding a new topic/consumer/stage, explaining how ELT relates to Kafka streaming, or wiring an end-to-end run.
---

# Pipeline Architecture

Governs how the platform is structured and sequenced. Stage-specific procedures live in the sibling skills; this skill decides *which* stage a change belongs to and *what must hold* between stages.

Adapted from the QuantBridge `pipeline-orchestration` skill, `patterns/pipeline-patterns.md`, and `hooks/` (see `references/stage-gates.md`).

## Target architecture (initial)

```
Kaggle dataset files (immutable, data/raw/...)
    │  dataset-ingestion: download, checksum, profile, contract
    ▼
Replay producer  ──or──  Telemetry simulator        (Python)
    │  event envelope with provenance + event_time
    ▼
Apache Kafka  (topics: raw → validated, + DLQ, + anomalies)
    │
    ├─► raw-sink consumer      → raw event store (Parquet, append-only)   ← the "L" in ELT
    ├─► validator consumer     → validated topic / DLQ
    └─► anomaly consumer       → anomalies topic + anomaly table
                                   │
DuckDB: transformations over raw store (the "T" in ELT) → curated tables
    ▼
Analytics + incident detection  →  (future) AI-agent investigation & recommendations
```

Stages 1–6 are the current scope. Stage 7 (AI agents) is **future**. Do not build it unless asked, but keep evidence (event IDs, offsets, queries) traceable so agents can cite it later.

## How ELT and streaming fit together

Streaming and ELT describe different things. Do not conflate them.

- **Kafka is the transport and the durable, replayable log.** It is not the storage of record and not the transformation layer.
- **E (extract):** the producer reads dataset rows or generates synthetic ones and publishes events.
- **L (load):** a dedicated raw-sink consumer writes every event *unchanged* (full envelope) to the raw store. This happens before any business transformation, so any downstream logic can be re-run from the raw store without touching Kafka or the source.
- **T (transform):** runs *after* load, in DuckDB SQL or Python, over the raw store. This produces curated and aggregated tables.
- **Streaming consumers** (validation, anomaly detection) are a parallel, low-latency path off the same topics. They must produce results that are reproducible by replaying the topic or the raw store.

Rule: the raw store is append-only and never filtered. Validation failures go to the DLQ *and* still exist in the raw store, tagged invalid by downstream checks, never silently dropped.

## Stage sequence and gates

| # | Stage | Skill | Gate before next stage |
|---|---|---|---|
| 1 | Dataset ingestion + profiling | `dataset-ingestion` | Profile reviewed by user; data contract written |
| 2 | Producer (replay or simulator) | `kafka-streaming`, `data-transformation` | Sample events shown and approved; deterministic `event_id` |
| 3 | Kafka topics | `kafka-streaming` | Topics exist with agreed partitions; broker healthy |
| 4 | Validation / processing consumers | `data-validation`, `kafka-streaming` | Invalid → DLQ with reason; offsets committed after success |
| 5 | Raw + curated storage | `storage-design` | Raw counts reconcile with produced counts per partition |
| 6 | Analytics + anomaly detection | `anomaly-detection` | Anomalies carry evidence and provenance |
| 7 | AI-agent investigation (future) | — | Not implemented |

Cross-cutting: `observability-reliability` (logging, retries, lag) and `pipeline-testing`.

Detailed per-stage checklists (adapted from the source `hooks/`): `references/stage-gates.md`.
Worked example of adding a new stream end to end: `references/add-new-stream.md`.

## Proposed repository layout

**Implemented (Phase 1).** Keep to this layout:

```
pyproject.toml, uv.lock                   # uv-managed; requirements.txt is exported from uv.lock
config/simulation.yaml                    # scenario + simulation settings
db/schema.sql, db/init_db.py              # SQLite schema (Phase 1 storage)
data/raw/hyperscale-data-center-dataset/  # downloaded files + MANIFEST.json, immutable, gitignored
data/datacenter_ops.db                    # SQLite database, gitignored
data/processed/                           # filtered CSV, validation report, gitignored
data/simulation/<run_id>/                 # generated telemetry artifacts, gitignored
docs/dataset/                             # profile.json + reviewed dataset notes
src/data_engineering/{ingestion,processing,simulation,validation}/
tests/                                    # pytest
logs/                                     # JSONL logs, gitignored
```

**Proposed for the Kafka phase.** Not built yet; confirm with the user first:

```
docker-compose.yml                        # local Kafka (see kafka-streaming)
src/data_engineering/events/              # event envelope, schemas, serialization
src/data_engineering/producers/           # replay/simulator producer
src/data_engineering/consumers/           # raw_sink.py, validator.py, anomaly_detector.py
data/lake/raw/<topic>/...                 # raw events landed from Kafka (Parquet), gitignored
```

Whether curated/analytics storage stays in SQLite or moves to DuckDB is an open decision for the Kafka phase.

## Design rules

1. **Inspect before designing.** The shape of the dataset decides whether stage 2 is a replay producer or a simulator. Do not decide this before `dataset-ingestion` has produced a reviewed profile.
2. **One responsibility per consumer group.** The raw sink, validator and anomaly detector are separate consumer groups, so each tracks its own offsets and can be replayed independently.
3. **Every event is traceable.** Each event carries `event_id`, `provenance`, `event_time`, `ingestion_time`, and a source reference (file plus row, or generator plus seed).
4. **Halt on contract violations at batch boundaries; route to DLQ in streams.** Batch stages fail the run. Streaming consumers isolate bad events and keep going.
5. **Minimal infrastructure.** One local Kafka broker (KRaft, Docker Compose), Python, DuckDB and Parquet. Add Schema Registry, Kafka Connect, Flink/Spark, Postgres or a UI only when there is a concrete need, and state that need first.
6. **Explain before implementing.** For a stage or flow change, describe the execution-flow change and its impact before writing code.

## What is NOT implemented

Do not assume these exist: the AI-agent investigation workflow, alerting/notifications, a serving database (Supabase/Postgres), CI workflows, dashboards. Refer to them only as future work.
