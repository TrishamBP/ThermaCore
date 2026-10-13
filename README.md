# Data Center Operations Intelligence: Data Engineering

AMD ACT III Hackathon. This repository builds the data layer for a platform that monitors one **hypothetical North American data center**. It turns a public Kaggle dataset into a stream of timestamped telemetry events, publishes them to **Apache Kafka**, consumes them into a database (SQLite or PostgreSQL) and a Parquet history queried with DuckDB, and estimates the current cooling/thermal state through a small read-only API. A dashboard and AI agents are later phases.

> **Data honesty, up front.** The Kaggle dataset describes itself as an *engineered synthetic dataset*. It has no timestamps and no equipment IDs. Every telemetry event in this project is **simulated** from it and labeled as such (`source_type: "simulated"`). None of it is real operational telemetry.

---

## Contents

- [A. Project overview](#a-project-overview)
- [B. Architecture](#b-architecture)
- [C. Kafka fundamentals, applied to `datacenter.telemetry`](#c-kafka-fundamentals-applied-to-datacentertelemetry)
- [D. Project structure](#d-project-structure)
- [E. Environment setup](#e-environment-setup)
- [F. Running the producer](#f-running-the-producer)
- [G. Example event](#g-example-event)
- [H. Observing messages](#h-observing-messages)
- [I. Reliability and troubleshooting](#i-reliability-and-troubleshooting)
- [J. Data correctness and provenance](#j-data-correctness-and-provenance)
- [K. Future integration](#k-future-integration)
- [L. Limitations and next steps](#l-limitations-and-next-steps)
- [Part II: Consumer, storage and analytics](#part-ii-consumer-storage-and-analytics)
  - [II.1 Architecture](#ii1-end-to-end-architecture) · [II.2 Validation, transformation, batches](#ii2-what-validation-transformation-and-batch-writes-mean-here) · [II.3 Groups, offsets, retries, DLQ](#ii3-consumer-groups-partitions-offsets-commits-retries-dead-letters) · [II.4 At-least-once](#ii4-at-least-once-delivery-and-idempotent-writes) · [II.5 SQLite vs PostgreSQL](#ii5-sqlite-or-postgresql) · [II.6 Parquet and DuckDB](#ii6-parquet-for-history-duckdb-for-questions) · [II.7 Event schema](#ii7-event-schema-seen-by-the-consumer) · [II.8 Database schema](#ii8-database-schema) · [II.9 Parquet layout](#ii9-parquet-layout-and-schema) · [II.10 Setup and running](#ii10-setup-and-running) · [II.11 DuckDB queries](#ii11-querying-with-duckdb) · [II.12 Tests](#ii12-tests) · [II.13 Verifying counts](#ii13-verifying-counts-across-kafka-the-database-and-parquet) · [II.14 Troubleshooting](#ii14-troubleshooting)
- [Part III: Cooling State Estimator](#part-iii-cooling-state-estimator)
- [Appendix: Phase 1 (dataset → SQLite → simulation)](#appendix-phase-1-dataset--sqlite--simulation)

---

## A. Project overview

**The platform** (end goal): ingest data center telemetry, detect anomalous behavior, and let AI agents investigate incidents and recommend evidence-based corrective actions.

**This repository today:**

| Phase | What | Status |
|---|---|---|
| 1 | Download the Kaggle dataset, profile it, keep the North American (`NA`) rows, load everything into SQLite, generate a labeled synthetic 24-hour, 1-minute telemetry run | **Implemented** |
| 2 | **Kafka producer**: turn simulated telemetry into validated JSON events and publish them to the Kafka topic `datacenter.telemetry`; local Kafka via Docker Compose; repeatable topic setup | **Implemented** |
| 3 | Kafka **consumer** → validation, DLQ, idempotent batch persistence (SQLite / PostgreSQL), Parquet history, DuckDB analytics (Part II) | **Implemented** |
| 4 | **Cooling State Estimator**: rules-based thermal/operational snapshot, served by the `app` API package at `GET /api/v1/telemetry/cooling-state` (Part III) | **Implemented** |
| 5 | Further API endpoints, frontend/dashboard, AI agents, cooling optimization | Not implemented |

**What the producer does.** It reads telemetry in one of two modes and publishes one JSON message per (asset, minute) to Kafka:

- **`simulate`** (Mode B): generates telemetry *while publishing*, using the Phase 1 simulation engine (`TelemetrySimulator`). Finite by default; `--continuous` runs until stopped.
- **`publish-stored`** (Mode A): streams a simulation run that Phase 1 already stored in SQLite.

Every event is validated against a versioned JSON Schema before sending, keyed by `asset_id`, and tracked until the broker confirms delivery.

---

## B. Architecture

```mermaid
flowchart LR
    subgraph implemented["Implemented"]
        K[(Kaggle CSV<br/>green_ai_datacenter.csv<br/>100,000 rows)] --> P1[Phase 1 pipeline<br/>load, NA filter,<br/>validation]
        P1 --> DB[(SQLite<br/>data/datacenter_ops.db<br/>source rows + stored runs)]
        DB -->|NA baseline rows| SIM[TelemetrySimulator<br/>streaming, seeded]
        DB -->|stored run| RD[stored-run reader]
        SIM --> PR[Kafka producer<br/>validate → JSON →<br/>key=asset_id]
        RD --> PR
        PR -->|acks=all, idempotent| T[["Kafka topic<br/>datacenter.telemetry<br/>3 partitions"]]
    end
        T --> C[Kafka consumer<br/>validate, dedupe by event_id]
        C --> ST[(SQLite / PostgreSQL<br/>+ Parquet history + DuckDB)]
    end
    subgraph planned["Planned: NOT implemented"]
        ST -.-> API[FastAPI]
        API -.-> UI[Frontend / dashboard]
        API -.-> AG[AI agents<br/>investigation and recommendations]
    end
```

**Flow, step by step**

1. **Source data → SQLite (Phase 1).** The Kaggle CSV is loaded unchanged into `source_records`. The 28,801 rows with `datacenter_region = "NA"` are marked as the North American baseline.
2. **Simulation.** `TelemetrySimulator` picks N distinct NA rows (seeded), turns each into a synthetic asset (`SIM-NA-001` …), and steps every asset forward one slot at a time. Values follow relationships fitted on the NA rows, so CPU tracks workload and airflow tracks fan speed. Missing readings and anomalies are injected and labeled.
3. **Producer.** Each slot becomes an event. It is validated against `telemetry_event.v1.schema.json`, serialized to compact JSON, and handed to the Kafka client with key `asset_id`.
4. **Kafka topic.** The broker appends the message to one of three partitions, chosen by hashing the key, and confirms the write.

**Why the producer doesn't write to the database directly**

- **Decoupling.** The producer only knows "publish an event". Storage, the API and detection are independent consumers that can be added, restarted or rewritten without touching it.
- **Buffering.** If the database is slow or down, events wait safely in Kafka instead of being lost or blocking generation.
- **Replay.** Kafka keeps the stream for the retention period (7 days here). A new consumer, such as a fixed anomaly detector, can re-read history from offset 0. A direct database write would be consumed once and gone.
- **Fan-out.** The same event can feed storage, real-time detection and agents at once, each at its own pace.

---

## C. Kafka fundamentals, applied to `datacenter.telemetry`

| Concept | Plain language | In this project |
|---|---|---|
| **Kafka** | A distributed, durable, append-only log. Programs write events to it and other programs read them later, at their own pace. | The event-streaming layer between the simulator and everything downstream. |
| **Broker** | One Kafka server process that stores data and serves clients. Production clusters have several. | One broker in the `dco-kafka` container (`apache/kafka:3.9.1`, KRaft mode, no ZooKeeper), reachable at `127.0.0.1:9092`. |
| **Topic** | A named stream of related events. | `datacenter.telemetry`: one message per asset per simulated minute. |
| **Producer** | A client that writes messages to a topic. | `data_engineering.streaming.producer` (confluent-kafka). |
| **Consumer** | A client that reads messages from a topic and tracks its position. | Not built yet. Today you can read with `kafka-console-consumer.sh` (section H). |
| **Partition** | A topic is split into ordered sub-logs so it can scale. **Order is guaranteed only within a partition.** | 3 partitions. In the 20-asset run they hold 6, 7 and 7 assets. |
| **Offset** | A message's position in its partition (0, 1, 2, …). Consumers remember "I've processed up to offset N". | After one 24-hour run: offsets 0–8,639 in partition 0 (6 assets × 1,440) and 0–10,079 in partitions 1 and 2. |
| **Message key** | Optional bytes attached to a message. The default partitioner hashes the key, so **the same key always goes to the same partition**. | Key = `asset_id`. All of `SIM-NA-001`'s readings land in one partition, in time order. Verified: every key maps to exactly one partition. |
| **Buffering (producer side)** | `produce()` only puts the message into a **local in-memory queue**. A background thread batches messages and sends them. The broker's acknowledgment arrives later through a **delivery callback**. | That's why the producer reports `queued` and `delivered` separately. Batching: `linger.ms=20`, `batch.size=64 KiB`, `lz4` compression. |
| **Decoupling** | Producer and consumers never talk to each other. They only share the topic. Either can run, stop or restart without the other. | The producer can publish a day of telemetry before any consumer exists. A consumer started tomorrow can still read it from offset 0. |
| **Slow consumer** | Kafka keeps messages for the retention period whether or not anyone reads them. A slow consumer simply falls behind (its *lag* grows) and the producer is unaffected. If it falls behind by more than the retention period, old messages are deleted before it reads them. | Retention is 7 days (`retention.ms=604800000`). A full 24-hour run is ~28,800 messages of ~1.7 KB each, roughly 48 MB uncompressed. |
| **Kafka temporarily unavailable** | The client keeps queued messages in memory and retries until `delivery.timeout.ms`. If the broker comes back in time, they're delivered. If not, each message's callback reports a timeout. If the local queue fills, `produce()` raises `BufferError`. | `delivery.timeout.ms=120000` (2 min). On `BufferError` the producer waits (serving callbacks) and retries, so nothing is dropped. Timed-out messages are counted as `failed` and the exit code is 1. |
| **Persistence guarantees** | Acknowledged messages are written to the broker's log and survive consumer restarts. Durability depends on replication: with **one broker and replication factor 1, a lost disk or deleted container loses the data**. Kafka also does not deduplicate by content or validate payloads. | Local dev only: `docker compose down` deletes the topic data. Consumers must deduplicate using `event_id`. |

---

## D. Project structure

Files added or changed for the Kafka producer are marked **(new)** or **(changed)**.

```
docker-compose.yml                           single local Kafka broker (127.0.0.1); optional `postgres` profile (Part II)
.env.example                                 (changed) every Kafka/producer setting with its default
pyproject.toml / uv.lock / requirements.txt  (changed) + confluent-kafka, jsonschema, python-dotenv; dev: numpy
config/simulation.yaml                       (changed) + streaming.data_center_id
src/data_engineering/
  streaming/                                 (new) everything Kafka-related
    producer.py                              CLI + TelemetryPublisher (validate, produce, delivery tracking, flush) + Pacer
    settings.py                              ProducerSettings: env/.env loading, validation, librdkafka config
    events.py                                build_event, to_json_value (NumPy/pandas/NaN handling), validate_event, serialize_event
    telemetry_event.v1.schema.json           the versioned event contract (JSON Schema 2020-12)
    sources.py                               event sources: simulated_events (Mode B), stored_run_events (Mode A)
    topic_setup.py                           idempotent topic creation + description
    inspect_stream.py                        text filter over kafka-console-consumer output (counts, keys, order, duplicates)
    consumer.py                              (Part II) consumer CLI: batches, OffsetTracker, retries, DLQ, shutdown
    event_validation.py                      (Part II) consumer-side parsing/validation with stable reason codes
    dead_letter.py                           (Part II) DLQ record format and confirmed publishing
  storage/                                   (Part II) base.py (shared SQL, idempotency, export ledger),
                                             sqlite_store.py, postgres_store.py, parquet_sink.py
  analytics/                                 (Part II) duckdb_queries.py (TelemetryAnalytics), query_telemetry.py (CLI)
  simulation/generate_telemetry.py           (changed) refactored into a streaming TelemetrySimulator; output of
                                             the stored Phase 1 run is byte-identical (verified by its content hash)
  ingestion/, processing/, validation/,      Phase 1 (see appendix)
  pipeline.py, db.py, config.py, ...
db/schema.sql, db/init_db.py                 Phase 1 SQLite schema
db/telemetry_store_{sqlite,postgres}.sql     (Part II) consumer store schema; db/init_telemetry_store.py applies it
tests/
  kafka_fakes.py                             (new) in-memory fake Producer, so unit tests need no broker
  test_kafka_events.py                       (new) schema, serialization, nulls, NumPy/pandas normalization, timestamps
  test_kafka_sources.py                      (new) counts, stable IDs, reproducibility, continuous mode, stored-run replay
  test_kafka_producer.py                     (new) topic/key, delivery failures, queue full, flush, shutdown, config, CLI exit codes
  integration/test_kafka_integration.py      (new) real-broker round trip (marked `integration`)
  test_ingestion.py, test_database.py, test_simulation.py   Phase 1
.claude/skills/                              project skills; kafka-streaming updated with the implemented setup
```

**Why confluent-kafka.** It wraps librdkafka, the C client used by most non-Java Kafka clients. It is actively maintained, ships Windows wheels for Python 3.13 (version 2.16.0 / librdkafka 2.16.0 here), and supports idempotent producers, delivery callbacks and the AdminClient used for topic creation. `kafka-python` is pure Python and simpler to install, but its idempotence and maintenance history are weaker.

**Why `apache/kafka:3.9.1`.** It's the official Apache image, runs KRaft (no ZooKeeper container), and the tag is pinned so everyone runs the same broker. librdkafka 2.16 fully supports Kafka 3.9.

---

## E. Environment setup

Commands were run on Windows 11 in **Git Bash** and checked in **Windows PowerShell 5.1** where syntax differs.

### 1. Prerequisites

- Python 3.13+, [uv](https://docs.astral.sh/uv/) (`uv --version`), Docker Desktop (running).

### 2. Install dependencies (creates `.venv`)

```bash
uv sync
```

You don't need to activate the virtual environment: `uv run <command>` always uses `.venv`. If you prefer activating it:

```bash
source .venv/Scripts/activate        # Git Bash (Windows)
source .venv/bin/activate            # macOS / Linux
```
```powershell
.\.venv\Scripts\Activate.ps1         # PowerShell (needs ExecutionPolicy RemoteSigned or looser)
```

### 3. Build the Phase 1 data the producer depends on (once)

The producer reads its baseline rows (and stored runs) from SQLite:

```bash
uv run python -m data_engineering.pipeline
```

This downloads the dataset, loads SQLite, generates the stored run and validates (20/20 checks). Details are in the appendix.

### 4. Configure `.env` (optional)

All settings have working defaults, so this step is only needed to change something.

```bash
cp .env.example .env                 # Git Bash
```
```powershell
Copy-Item .env.example .env          # PowerShell
```

Precedence: CLI flag > real environment variable > `.env` > built-in default.

### 5. Start Kafka

```bash
docker compose up -d --wait kafka    # waits until the health check passes
docker compose ps                    # expect: dco-kafka ... (healthy)  127.0.0.1:9092->9092/tcp
```

### 6. Create and verify the topic (repeatable)

```bash
uv run python -m data_engineering.streaming.topic_setup
# topic datacenter.telemetry: created, 3 partitions, 1 broker(s)
# topic datacenter.telemetry.dlq: created, 3 partitions, 1 broker(s)     <- dead-letter topic for the consumer
# (second run)  ... already existed ...
```

The broker has automatic topic creation **disabled**, so a typo in a topic name fails loudly instead of silently creating a new topic. `topic_setup` never alters an existing topic; it reports a partition-count mismatch instead.

### 7. Verify broker connectivity

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic datacenter.telemetry
docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -1
```

> **Git Bash only:** prefix `docker compose exec` commands with `MSYS_NO_PATHCONV=1` (or `export MSYS_NO_PATHCONV=1` once). Otherwise Git Bash rewrites `/opt/kafka/bin/...` into a Windows path. PowerShell doesn't need this.
>
> Inside the container, `localhost:9092` is correct. From your machine, use `127.0.0.1:9092` (see section I).

---

## F. Running the producer

All commands: `uv run python -m data_engineering.streaming.producer <mode> [options]`. Use `--help` on either mode for the full list.

| Option | Modes | Meaning |
|---|---|---|
| `--assets N` | simulate | number of simulated assets (default 20, max = number of NA rows) |
| `--duration-minutes M` | simulate | simulated duration (default 1440). In continuous mode, the anomaly window length |
| `--interval-seconds S` | simulate | seconds between readings (default 60) |
| `--start 2026-01-01T00:00:00Z` | simulate | simulation start, UTC with trailing `Z` |
| `--seed N` | simulate | random seed (default 42) |
| `--missing-rate P` | simulate | probability a slot has no reading (default 0.01) |
| `--continuous` | simulate | keep generating until Ctrl+C or `--max-events` |
| `--run-id ID` | publish-stored | which stored run (default: latest) |
| `--max-events N` | both | stop after N events |
| `--rate R` | both | R events per second |
| `--speedup X` | both | X simulated seconds per real second (`--rate` and `--speedup` are mutually exclusive) |
| `--bootstrap-servers`, `--topic` | both | override `KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_TOPIC` |
| `--dry-run` | both | validate and print events as JSON lines; nothing is sent |
| `--config PATH` | both | alternative simulation YAML |

Defaults for `simulate` come from `config/simulation.yaml`. Finite mode is the default, so a plain command can never start an endless stream.

**Small finite run (30 events)**

```bash
uv run python -m data_engineering.streaming.producer simulate --assets 3 --duration-minutes 10
# SUCCESS: attempted=30 delivered=30 rejected=0 failed=0 undelivered=0 (queued=30)
```

**Full 24-hour, one-minute simulation (20 × 1,440 = 28,800 events; ~14 s unthrottled)**

```bash
uv run python -m data_engineering.streaming.producer simulate
# same as: simulate --assets 20 --duration-minutes 1440 --interval-seconds 60
```

With the default config this is run `sim-c9944cc35156`, identical to the Phase 1 stored run.

**Accelerated / paced simulation**

```bash
# 1 simulated minute per real second -> 24 h in 24 min
uv run python -m data_engineering.streaming.producer simulate --speedup 60
# 24 h in about 1 minute
uv run python -m data_engineering.streaming.producer simulate --speedup 1440
# real time (one reading per asset per minute)
uv run python -m data_engineering.streaming.producer simulate --speedup 1
# fixed rate regardless of simulated time
uv run python -m data_engineering.streaming.producer simulate --rate 50
```

`--speedup` controls **when** an event is sent. It never changes `event_timestamp`.

**Continuous run** (stop with Ctrl+C; the producer flushes and prints the summary)

```bash
uv run python -m data_engineering.streaming.producer simulate --continuous --speedup 60
uv run python -m data_engineering.streaming.producer simulate --continuous --max-events 5000
```

After the first `--duration-minutes` window, time continues (`2026-01-02T00:00:00Z`, …), the signal carries on smoothly, and anomalies are re-placed for each window (IDs like `<run>-W001-A003`).

**Publish existing (stored) telemetry: Mode A**

```bash
uv run python -m data_engineering.streaming.producer publish-stored                       # latest run
uv run python -m data_engineering.streaming.producer publish-stored --run-id sim-c9944cc35156
# SUCCESS: attempted=28800 delivered=28800 ...
```

Rows are streamed from SQLite with a cursor (`ORDER BY slot_index, asset_id`), not loaded all at once. The events are identical to `simulate` for the same run, except `provenance.published_from = "stored_run"`. A unit test asserts this.

**Inspect events without Kafka**

```bash
uv run python -m data_engineering.streaming.producer simulate --assets 2 --duration-minutes 2 --dry-run
```

**Change broker or topic**

```bash
uv run python -m data_engineering.streaming.topic_setup --topic datacenter.telemetry.dev
uv run python -m data_engineering.streaming.producer simulate --topic datacenter.telemetry.dev --bootstrap-servers 127.0.0.1:9092
KAFKA_TOPIC=datacenter.telemetry.dev uv run python -m data_engineering.streaming.producer simulate   # Git Bash
```
```powershell
$env:KAFKA_TOPIC = "datacenter.telemetry.dev"; uv run python -m data_engineering.streaming.producer simulate
Remove-Item Env:KAFKA_TOPIC
```

**Exit status:** `0` = every event validated and broker-confirmed. `1` = something was rejected, failed or left unconfirmed. `2` = configuration, data-source, broker or topic problem before publishing.

**Run the tests**

```bash
uv run pytest                    # unit tests: no Kafka needed (integration tests are excluded by default)
uv run pytest -m integration     # real-broker round trip; requires `docker compose up -d kafka`
```

---

## G. Example event

This is the **actual first event** of `uv run python -m data_engineering.streaming.producer simulate --dry-run` (default config):

```json
{
  "schema_version": "1.0",
  "event_id": "sim-c9944cc35156:SIM-NA-001:0",
  "event_timestamp": "2026-01-01T00:00:00Z",
  "timestamp_source": "simulated",
  "asset_id": "SIM-NA-001",
  "data_center_id": "dc-na-sim-001",
  "region": "North America",
  "region_code": "NA",
  "source_type": "simulated",
  "simulation_run_id": "sim-c9944cc35156",
  "slot_index": 0,
  "reading_status": "observed",
  "metrics": {
    "workload_intensity": 0.337,
    "cpu_utilization": 28.198,
    "gpu_utilization": 0.0,
    "memory_utilization": 47.442,
    "storage_utilization": 21.788,
    "disk_io_iops": 4370.31,
    "network_throughput_gbps": 15.229,
    "network_latency_ms": 36.983,
    "ambient_temperature_c": 19.223,
    "humidity_percentage": 47.112,
    "inlet_temperature_c": 20.573,
    "exhaust_temperature_c": 23.088,
    "fan_speed_rpm": 2488.522,
    "airflow_cfm": 151.341,
    "power_consumption_kw": 94.748,
    "pue": 1.05
  },
  "asset_attributes": {
    "server_type": "Compute",
    "cooling_type": "Air",
    "server_age_years": 2,
    "rack_u_position": 36,
    "energy_source_primary": "Grid"
  },
  "labels": {
    "source_failure_risk_level": "Low",
    "source_energy_efficiency_class": "Medium",
    "source_maintenance_status": "Recent",
    "injected_anomalies": []
  },
  "provenance": {
    "dataset": "deeplumiere/hyperscale-data-center-dataset",
    "source_file": "green_ai_datacenter.csv",
    "source_sha256": "6d963fa43926e355e82ee6b167dabda4944b14cdcaf3b46c89676cda9ca0e8f5",
    "baseline_source_row_number": 66805,
    "generator": "dco-baseline-telemetry",
    "generator_version": "1.0.0",
    "seed": 42,
    "published_from": "simulator",
    "field_sources": {
      "metrics": "simulated",
      "event_timestamp": "simulated",
      "asset_id": "simulated",
      "data_center_id": "simulated",
      "region_code": "source_dataset",
      "region": "transformed",
      "asset_attributes": "source_dataset",
      "source_labels": "source_dataset",
      "injected_anomalies": "simulated"
    }
  }
}
```

Field by field. **S** = copied from the source dataset, **T** = transformed from source values, **G** = generated (simulated) or a project constant.

| Field | Type | Origin | Meaning |
|---|---|---|---|
| `schema_version` | string `"1.0"` | G | Event format version. A breaking change means `"2.0"` and a new schema file |
| `event_id` | string | G | `<simulation_run_id>:<asset_id>:<slot_index>`. **Deterministic**: republishing the same run produces the same IDs, so consumers can deduplicate |
| `event_timestamp` | string, UTC `YYYY-MM-DDTHH:MM:SSZ` | G | Simulated measurement time = start + `slot_index` × interval. **Not** the publish time |
| `timestamp_source` | `"simulated"` | G | States explicitly that the timestamp is not from the dataset (which has none) |
| `asset_id` | string | G | Synthetic, stable within a run: `SIM-NA-001` … `SIM-NA-<N>` |
| `data_center_id` | string | G | The one hypothetical data center (`config/simulation.yaml` → `streaming.data_center_id`) |
| `region_code` | string | S | `datacenter_region` of the baseline rows |
| `region` | string | T | `NA` → "North America". Inferred mapping; the dataset doesn't define its codes |
| `source_type` | `"simulated"` | G | Only value in v1.0. Source rows are never published as telemetry (no timestamps, no asset IDs) |
| `simulation_run_id` | string | G | Hash of the simulation config + source checksum. Same config gives the same ID |
| `slot_index` | integer ≥ 0 | G | Slot number from the start |
| `reading_status` | `"observed"` \| `"missing"` | G | `missing` = the slot exists but produced no reading; then **every metric is `null`** |
| `metrics.*` | number, or `null` only when missing | G | 16 simulated measurements. Units are inferred from column names (see the schema `description`s): `_c` °C, `_kw` kW, `_ms` ms, `_gbps` Gbit/s, `_rpm`, `_cfm`, `_iops`; utilizations are % (inferred from their 0–100 source range); `workload_intensity` and `pue` are unitless |
| `asset_attributes.*` | string / integer | S | Static attributes of the asset's baseline source row |
| `labels.source_*` | string | S | Labels of the baseline source row (`failure_risk_level`, `energy_efficiency_class`, `maintenance_status`). **Static per asset**, not re-evaluated each minute |
| `labels.injected_anomalies` | array | G | Ground truth for anomalies the simulator deliberately injected at this slot, e.g. `[{"anomaly_id": "sim-c9944cc35156-A004", "anomaly_type": "power_spike", "affected_metrics": ["power_consumption_kw"]}]` (actual event `sim-c9944cc35156:SIM-NA-006:101`). Empty otherwise |
| `provenance.*` | object | G/S | Dataset, file checksum and source row behind the asset, generator + seed, and how it was published. `field_sources` repeats the S/T/G column above inside every message |

A **missing** reading looks like this (actual event `sim-c9944cc35156:SIM-NA-004:5`, `2026-01-01T00:05:00Z`): `"reading_status": "missing"`, `"metrics": {"workload_intensity": null, "cpu_utilization": null, …}`, `"injected_anomalies": []`.

**Kafka record around the payload:** key = `asset_id` (UTF-8). Headers: `schema_version`, `source_type`, `content-type: application/json`. The record timestamp is the broker-side *CreateTime* (publish time) on purpose; see J.

**Validation.** Every event is checked with `jsonschema` (Draft 2020-12) against `src/data_engineering/streaming/telemetry_event.v1.schema.json` before it is sent:

- no unknown fields, and exactly the 16 metrics
- the timestamp pattern
- observed slots must have **all** metrics as numbers
- missing slots must have **all** metrics `null` and no anomalies
- `field_sources` must match exactly

Serialization uses `allow_nan=False`, so `NaN` can never leave as invalid JSON. Failing events are counted as `rejected`, logged with the reasons, and never sent.

---

## H. Observing messages

The image ships Kafka's CLI tools under `/opt/kafka/bin`. Git Bash users: `export MSYS_NO_PATHCONV=1` first.

**Are messages arriving? How many per partition?**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic datacenter.telemetry
# datacenter.telemetry:0:8640
# datacenter.telemetry:1:10080
# datacenter.telemetry:2:10080        <- after one 24 h run: 8,640 + 10,080 + 10,080 = 28,800
```

The numbers are the *next* offset per partition, so their sum is the total number of messages ever written (within retention).

**Read messages with key, partition, offset and record timestamp**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic datacenter.telemetry --from-beginning --max-messages 3 \
  --property print.key=true --property print.partition=true --property print.offset=true --property print.timestamp=true
# CreateTime:1791874301330  Partition:1  Offset:0  SIM-NA-003  {"schema_version":"1.0","event_id":"sim-c9944cc35156:SIM-NA-003:0","event_timestamp":"2026-01-01T00:00:00Z",...
```

Notice the two different times: `CreateTime` is when it was published (October 2026), and `event_timestamp` is the simulated measurement time (2026-01-01).

**Check keys, timestamp order, counts and duplicates in one go**

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic datacenter.telemetry --from-beginning --timeout-ms 10000 --property print.key=true \
  | uv run python -m data_engineering.streaming.inspect_stream
```

`inspect_stream` is a text filter over the console consumer's output, not a Kafka consumer. It prints:

- `messages` and `messages_per_key` (expect 1,440 per asset for one 24 h run)
- `runs`
- `reading_status` counts
- `event_timestamp_range`
- `key_not_equal_asset_id` (expect 0)
- `schema_invalid` (expect 0)
- `timestamps_not_increasing_per_key` (expect `{}` for a single run)
- `duplicate_event_ids` (expect 0 for a single run)

After publishing the same run twice, you will see `duplicate_event_ids: 28800` and non-increasing timestamps per key. That's correct: it's the same events published again (see I, "Duplicates").

**Expected counts:** see J. `kafka-console-consumer.sh` registers a temporary consumer group (`console-consumer-NNNN`); `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list` shows them.

---

## I. Reliability and troubleshooting

**Delivery semantics (configured explicitly in `settings.py`)**

- `acks=all`: the broker confirms only after all in-sync replicas have the write (one replica locally).
- `enable.idempotence=true`: the broker discards duplicates created by the client's *own* internal retries and keeps per-partition order. This does **not** deduplicate a second run of the producer.
- Retries are librdkafka's: transient errors are retried automatically, with `retry.backoff.ms=100`, until `delivery.timeout.ms=120000`. After that the delivery callback reports the failure. The producer then counts it as `failed`, does **not** re-send it (that could reorder or duplicate), and exits with 1. Rerunning the same command republishes with the same `event_id`s.
- Counters: `attempted` (read from source) → `rejected` (schema/serialization) or `queued` (in the client buffer) → `delivered` (broker-confirmed) / `failed` (error callback or `produce()` error) / `undelivered` (still unconfirmed when the final `flush()` gave up after `KAFKA_FLUSH_TIMEOUT_S`). **Queued is not delivered.**
- End to end this is **at-least-once**, not exactly-once. Consumers must be idempotent: deduplicate or UPSERT on `event_id`.

| Symptom | Cause / fix |
|---|---|
| `cannot reach Kafka at 127.0.0.1:9092 within 10.0s (... _TRANSPORT ... / _TIMED_OUT ...)`, exit 2 | Broker not running (`docker compose ps`; start with `docker compose up -d --wait kafka`), Docker Desktop not started, or a wrong `KAFKA_BOOTSTRAP_SERVERS` / `--bootstrap-servers` (wrong port gives `_TRANSPORT` quickly) |
| Intermittent multi-second connects or metadata timeouts with `localhost:9092` | **Measured on this machine:** `localhost` resolves to IPv6 `::1` first, nothing listens there, and each attempt took ~2 s to fail. Sometimes several in a row exceeded the 10 s metadata timeout. Fix (already applied): the broker *advertises* `127.0.0.1:9092`, and the default bootstrap is `127.0.0.1:9092`. Use `127.0.0.1`, not `localhost`, from your machine |
| Docker networking / advertised listeners | A client first contacts the bootstrap address, then reconnects to whatever address the broker **advertises**. Host programs use `PLAINTEXT_HOST` → `127.0.0.1:9092`. Other containers on the compose network must use `kafka:19092` (`PLAINTEXT` listener). Advertising an address the client can't reach causes connect loops even though the bootstrap worked |
| `topic 'datacenter.telemetry' is not available (... UNKNOWN_TOPIC_OR_PART ...)`, exit 2 | Topic not created (auto-creation is off), or deleted by `docker compose down`. Run `uv run python -m data_engineering.streaming.topic_setup` |
| `event rejected by schema validation` with `problems` | The event breaks the contract (e.g. an observed slot with a `null` metric, an unknown metric, a bad timestamp format). Counted as `rejected`, never sent, exit 1. Fix the source/mapping, not the schema, unless the contract really changed (then bump `schema_version`) |
| Serialization error `serialization: Out of range float values are not JSON compliant` | A `NaN`/`Infinity` reached serialization. `build_event` maps NaN to `null` first, so from the built-in sources this surfaces as a schema rejection instead |
| Queue saturation (`queue_full_waits` > 0 in the summary) | Producing faster than the broker accepts. `produce()` raised `BufferError`; the producer served callbacks and retried, so nothing was dropped. Raise `KAFKA_QUEUE_MAX_MESSAGES`, or throttle with `--rate` / `--speedup` |
| `delivery failed ... _MSG_TIMED_OUT` / `undelivered > 0` | The broker was unreachable for longer than `KAFKA_DELIVERY_TIMEOUT_MS` (or the flush timeout). Exit 1. Check the broker, then rerun |
| Duplicate events | Running the same command twice publishes the same events twice (same `event_id`), which `inspect_stream` shows as `duplicate_event_ids`. Kafka does not deduplicate across runs. The future consumer must. Use a different `--seed`/config to get a different `simulation_run_id` |
| Graceful shutdown | Ctrl+C (SIGINT), SIGTERM, or Ctrl+Break (Windows SIGBREAK) sets a stop flag. The producer stops reading, `flush()`es outstanding messages (up to `KAFKA_FLUSH_TIMEOUT_S`) and prints the summary. **Verified:** a continuous run stopped with Ctrl+Break reported `attempted=28 delivered=28` and exited 0 |
| `could not import 'pandas'` (test skipped) | pandas is not a project dependency. The pandas-NA normalization is covered by a duck-typed test; the real-pandas part runs only if pandas is installed |
| `window too short for all configured anomalies; placed fewer` | Very short `--duration-minutes`: anomalies are placed per window without overlapping, and fewer fit. Informational |

---

## J. Data correctness and provenance

**Three kinds of data**

| Kind | Where | Example |
|---|---|---|
| **Source** | Kaggle files (`data/raw/`), `source_records` in SQLite | `failure_risk_level = "Low"` of row 66,805 |
| **Transformed** | Derived from source values by a documented rule | `region = "North America"` from `region_code = "NA"`; fitted slopes (CPU per unit workload) used by the simulator |
| **Synthetic** | Generated by this project | every metric value, `event_timestamp`, `asset_id`, missing slots, injected anomalies |

The source itself is self-described synthetic data, so even "source" fields are not observations of a real facility. Never present this stream as real operational telemetry, and never present simulated anomalies as real incidents. The `source_type`, `timestamp_source` and `provenance.field_sources` fields exist so downstream code (and AI agents) can't mistake one for the other.

**Missing measurements must not become zero.** `0 kW` or `0 %` CPU is a plausible, *wrong* reading. A detector would flag a fake outage, and averages would be dragged down. A missing slot is an explicit event with `reading_status: "missing"` and `null` metrics, so consumers can count gaps (1 % by default: 311 of 28,800 in the default run) and choose a policy.

**How timestamps are assigned.** The dataset has no timestamps, and its `time_of_day` column is only a three-value bucket. Slots are generated on a regular grid: `event_timestamp = start + slot_index × interval`, in UTC.

- The interval is **half-open**: `[start, start + duration)`. 24 h at 60 s gives slots `00:00` … `23:59`, which is 1,440 slots per asset. `24:00` is the first slot of the next window.
- `--speedup` and `--rate` change only *when* events are sent, never their timestamps.
- The Kafka record timestamp is left as publish time (CreateTime) on purpose. Kafka's time-based retention uses record timestamps, so stamping records with simulated January dates would make the broker delete them almost immediately.

**Seeds and reproducibility.** Each random component draws from its own stream derived from the seed:

- asset selection
- per-asset workload regime
- per-metric noise
- missingness
- anomaly placement per window

So the same seed and config always produce identical events and IDs (tested), and changing `missing_rate` doesn't change the underlying signal. The run ID is a hash of the config + source checksum. The default `simulate` run reproduces Phase 1's stored run `sim-c9944cc35156` exactly.

**Expected event counts.**

```
slots_per_asset = duration_minutes × 60 / interval_seconds        (1440 × 60 / 60 = 1,440)
events          = assets × slots_per_asset                        (20 × 1,440 = 28,800)
events          = min(events, --max-events)                       if --max-events is set
observed ≈ events × (1 − missing_rate); missing ≈ events × missing_rate   (missing slots are still events)
```

Continuous mode has no fixed total. It produces `assets` events per simulated interval until stopped.

---

## K. Future integration

The consumer, database persistence, Parquet history and DuckDB analytics described here as "next" are now **implemented**; see [Part II](#part-ii-consumer-storage-and-analytics).

Still **not implemented**:

1. **More FastAPI endpoints.** The `app` package (`src/app/`, Part III.0) serves `/api/v1/health` and `/api/v1/telemetry/cooling-state`. New read-only endpoints go into the `telemetry` domain or new domain packages:
   - assets
   - per-asset readings in a time window: `readings_between`, backed by the `(asset_id, event_timestamp)` index
   - `anomaly_records`
   - ingestion health: `ingestion_batches`, consumer lag

   Plus analytical endpoints over `TelemetryAnalytics` (DuckDB on Parquet). The API never reads Kafka directly for request/response queries.
2. **Frontend and diagnostic agents** call the API. Agents must cite `event_id`s, filter by `simulation_run_id`, and respect `source_type` / `field_sources` (everything is simulated) when making claims.
3. An anomaly **detector**. Today `anomaly_records` holds only the simulator's injected labels, which is ground truth for evaluating a future detector.

---

## L. Limitations and next steps

**Limitations**

- **All telemetry is synthetic.**
  - Relationships come from an engineered dataset, and units are inferred from column names.
  - The diurnal workload shape is an assumption.
  - Asset labels are static per asset, not time-varying risk assessments.
  - Simulated asset IDs repeat across runs; filter by `simulation_run_id`.
- **Single broker, replication factor 1, no TLS/auth: local development only.** `docker compose down` deletes topic data. No Schema Registry: the JSON Schema file is the contract.
- **Delivery is at-least-once with idempotent storage, not exactly-once.** DLQ records can repeat after restarts or with several consumer groups.
- **Consumer scale.** Tested with one consumer process per group:
  - SQLite allows one writer, so run a single instance.
  - With PostgreSQL, several instances should work for the database writes, but concurrent Parquet exporters were not tested.
- **One poison message blocks its partition while the DLQ is unavailable.** That's correctness over availability (II.3).
- **Retry timing.** Worst-case retry time includes the PostgreSQL connect timeout (10 s per reconnect attempt); keep it under `KAFKA_MAX_POLL_INTERVAL_MS`.
- **Storage limits.**
  - No retention or pruning of `telemetry_readings` yet; it grows with the stream.
  - Schema migrations are a version table plus idempotent DDL, not a migration framework.
  - Parquet history lags the database by up to `PARQUET_FLUSH_ROWS` / `PARQUET_FLUSH_INTERVAL_S`.
- **Lag reporting.** Consumer lag comes from the client's cached watermarks; it's `null` when unknown.
- **Tested on:** Windows 11 (Git Bash + PowerShell 5.1), Docker Desktop, Kafka 3.9.1, PostgreSQL 16 (Docker), SQLite 3.45. Linux/macOS were not run.

**Suggested development sequence**

1. FastAPI read endpoints over the telemetry store and DuckDB analytics (K.1).
2. A simple anomaly-detection consumer group (rolling robust z-score per asset and metric), evaluated against `anomaly_records`.
3. Retention for `telemetry_readings`: delete rows that are both older than N days and exported (`export_id` is set).
4. Dashboard, then AI-agent investigation over stored evidence.

---

# Part II: Consumer, storage and analytics

Part I (sections A–L) built the producer. Part II is the other side of the topic: the **Kafka consumer** that validates every event, stores it in a database (SQLite by default, PostgreSQL optional), keeps an analytical history in **Parquet**, and answers historical questions with **DuckDB**. FastAPI, the dashboard and agents are still **not implemented**.

## II.1 End-to-end architecture

```mermaid
flowchart LR
    subgraph implemented["Implemented"]
        PR[Kafka producer] --> T[["datacenter.telemetry<br/>3 partitions"]]
        T --> C["Consumer, group dco-telemetry-sink<br/>validate → batch"]
        C -->|"① one transaction per batch<br/>(idempotent on event_id)"| DB[("Telemetry store<br/>SQLite data/telemetry_store.db<br/>or PostgreSQL")]
        C -->|"② invalid messages"| DLQ[["datacenter.telemetry.dlq"]]
        C -.->|"③ commit offsets<br/>after ① and ②"| T
        DB -->|"④ export ledger<br/>(redoable)"| PQ[("Parquet history<br/>data/telemetry/region_code=…/event_date=…")]
        PQ --> DD[DuckDB analytics<br/>query_telemetry CLI]
    end
    subgraph planned["Planned: NOT implemented"]
        DB -.-> API[FastAPI]
        DD -.-> API
        API -.-> UI[Frontend]
        API -.-> AG[Diagnostic agents]
    end
```

| Component | Responsibility in this project | Not its job |
|---|---|---|
| **Producer** (Part I) | Turn simulated slots into validated events, key them by `asset_id`, get broker confirmation | Storage, deduplication |
| **Topic** `datacenter.telemetry` | Durable, ordered-per-partition buffer between producer and consumers; replayable for 7 days | Queries, deduplication, long-term history |
| **Consumer** (`streaming/consumer.py`) | Validate, transform, write batches, dead-letter bad messages, commit offsets only after the work is durable | Analytics |
| **Telemetry store** (SQLite/PostgreSQL) | Operational source of truth: asset inventory, queryable telemetry, explicit anomaly labels, batch metadata, Parquet export ledger | Large historical scans |
| **DLQ topic** `datacenter.telemetry.dlq` | Keep every unprocessable message with its reason and original bytes, so nothing is silently dropped | Retrying (bad messages are permanently bad) |
| **Parquet** (`storage/parquet_sink.py`) | Compressed, columnar, append-only history, partitioned by region and day | Transactions, updates, deduplication |
| **DuckDB** (`analytics/duckdb_queries.py`) | SQL over the Parquet files in place: aggregates, buckets, completeness | Being a database of record |

**Why Kafka is not the database.** Kafka keeps messages only for the retention period (7 days here). It can't answer "SIM-NA-004 between 10:00 and 11:00" without reading every message, and it has no notion of uniqueness: the same event published twice is two messages (we published one run twice on purpose, see II.6). The consumer turns the stream into **stored state that can be queried and deduplicated**.

## II.2 What validation, transformation and batch writes mean here

**Validation** (`streaming/event_validation.py`) runs on every message *before* anything is written. It reuses the producer's own contract, `telemetry_event.v1.schema.json`, so producer and consumer cannot drift apart. On top of that, it checks things only a consumer can see:

| Reason code (DLQ `failure_reason`) | Example |
|---|---|
| `malformed_json` | empty value, not UTF-8, `{not json`, `NaN` literals |
| `not_json_object` | `[1, 2, 3]` |
| `unsupported_schema_version` | `"schema_version": "9.9"` (this consumer understands `1.0`) |
| `schema_violation` | missing `asset_id`, unknown top-level field, empty identifier |
| `invalid_timestamp` | `2026-01-01 00:00:00` (wrong format), `2026-02-30T00:00:00Z` (no such day) |
| `invalid_metric_value` | `"12.5"` (string), `true`, `1e400` (overflows to infinity), `null` on an *observed* reading, unknown metric |
| `invalid_identifier` | `event_id` not equal to `<simulation_run_id>:<asset_id>:<slot_index>`; deduplication depends on this |

Missing readings (`reading_status: "missing"`, all metrics `null`) are **valid** and stored as SQL `NULL` and Parquet null, never 0. A message whose Kafka key differs from its `asset_id` is stored but counted (`key_mismatches`) and logged, because per-asset ordering is no longer guaranteed for it.

**Transformation** (`storage/base.py: prepare_event`) maps one event onto table rows:

| Event field | Stored as |
|---|---|
| `event_id`, `simulation_run_id`, `asset_id`, `slot_index`, `reading_status`, `schema_version`, `source_type`, `timestamp_source` | same-named columns of `telemetry_readings` |
| `event_timestamp` | `event_timestamp` (normalized UTC instant) **and** `event_timestamp_raw` (the exact received string, for traceability) |
| `metrics.*` (16) | 16 typed numeric columns (`NULL` when missing) |
| `asset_attributes.*`, `labels.source_*`, `provenance.*` | `assets` row (static per asset and run); `provenance.published_from` stays on the reading |
| `labels.injected_anomalies[]` | one `anomaly_records` row per anomaly per event |
| Kafka topic / partition / offset | `kafka_topic`, `kafka_partition`, `kafka_offset` on the reading |

**Batch writes.** The consumer collects up to `CONSUMER_BATCH_MAX_MESSAGES` (1,000) messages or waits at most `CONSUMER_BATCH_MAX_WAIT_S` (2 s), whichever comes first. It then writes all valid events of that batch in **one transaction**: either every row of the batch exists afterwards, or none does. One commit per batch instead of per event is what makes this fast. Measured on this laptop with SQLite: 57,666 messages in 58 batches, about 32 s of processing.

## II.3 Consumer groups, partitions, offsets, commits, retries, dead letters

- **Consumer group.** `KAFKA_GROUP_ID` (default `dco-telemetry-sink`) names the consumer. Kafka stores *per group* how far it has read. A second group (we used `dco-telemetry-sink-pg` for PostgreSQL) reads the same topic independently from its own position.
- **Partitions.** The topic has 3 partitions. Within a group, each partition is read by exactly one consumer process, so up to 3 processes could share the work. One process is the tested setup (see limitations). Because the producer keys by `asset_id`, all of one asset's events are in one partition, in order.
- **Offsets and commits.** Committing offset *N* for a partition means "everything before *N* is done; resume at *N*". Auto-commit is **disabled** (`enable.auto.commit=false`, `enable.auto.offset.store=false`). The consumer commits synchronously, and only:
  1. after the batch's database transaction has committed, and
  2. after every invalid message of the batch has been confirmed by the broker on the DLQ.
- **Never past a gap.** `OffsetTracker` records, per partition, which offsets are handled. It commits only up to the end of the *contiguous* handled run. If offset 5 could not be dead-lettered, offsets 6, 7, … of that partition are not committed either, even though they were stored (`test_dlq_failure_blocks_commits_past_the_invalid_message`).
- **Retries** use bounded exponential backoff with jitter: delay = `cap/2 + random × cap/2`, with `cap = min(CONSUMER_RETRY_BACKOFF_MAX_S, CONSUMER_RETRY_BACKOFF_INITIAL_S × 2^attempt)`. That gives 0.375 s, 0.75 s, 1.5 s, … up to `CONSUMER_MAX_RETRIES` attempts.

| Failure | Retried? | If it keeps failing |
|---|---|---|
| Transient DB error (SQLite `database is locked` / `disk I/O error`; PostgreSQL connection loss, SQLSTATE 08/40/53/57P) | yes, reconnecting between attempts | consumer stops, exit 1, **nothing committed**; the batch is redelivered after restart |
| Permanent DB error (constraint violation, bug) | no | same: stop, exit 1, nothing committed |
| Invalid message | no (it will never become valid) | sent to the DLQ |
| DLQ not confirming | yes | commit the safe prefix, stop, exit 1 |
| Offset commit error | yes if retriable; not for rebalances (`ILLEGAL_GENERATION`, …) | logged; events are redelivered and deduplicated; the next successful commit covers them |
| Parquet export error | on the next loop, after a 5 s backoff | stays in the ledger; reported at shutdown with exit 1 |

The settings loader refuses `CONSUMER_MAX_RETRIES × CONSUMER_RETRY_BACKOFF_MAX_S` close to `KAFKA_MAX_POLL_INTERVAL_MS`. If one batch's retries took longer than that, Kafka would evict the consumer from the group mid-retry.

- **Rebalances.** A batch is fully processed and committed before the next poll, so normally nothing is in flight when partitions move. `on_revoke` drops tracking for revoked partitions. Anything uncommitted is redelivered to the new owner and deduplicated.
- **Dead letters.** Each DLQ record is JSON around the untouched original. This is a real record from the demo, payload shortened:

```json
{"dlq_schema_version": "1.0", "failure_reason": "malformed_json",
 "failure_details": ["Expecting ',' delimiter: line 1 column 47 (char 46)"],
 "failed_at": "2026-10-13T07:41:59.361491Z", "consumer_group": "dco-telemetry-sink",
 "source_topic": "datacenter.telemetry", "source_partition": 1, "source_offset": 20160,
 "source_timestamp_ms": 1791877316724, "source_key": "SIM-NA-003",
 "original_payload_utf8": "{\"schema_version\": \"1.0\", \"event_id\": \"broken\"", "original_payload_base64": null}
```

Headers repeat the reason and the source coordinates. Non-UTF-8 payloads are kept in `original_payload_base64`. **DLQ unavailable:** dropping the message or committing past it would lose it silently, so the consumer commits only the safe prefix and stops (exit 1). The trade-off is that one poison message plus a DLQ outage halts consumption of that partition until the DLQ is back. That's a deliberate choice of *correctness over availability*.

## II.4 At-least-once delivery and idempotent writes

Kafka offsets and the database cannot be committed in one atomic operation, so there is always a window where one has happened and the other hasn't. The design makes that window harmless:

| What happens | Result | Why it is safe |
|---|---|---|
| Crash before the DB commit | batch rolled back, offsets not committed | redelivered, written on restart |
| Crash after the DB commit, before the offset commit | batch redelivered | every event already exists: counted as `rows_deduplicated`, not inserted |
| Offset commit fails | same as above | idempotent insert |
| Producer publishes the same run again | same `event_id`s arrive again | deduplicated |
| Replay with a new group or reset offsets | every event arrives again | deduplicated; no new Parquet rows (only *newly inserted* rows are exported) |
| Same `event_id`, **different content** | `rows_conflicting` + warning log | first stored version wins, never silently overwritten |

"Same content" is a SHA-256 fingerprint (`payload_sha256`) of the event without `provenance.published_from`. Replaying a stored run therefore counts as a duplicate, not a conflict.

This is **at-least-once delivery + idempotent writes = each event stored once**. It is *not* exactly-once processing: side effects outside the database can repeat, for example a DLQ record published twice by two groups.

Verified on the real broker:

- **Draining the existing topic.** 57,666 messages, including one 24-hour run published twice, became 28,834 stored rows and 28,832 duplicates.
- **Hard kill.** A consumer killed mid-stream (`TerminateProcess`) and restarted ended with 14,400 / 14,400 rows in the database and Parquet, with no duplicates.
- **Full replay.** A new group re-read everything and inserted 0 new rows (`tests/integration/test_consumer_integration.py`).

## II.5 SQLite or PostgreSQL

| | SQLite (default) | PostgreSQL (optional) |
|---|---|---|
| Setup | nothing: a file at `data/telemetry_store.db` | `docker compose --profile postgres up -d --wait postgres` or any server |
| Concurrency | one writer at a time (WAL lets readers run alongside) | many writers and readers |
| Durability settings | `journal_mode=WAL`, `synchronous=FULL` (a committed batch survives power loss; needed because offsets are committed right after) | server-managed |
| Types | text timestamps in one fixed-width UTC format (`2026-01-01T00:00:00.000000Z`, so text order = time order), REAL | `TIMESTAMPTZ`, `DOUBLE PRECISION`, `JSONB` |
| Good for | local development, demos, the hackathon | a shared API, several consumer instances, larger data |

Both run **the same SQL**, written once with `?` placeholders (`storage/base.py`). The backends only differ in connection handling, type conversion and error classification (`sqlite_store.py`, `postgres_store.py`). All values are bound parameters, and table/column names come only from constants. Switching needs no code change: `TELEMETRY_DB_BACKEND=postgres` plus `TELEMETRY_POSTGRES_URL`. Passwords are never logged; URLs appear as `postgresql://dco:***@…`.

## II.6 Parquet for history, DuckDB for questions

**Why Parquet.** Columnar and compressed: the 28,834-row demo history is **1.8 MB** on disk, versus about 1.7 KB of JSON per event on the topic. A query reading 2 of 40 columns reads only those 2. Files are immutable, so many readers can scan them safely. Partition directories (`event_date=2026-01-01`) let a query skip whole days.

**Why not write Parquet directly from Kafka.** A Parquet file is not a transaction log, and the database and a file write can't commit atomically. So the database is the durability point, and Parquet is **derived from it** through an export ledger:

1. Each committed batch is recorded in `ingestion_batches`, still unassigned.
2. When `PARQUET_FLUSH_ROWS` (50,000) rows are waiting, or the oldest has waited `PARQUET_FLUSH_INTERVAL_S` (60 s), or at shutdown, an export job is claimed in one transaction (`parquet_exports`, status `writing`).
3. Its rows are read back from the database and written as `part-<export_id>-NNNN.parquet.tmp`, then atomically renamed.
4. The job is marked `done`.

The guarantees that follow:

- A crash anywhere in steps 2–4 leaves the job `writing`. It is redone on the next start, writing the **same file names**, so history is never duplicated.
- Only rows a batch *newly inserted* belong to it, so Kafka redeliveries never reach Parquet twice.
- If a Parquet write fails, Kafka offsets are still committed (the data is safe in the database), the job stays pending, and the failure is logged, counted and reported at shutdown (exit 1).
- Files are bounded (`PARQUET_MAX_ROWS_PER_FILE` 250,000), and the flush threshold avoids tiny files.

**How DuckDB reads it.** `read_parquet('data/telemetry/**/*.parquet', hive_partitioning = true)` treats the directory tree as one table. The partition directories become the columns `region_code` and `event_date`. Nothing is imported into a database or pandas first; DuckDB reads only the needed columns and partitions. Time filters are also applied to `event_date`, so out-of-window days are skipped. The glob ignores `*.tmp`, so half-written files are never read.

## II.7 Event schema seen by the consumer

The consumer accepts exactly the producer's format: schema `1.0`, see section G for a full real example and field-by-field provenance. Contract file: `src/data_engineering/streaming/telemetry_event.v1.schema.json`. `SUPPORTED_SCHEMA_VERSIONS = {"1.0"}`. A future `2.0` must be added to the consumer **before** producers start sending it. Until then, such messages go to the DLQ as `unsupported_schema_version`; they are not lost.

## II.8 Database schema

Two dialect files define the same tables: `db/telemetry_store_sqlite.sql` and `db/telemetry_store_postgres.sql`. Initialization (`db/init_telemetry_store.py`) is separate from consumption, and the consumer refuses an uninitialized database. This store is **separate from the Phase 1 database**, because Phase 1 already has a different `telemetry_readings` table. Init refuses to run against it.

| Table | Key | Purpose |
|---|---|---|
| `assets` | (`simulation_run_id`, `asset_id`) | Inventory: data center, region, `asset_type` (= `server_type`), cooling, age, rack position, energy source, the baseline row's labels, provenance (dataset, file checksum, generator, seed), first/last event time. Keyed per run because `SIM-NA-001` is a different simulated machine in every run (the demo store has 24 asset rows for 20 distinct IDs). Updated by upsert; attributes change only if the incoming event is newer, and first/last use `min`/`max` |
| `telemetry_readings` | `event_id` | One row per event: identifiers, normalized + raw timestamp, `reading_status`, the 16 typed metrics (`NULL` = missing; a `CHECK` forbids values on missing readings), `payload_sha256`, Kafka coordinates, `ingested_at`, `ingest_batch_id`. Indexes: (`asset_id`, `event_timestamp`) for per-asset time ranges, `event_timestamp`, `ingest_batch_id` |
| `anomaly_records` | (`anomaly_id`, `event_id`) | Only events carrying `labels.injected_anomalies`: `anomaly_type`, `affected_metrics_json`, `label_source = 'simulator_injected'`. There's **no severity column**: the events don't carry one, and nothing here is a detector's output |
| `ingestion_batches` | `batch_id` | Per batch: group, times, messages received / valid / rejected, rows inserted / deduplicated / conflicting, anomaly rows, DLQ count, per-partition offset ranges, `status` (`persisted` → `committed`, or `commit_failed`), `export_id` |
| `parquet_exports` | `export_id` | The export ledger: `writing` / `done`, row and batch counts, files, attempts, last error |
| `schema_migrations` | `version` | Schema version (1) |

**Why typed metric columns and not JSON.** The metric set is fixed by the versioned schema: exactly 16 named properties, with no extra metrics allowed. That isn't dynamic, so typed columns give real types, `CHECK` constraints and efficient filtering. A new metric means `schema_version` 2.0 plus a migration.

## II.9 Parquet layout and schema

```
data/telemetry/                                   (PARQUET_OUTPUT_DIR; gitignored under data/)
└── region_code=NA/
    └── event_date=2026-01-01/
        └── part-exp-20261013T073621-52e27101-0000.parquet
```

Columns:

- **Identity and time:** `event_id`, `asset_id`, `simulation_run_id`, `data_center_id`, `region`, `event_timestamp` (`timestamp[us, UTC]`), `event_timestamp_raw`, `slot_index`.
- **Status and provenance:** `reading_status`, `schema_version`, `source_type`, `timestamp_source`, `published_from`.
- **Asset and labels:** `asset_type`, the 16 metrics (`float64`, nullable), the three `source_*` labels, `injected_anomalies` (list of `{anomaly_id, anomaly_type, affected_metrics}`).
- **Kafka coordinates and batch:** `kafka_topic`, `kafka_partition`, `kafka_offset`, `ingested_at`, `ingest_batch_id`, `export_id`.

The schema is declared explicitly in `parquet_sink.FILE_FIELDS` and is identical in every file (tested), with a schema-version file metadata key. Partition values are sanitized: only `A–Z a–z 0–9 . _ -`, never empty or `..`. `PARQUET_PARTITION_STRATEGY=date` drops the region level and keeps `region_code` inside the files.

## II.10 Setup and running

Prerequisites: everything from section E (Python 3.13, uv, Docker Desktop), plus the Phase 1 data if you want to publish simulated events.

```bash
uv sync                                                  # includes pyarrow, duckdb; psycopg via the dev group
docker compose up -d --wait kafka
uv run python -m data_engineering.streaming.topic_setup  # creates datacenter.telemetry AND datacenter.telemetry.dlq
# topic datacenter.telemetry: already existed, 3 partitions, 1 broker(s)
# topic datacenter.telemetry.dlq: created, 3 partitions, 1 broker(s)
uv run python db/init_telemetry_store.py                 # SQLite store, idempotent
# initialized sqlite:...\data\telemetry_store.db (schema version 1): assets=0, telemetry_readings=0, ...
```

(Production-style install without dev tools: `uv sync --no-dev --extra postgres`. `requirements.txt` lists the core dependencies only.)

**Run the consumer (SQLite)**

```bash
uv run python -m data_engineering.streaming.producer simulate --assets 3 --duration-minutes 10   # 30 events
uv run python -m data_engineering.streaming.consumer --idle-timeout-s 10   # finite: stop 10 s after the last message
uv run python -m data_engineering.streaming.consumer                        # normal: run until Ctrl+C
uv run python -m data_engineering.streaming.consumer --max-messages 500     # finite: stop after 500 messages
```

Options:

- `--max-batches`, `--group-id`, `--topic`, `--dlq-topic`, `--bootstrap-servers`
- `--db-backend`, `--sqlite-path`, `--parquet-dir`, `--no-parquet`

Exit status: `0` ok · `1` runtime failure (retries exhausted, DLQ unavailable, Parquet backlog at shutdown) · `2` configuration/startup problem.

Ctrl+C (SIGINT), SIGTERM and Ctrl+Break (SIGBREAK) finish the current batch. On shutdown the consumer then:

1. commits the batch's offsets,
2. exports everything persisted to Parquet,
3. flushes the DLQ producer,
4. leaves the group.

The idle timer starts only once partitions are assigned. Right after a crash, Kafka may wait up to the session timeout (45 s) before reassigning the dead member's partitions.

**Run the consumer with PostgreSQL (optional)**

```bash
# 1. In .env (never commit it):
#    POSTGRES_PASSWORD=<choose one>
#    TELEMETRY_DB_BACKEND=postgres
#    TELEMETRY_POSTGRES_URL=postgresql://dco:<same password>@127.0.0.1:55432/dco
docker compose --profile postgres up -d --wait postgres
uv run python db/init_telemetry_store.py
# initialized postgres:postgresql://dco:***@127.0.0.1:55432/dco (schema version 1): ...
uv run python -m data_engineering.streaming.consumer --group-id dco-telemetry-sink-pg --parquet-dir data/telemetry_pg --idle-timeout-s 10
```

A separate group and Parquet directory let SQLite and PostgreSQL both hold the full stream. For an existing server, just point `TELEMETRY_POSTGRES_URL` at it. One-off overrides in PowerShell: `$env:TELEMETRY_DB_BACKEND = "postgres"`. In Git Bash: `TELEMETRY_DB_BACKEND=postgres uv run ...`.

## II.11 Querying with DuckDB

All commands print JSON. The sample outputs below are from the first full drain of the topic in this session; yours depend on what was published. `--start` is inclusive and `--end` exclusive, in UTC. Asset IDs repeat across simulation runs, so `overview` lists the runs and `--run` selects one.

```bash
Q="uv run python -m data_engineering.analytics.query_telemetry"     # Git Bash; in PowerShell type the command out
$Q overview
#   "rows": 28834, "distinct_event_ids": 28834, "assets": 20, "files": 1, "missing_readings": 311,
#   "runs": {"sim-c9944cc35156": 28800, "sim-0531de355e95": 28, "sim-12b37278e5e8": 6}
$Q readings SIM-NA-001 --run sim-c9944cc35156 --start 2026-01-01T00:00:00Z --end 2026-01-01T00:03:00Z --metrics cpu_utilization power_consumption_kw
$Q stats --metrics power_consumption_kw inlet_temperature_c --all-assets
#   power_consumption_kw: readings 28523, nulls 311, avg 118.35, min 32.987, max 448.364
$Q counts --bucket hour --asset SIM-NA-004 --run sim-c9944cc35156   # 24 buckets x 60 events, observed/missing split
$Q anomalies --bucket day --run sim-c9944cc35156                    # explicit injected labels per asset and type
$Q completeness SIM-NA-001 --run sim-c9944cc35156 --start 2026-01-01T00:00:00Z --end 2026-01-02T00:00:00Z
#   "expected_slots": 1440, "events": 1440, "observed_readings": 1428, "missing_readings": 12,
#   "absent_slots": 0, "duplicate_events": 0, "absent_intervals": []
```

`completeness` separates **missing readings** (the producer reported the slot with `null` metrics) from **absent slots** (no event at all, i.e. lost or never published), and lists absent intervals. Without `--run`, two runs of the same asset show up as `duplicate_events`.

In Python: `TelemetryAnalytics("data/telemetry").metric_stats(["cpu_utilization"], start, end, run_id=...)`.

All values are bound parameters. Metric names must be in the schema's 16-metric allowlist, and bucket units in `minute|hour|day`. Anything else is a `ValueError` (the CLI also rejects it at argument parsing). An empty or missing directory returns empty results, not an error.

## II.12 Tests

| Command | What runs | Needs |
|---|---|---|
| `uv run pytest` | all unit tests: validation, store, consumer (mocked Kafka), Parquet export/recovery, DuckDB, plus Part I tests | nothing |
| `TELEMETRY_TEST_POSTGRES_URL=postgresql://dco:<pw>@127.0.0.1:55432/dco uv run pytest -m postgres` | every store test again on PostgreSQL, in a throwaway schema | a PostgreSQL server |
| `uv run pytest -m integration` | real broker: produce → consume → SQLite + DLQ + Parquet → DuckDB, restart, full replay | `docker compose up -d kafka` |

Without `TELEMETRY_TEST_POSTGRES_URL`, the PostgreSQL variants are reported as **skipped**, not passed.

Results at the time of writing, with everything enabled: **210 passed, 1 skipped**. The skip is a real-pandas check; pandas isn't a dependency.

Files:

- `test_event_validation.py`, `test_telemetry_store.py`, `test_consumer.py`, `test_parquet_export.py`, `test_duckdb_queries.py`
- `integration/test_consumer_integration.py`
- Helpers in `kafka_fakes.py` and `telemetry_helpers.py`

## II.13 Verifying counts across Kafka, the database and Parquet

```bash
export MSYS_NO_PATHCONV=1   # Git Bash only
# Kafka: messages in the topic (sum of end offsets) and this group's position/lag
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic datacenter.telemetry
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group dco-telemetry-sink
# Database: what the consumer saw and stored
uv run python -c "import sqlite3; c=sqlite3.connect('data/telemetry_store.db'); print(c.execute('select sum(messages_received), sum(events_valid), sum(events_rejected), sum(rows_inserted), sum(rows_deduplicated), sum(rows_conflicting) from ingestion_batches').fetchone(), c.execute('select count(*), count(distinct event_id) from telemetry_readings').fetchone())"
# Parquet: rows and distinct events
uv run python -m data_engineering.analytics.query_telemetry overview
```

The identities that must hold, with the real numbers from the first drain in this session:

- **Kafka messages consumed** (offsets, lag 0) = `messages_received` = valid + rejected: 57,666 = 57,666 + 0.
- **Rows in `telemetry_readings`** = Σ `rows_inserted` = valid − deduplicated − conflicting: 28,834 = 57,666 − 28,832 − 0.
- **Parquet rows** = DB rows, once `parquet_exports` has no `writing` jobs and no batch has `export_id IS NULL` with rows: 28,834 = 28,834, distinct 28,834.
- **DLQ records** ≥ rejected. They can exceed it after restarts or with several groups (at-least-once).

## II.14 Troubleshooting

| Symptom (actual messages) | Meaning / fix |
|---|---|
| `consumer could not start ... cannot reach Kafka at 127.0.0.1:9092`, exit 2 | Broker down or wrong `KAFKA_BOOTSTRAP_SERVERS`. See section I |
| `topic(s) ['datacenter.telemetry.dlq'] not found. Create them with ...topic_setup`, exit 2 | Run `uv run python -m data_engineering.streaming.topic_setup` (auto-creation is disabled) |
| `telemetry store schema version is None, expected 1. Run ... db/init_telemetry_store.py first.`, exit 2 | Initialize the store (`TELEMETRY_DB_BACKEND` decides which) |
| `this database already has a different telemetry_readings table (the Phase 1 database?)` | `TELEMETRY_SQLITE_PATH` points at `data/datacenter_ops.db`. Use the default `data/telemetry_store.db` |
| `ConnectionTimeout: connection timeout expired`, exit 2 | PostgreSQL not reachable at startup: `docker compose --profile postgres ps`, check URL and password |
| `database write failed; retrying` ... then `database write failed (transient, retries exhausted)`, exit 1 | Database outage longer than the retry budget. Nothing was committed; fix the database and start the consumer again, and it resumes exactly there. *Measured:* a 5.2 s PostgreSQL restart mid-run was absorbed by 2 retries; a permanent outage stopped the consumer with lag unchanged, and the rerun processed all 120 pending events |
| Restart right after a crash reads nothing for a while | The crashed member still holds the partitions until `KAFKA_SESSION_TIMEOUT_MS` (45 s) expires. A clean stop leaves the group immediately |
| `message rejected` with `reason` (first 5 per reason are logged) | Bad producer data. Inspect the DLQ topic: `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic datacenter.telemetry.dlq --from-beginning --property print.key=true` |
| `unsupported_schema_version` in the DLQ | A producer sends a newer schema. Upgrade the consumer, then re-publish from the source (DLQ records keep the original payload) |
| `DLQ publish not confirmed; retrying` → `could not be published to datacenter.telemetry.dlq`, exit 1 | DLQ topic or broker unavailable. Offsets stop just before the poison message; nothing is lost. Fix and restart |
| `offset commit failed` / `giving up on this commit` | Typically a rebalance. Harmless: the events are stored, will be redelivered, and counted as `rows_deduplicated` |
| `rows_deduplicated` high | Expected after republishing, replays or restarts |
| `conflicting duplicates: same event_id, different content` | The same event ID arrived with different data, i.e. a producer bug. The first version is kept; see `rows_conflicting` in `ingestion_batches` |
| Reprocess everything on purpose | Stop the consumer, then `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group dco-telemetry-sink --topic datacenter.telemetry --reset-offsets --to-earliest --execute`. Safe thanks to idempotent writes |
| `parquet export failed; it stays pending` / `parquet export backlog remains ... at shutdown`, exit 1 | Disk full, permissions, or the database down during export. Rows are safe in the database. Fix and start the consumer again: pending exports are redone first. Inspect with `select * from parquet_exports where status = 'writing'` |
| DuckDB shows more rows per asset than expected | Several runs share asset IDs. Use `--run` (see `overview` → `runs`) |

---

# Part III: Cooling State Estimator

A **deterministic, rules-based, read-only** component. It reads the latest telemetry plus a rolling history from the telemetry store (Part II) and returns a structured snapshot of the thermal and operational state, for one asset or for the whole data center. It does **not** optimize anything, change cooling settings, call an LLM, schedule itself or consume Kafka. It is exposed as `GET /api/v1/telemetry/cooling-state` and as a CLI.

## III.0 API structure (`src/app/`)

The HTTP API is the `app` package, next to the pipeline package `data_engineering`. Both are built from `src/` (`[tool.uv.build-backend] module-name = ["data_engineering", "app"]`). Its layout follows the domain-module convention: one package per business domain, each with its own router, schemas, dependencies, service and exceptions. Modules are added when a domain is actually built; there are no placeholder stubs.

```
src/app/
├── main.py              # the single FastAPI entry point; lifespan creates the shared objects
├── config.py            # AppSettings (pydantic-settings, API_ prefix)
├── constants.py         # title, version, /api/v1 prefix, request-ID header
├── exceptions.py        # AppError hierarchy + handlers rendering every error as ErrorResponse
├── middleware.py        # request ID + access log, security headers, optional CORS
├── database.py          # StoreFactory + get_db_session (wraps the existing store, no ORM)
├── api/v1/router.py     # aggregates domain routers; /health
├── telemetry/           # domain module (copy this layout for new domains)
│   ├── router.py        # GET /telemetry/cooling-state
│   ├── schemas.py       # CoolingStateQuery (query parameters) + CoolingStateSnapshot (response)
│   ├── dependencies.py  # get_cooling_config, get_telemetry_service
│   ├── service.py       # TelemetryService: runs the estimator, maps errors to domain exceptions
│   ├── exceptions.py    # AssetNotFoundError, RunNotFoundError, InvalidTelemetryQueryError
│   └── constants.py     # ID pattern, window bounds
└── common/
    ├── schemas/base.py       # CustomModel (extra="forbid"), UtcDatetime (timezone required, normalized to UTC)
    ├── schemas/responses.py  # ErrorResponse
    ├── logging.py            # dco.api.* loggers (same JSON log stream as the pipeline)
    └── security/headers.py   # security-headers ASGI middleware
```

Not created yet, because none of it exists: `auth/`, `rag/`, `agents/`, `cognition/`, `execution/`, `llm/`, `guardrails/`, `providers/`, `evaluation/`, `pagination.py`, shared `models.py` (no ORM) and `docs/standards/`. Endpoints are **unauthenticated**, so bind to 127.0.0.1.

**Lifespan.** Created once at startup:
- the validated cooling config (an invalid `config/cooling_state.yaml` **stops startup**);
- the `StoreFactory`;
- the threadpool limiter size (`API_THREADPOOL_TOKENS`).

A database that is down at startup does not stop the app; requests return 503 until it's back.

**Database.** `get_db_session` opens one store connection per request and closes it afterwards. Handlers are sync `def`, so database work runs in the threadpool, never on the event loop. FastAPI runs the sync dependency and the handler in *different* worker threads. So SQLite connections are opened with `check_same_thread=False`, and each is used by one thread at a time. A concurrency test caught this; the earlier single-file API had the same latent bug.

**Errors.** Every non-2xx response, including validation errors, unknown routes, 405s and unexpected exceptions, has one shape. The submitted input is never echoed back:

```json
{"error": {"code": "unknown_asset", "message": "asset 'NOPE' not found in simulation run 'sim-c9944cc35156'", "details": null},
 "request_id": "41450fa8bec24571bfd1a8de27df8784"}
```

**Middleware.**
- `X-Request-ID` is accepted if well-formed (`[A-Za-z0-9._-]{1,64}`), otherwise generated, and echoed on every response.
- One access-log line per request, recording the path without the query string.
- Security headers: `nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, and a strict CSP plus `no-store` on everything except `/docs`.
- CORS only for `API_CORS_ORIGINS`.

**Paths.** `/api/v1/health` and `/api/v1/telemetry/cooling-state`. The pre-v1 `/api/health` and `/api/telemetry/cooling-state` still answer, as deprecated aliases hidden from the OpenAPI schema.

**Tests.**
- `tests/telemetry/test_cooling_state_api.py`
- `tests/security/api/test_http_hardening.py`
- `tests/test_app_lifecycle.py`

They use an async client (`httpx.AsyncClient` + `ASGITransport`) with the real lifespan; fixtures are in `tests/conftest.py`.


## III.1 What the data provides (inspected, not assumed)

**Source table.** `telemetry_readings` in the consumer's store: SQLite `data/telemetry_store.db` or PostgreSQL, selected by `TELEMETRY_DB_BACKEND`. Each row is one reading for one `(simulation_run_id, asset_id)` at one `event_timestamp`. `assets` supplies `asset_type` and the newest event and ingest times.

**Fields the estimator uses** (units from the event schema, inferred from column names):

| Group | Field | Unit |
|---|---|---|
| Thermal | `inlet_temperature_c`, `exhaust_temperature_c` | °C |
| Workload | `cpu_utilization`, `gpu_utilization` | % |
| Workload | `workload_intensity` | 0–1 |
| Cooling / airflow | `fan_speed_rpm` | rpm |
| Cooling / airflow | `airflow_cfm` | cfm |
| Electrical | `power_consumption_kw` | kW, per asset |
| Electrical | `pue` | ratio |

`reading_status` says whether the slot was `observed` or `missing`.

**Not available, and reported as such:**

- **Cooling efficiency.** The Kaggle rows have a static value, but the simulator doesn't produce it per minute.
- **Power-supply efficiency and WUE.** Same situation.
- **Cooling power.** The schema has only per-asset `power_consumption_kw`, so nothing is labeled cooling power and no energy savings are computed.
- **Equipment limits.** None exist in the data or the documentation.

**GPU.** Compute, Edge and Storage servers report `gpu_utilization = 0.0` in every row. That's structural, not a measurement, so for them the GPU metric is `applicable: false` with values `null`, and it's left out of workload and aggregate calculations.

**Two clocks.**

- `event_timestamp` is the measurement time, which is simulated: 2026-01-01.
- `ingested_at` is the wall-clock time the consumer stored the row.

The window is `(as_of − window, as_of]` in event time. `as_of` is the newest reading of the run, unless you pass one explicitly. Freshness is checked on both clocks (see DQ rules).

**Runs.** Simulated asset IDs repeat in every simulation run. So every evaluation is scoped to one run: by default the run that most recently received readings, or `run_id=…`.

## III.2 Metrics (per asset)

For each metric in each asset's window. Readings are sorted by `event_timestamp`. Same-timestamp rows are collapsed (the newest `ingested_at` wins; differing duplicates are counted). Missing values are excluded, never set to 0.

| Field | Meaning |
|---|---|
| `latest`, `latest_timestamp`, `latest_age_seconds` | newest observed value and how far it is behind `as_of` |
| `first`, `change`, `change_elapsed_minutes` | first observed value, `latest − first`, and the real elapsed time between them |
| `mean`, `minimum`, `maximum`, `std_dev` | over observed values (population standard deviation) |
| `trend_per_minute` | least-squares slope against **actual elapsed minutes** (gaps and irregular spacing don't distort it); needs `min_points` (5) |
| `recent_rate_per_minute` | the same slope over the last `recent_minutes` (5); needs ≥ 3 readings |
| `direction` | `increasing` / `decreasing` / `stable`: `trend × elapsed` compared with `trend_min_change` (config, in the metric's unit). Unknown (`null`) with too few readings |
| `observed_points`, `null_points`, `sufficient_points` | how much data the numbers rest on |

**Derived.** `temperature_delta_c` = exhaust − inlet of the **same** reading.

**Workload direction.** Combines CPU, workload intensity, and GPU where applicable. Agreeing non-stable directions win, disagreement gives `mixed`, all stable gives `stable`.

**Data-center scope.** Every asset is computed separately; readings of different assets are never merged into one series. `metrics` then holds statistics of each asset's own values:

- `mean_of_latest`, `min_of_latest`, `max_of_latest` (+ `max_asset_id`)
- `mean_trend_per_minute`, `max_trend_per_minute` (+ asset)
- direction counts
- `sum_of_latest` for power only, because it's the one additive quantity

`assets` lists every asset's state.

## III.3 Rules

All thresholds live in `config/cooling_state.yaml`, which can be overridden with `COOLING_STATE_CONFIG=<path>`. **Every equipment limit ships as `null`.** A rule whose limit is unset is not evaluated; it's listed in `rules_not_evaluated` with the reason. The config cites ASHRAE TC 9.9's *recommended* 18–27 °C inlet envelope only as a reference to confirm against your own equipment.

| Rule | Kind | Fires when | State | Severity |
|---|---|---|---|---|
| `TH_INLET_ABOVE_LIMIT` | observation | latest inlet ≥ `inlet_temperature_high_c` | HIGH_THERMAL_LOAD | WARNING (CRITICAL if ≥ `inlet_temperature_critical_c`) |
| `TH_EXHAUST_ABOVE_LIMIT` | observation | latest exhaust ≥ `exhaust_temperature_high_c` | HIGH_THERMAL_LOAD | WARNING |
| `TH_DELTA_ABOVE_LIMIT` | observation | latest exhaust − inlet ≥ `temperature_delta_high_c` | HIGH_THERMAL_LOAD | WARNING |
| `TH_SUSTAINED_INLET_RISE` | inference | window inlet trend ≥ `sustained_inlet_rise_c_per_minute` | HIGH_THERMAL_LOAD | WARNING |
| `TH_RAPID_INLET_RISE` | inference | recent inlet rate ≥ `rapid_inlet_rise_c_per_minute` | RAPID_TEMPERATURE_RISE | WARNING |
| `AF_AIRFLOW_DROP_WITH_RISING_TEMPERATURE` | inference | airflow fell ≥ `airflow_drop_fraction` over the window **and** inlet or delta is increasing | POTENTIAL_AIRFLOW_ISSUE | WARNING |
| `AF_FAN_AIRFLOW_DIVERGENCE` | inference | fan speed rose ≥ `fan_airflow_divergence_fraction` while airflow did not rise (airflow tracks fan speed in this dataset, r = +0.995) | POTENTIAL_AIRFLOW_ISSUE | WARNING |
| `WL_INCREASING` / `WL_DECREASING` | observation | workload direction | (context only) | INFO |
| `DQ_NO_READINGS` | data_quality | no observed reading in the window | INSUFFICIENT_DATA | WARNING |
| `DQ_INSUFFICIENT_POINTS` | data_quality | fewer than `min_points` inlet readings; trend rules then skipped | DATA_QUALITY_ISSUE | WARNING |
| `DQ_STALE_ASSET` | data_quality | the asset's newest reading lags `as_of` by > `stale_after_seconds` (event time) | DATA_QUALITY_ISSUE | WARNING |
| `DQ_PIPELINE_STALE` | data_quality | nothing stored for > `max_ingest_age_seconds` (wall clock) | DATA_QUALITY_ISSUE | WARNING |
| `DQ_LOW_COVERAGE` | data_quality | observed / expected readings < `min_coverage` | DATA_QUALITY_ISSUE | WARNING |
| `DQ_MISSING_READINGS` | data_quality | explicitly missing share > `max_missing_fraction` | DATA_QUALITY_ISSUE | WARNING |
| `DQ_TIMESTAMP_GAPS` | data_quality | an interval > `gap_tolerance_factor` × `expected_interval_seconds` | DATA_QUALITY_ISSUE | WARNING |
| `DQ_IRREGULAR_INTERVALS` | data_quality | an interval much shorter than expected | DATA_QUALITY_ISSUE | WARNING |
| `DQ_DUPLICATE_TIMESTAMPS` | data_quality | rows shared a timestamp | DATA_QUALITY_ISSUE if their values differ (WARNING), else INFO | |
| `DQ_NO_TELEMETRY` | data_quality | the store has no readings at all | INSUFFICIENT_DATA | WARNING |

Lower fan speed or lower airflow alone are **not** flagged: neither is assumed efficient or safe. The airflow rules need an accompanying temperature or fan signal. Their findings say *potential* and "not a confirmed hardware fault".

**Precedence (deterministic).** All findings are always returned; the precedence only decides the headline state.

1. `INSUFFICIENT_DATA` when the scope has no usable readings. One silent asset in a data-center snapshot counts as a data-quality issue instead.
2. Otherwise, the state of the most severe finding (CRITICAL > WARNING). Ties go HIGH_THERMAL_LOAD > RAPID_TEMPERATURE_RISE > POTENTIAL_AIRFLOW_ISSUE > DATA_QUALITY_ISSUE.
3. Otherwise, `PARTIALLY_ASSESSED` if any thermal or airflow rule could not run (unset limit, too few readings), and `NORMAL` only if every rule ran and none fired. INFO findings never change the state.

**Checked against the simulator's ground truth.** The default run injected a `thermal_rise` on SIM-NA-013 from 13:37 to 14:18: a ramp over the first third, then a hold. Using trial limits (inlet 22 °C, sustained 0.08 and rapid 0.15 °C/min):

| as_of | Findings |
|---|---|
| 13:30 | none |
| 13:45 | sustained + rapid rise |
| 13:52 | above limit + sustained rise |
| 14:10 | above limit only (plateau; the rise rules correctly stop) |
| 14:30 | none (anomaly over) |

The estimator never reads the anomaly labels; they're used only for this check.

## III.4 API and CLI

```bash
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000
curl "http://127.0.0.1:8000/api/v1/health"
curl "http://127.0.0.1:8000/api/v1/telemetry/cooling-state"                                   # data center, newest run
curl "http://127.0.0.1:8000/api/v1/telemetry/cooling-state?asset_id=SIM-NA-013&window_minutes=30"
curl "http://127.0.0.1:8000/api/v1/telemetry/cooling-state?run_id=sim-c9944cc35156&asset_id=SIM-NA-013&as_of=2026-01-01T13:45:00Z"
```

Interactive docs are at `http://127.0.0.1:8000/docs`.

| Parameter | Validation |
|---|---|
| `asset_id`, `run_id` | `^[A-Za-z0-9._:-]{1,64}$` |
| `window_minutes` | 1–1440 |
| `as_of` | must include a timezone |

| Response | When |
|---|---|
| `200` | snapshot, including `INSUFFICIENT_DATA` for an empty store |
| `404` | `unknown_asset` / `unknown_run` |
| `422` | `validation_error` (bad or unknown parameter, `as_of` without a timezone) / `invalid_parameter` |
| `503` | `database_unavailable` / `store_not_initialized` |
| `500` | `internal_error` (unexpected; generic message only) |

An invalid `config/cooling_state.yaml` stops the API at startup instead of failing requests. Errors use the `ErrorResponse` shape from III.0. Handlers are sync, so database work runs in the threadpool; each request opens and closes its own connection. PostgreSQL connections time out after `API_DB_CONNECT_TIMEOUT_S` (3 s).

The CLI prints the same JSON and can override limits for one run without editing the config:

```bash
uv run python -m data_engineering.cooling.estimator --asset SIM-NA-013 --run sim-c9944cc35156 \
  --as-of 2026-01-01T13:45:00Z --set-limit inlet_temperature_high_c=27 \
  --set-limit sustained_inlet_rise_c_per_minute=0.08 --set-limit rapid_inlet_rise_c_per_minute=0.15
```

**Example response** for that command. It's real output, but trimmed: 3 of 11 metrics, 2 of 5 `rules_not_evaluated`, 1 of 3 notes. `DQ_PIPELINE_STALE` is present because no producer was running at the time.

```json
{
  "schema_version": "1.0", "evaluated_at": "2026-10-13T08:47:36.151090Z",
  "as_of": "2026-01-01T13:45:00Z", "window_minutes": 15, "window_start": "2026-01-01T13:30:00Z",
  "scope": {"level": "asset", "asset_id": "SIM-NA-013", "asset_type": "Compute",
            "simulation_run_id": "sim-c9944cc35156", "data_center_id": "dc-na-sim-001"},
  "state": "HIGH_THERMAL_LOAD", "severity": "WARNING", "workload_direction": "stable",
  "metrics": {
    "inlet_temperature_c": {"unit": "degC", "source": "telemetry", "available": true, "applicable": true,
      "reason": null, "observed_points": 15, "null_points": 0, "sufficient_points": true,
      "latest": 21.509, "latest_timestamp": "2026-01-01T13:45:00Z", "latest_age_seconds": 0.0,
      "mean": 20.3748, "minimum": 19.771, "maximum": 21.509, "std_dev": 0.592640666846277,
      "first": 19.771, "change": 1.7379999999999995, "change_elapsed_minutes": 14.0,
      "trend_per_minute": 0.13141785714285714, "recent_rate_per_minute": 0.1897000000000002,
      "direction": "increasing"},
    "gpu_utilization": {"unit": "percent", "applicable": false,
      "reason": "asset_type 'Compute' has no GPU; the telemetry value (0) is structural, not a measured utilization",
      "latest": null, "mean": null, "trend_per_minute": null, "direction": null, "...": "other fields null/0"},
    "cooling_efficiency": {"unit": "ratio", "available": false,
      "reason": "not in the telemetry schema v1.0: the Kaggle source has a static per-row cooling_efficiency, but the simulator does not produce it per minute",
      "latest": null, "...": "other fields null/0"}
  },
  "findings": [
    {"rule_id": "TH_SUSTAINED_INLET_RISE", "state": "HIGH_THERMAL_LOAD", "severity": "WARNING", "kind": "inference",
     "asset_id": "SIM-NA-013",
     "explanation": "Inlet temperature is rising 0.1314 deg C/min over the window (threshold 0.08 deg C/min).",
     "evidence": {"rate_c_per_minute": 0.1314, "threshold_c_per_minute": 0.08, "latest": 21.509,
                  "change_over_window": 1.738, "elapsed_minutes": 14.0}},
    {"rule_id": "TH_RAPID_INLET_RISE", "state": "RAPID_TEMPERATURE_RISE", "severity": "WARNING", "kind": "inference",
     "asset_id": "SIM-NA-013",
     "explanation": "Inlet temperature is rising 0.1897 deg C/min over the last 5 minutes (threshold 0.15 deg C/min).",
     "evidence": {"rate_c_per_minute": 0.1897, "threshold_c_per_minute": 0.15, "latest": 21.509,
                  "change_over_window": 1.738, "elapsed_minutes": 14.0}},
    {"rule_id": "DQ_PIPELINE_STALE", "state": "DATA_QUALITY_ISSUE", "severity": "WARNING", "kind": "data_quality",
     "asset_id": null,
     "explanation": "No telemetry was stored for 4295 s (wall clock); the stream may have stopped, so the snapshot may not reflect the current state.",
     "evidence": {"ingest_age_seconds": 4295.2, "max_ingest_age_seconds": 300}}
  ],
  "rules_not_evaluated": [
    {"rule_id": "TH_EXHAUST_ABOVE_LIMIT", "reason": "limits.exhaust_temperature_high_c is not configured", "asset_id": "SIM-NA-013"},
    {"rule_id": "TH_DELTA_ABOVE_LIMIT", "reason": "limits.temperature_delta_high_c is not configured", "asset_id": "SIM-NA-013"}
  ],
  "data_quality": {"status": "DEGRADED", "stale": true, "ingest_age_seconds": 4295.2, "expected_points": 15,
    "present_points": 15, "observed_points": 15, "missing_readings": 0, "duplicate_timestamps": 0,
    "conflicting_duplicates": 0, "irregular_intervals": 0, "median_interval_seconds": 60.0, "coverage": 1.0,
    "gaps": [], "missing_fields": [], "unavailable_fields": ["cooling_efficiency"],
    "warnings": ["DQ_PIPELINE_STALE: No telemetry was stored for 4295 s (wall clock); the stream may have stopped, so the snapshot may not reflect the current state."]},
  "assets": [], "source_type": "simulated",
  "notes": ["All telemetry is simulated (source_type=simulated) from an engineered synthetic Kaggle dataset; this is not real operational telemetry."]
}
```

The response schema (`CoolingStateSnapshot`, `src/data_engineering/cooling/models.py`) is validated by Pydantic, and FastAPI publishes it at `/openapi.json`.

## III.5 Verify against the live stream

```bash
docker compose up -d --wait kafka
uv run python -m data_engineering.streaming.consumer                                                   # terminal 1
uv run python -m data_engineering.streaming.producer simulate --assets 5 --seed 21 --continuous --speedup 60   # terminal 2
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000                                 # terminal 3
curl "http://127.0.0.1:8000/api/v1/telemetry/cooling-state"        # after ~20 s, so the 15-minute window is full
```

Measured in this session:

- **While streaming:** the newest run is picked automatically, `ingest_age_seconds` ≈ 1.2, coverage 1.0, state `PARTIALLY_ASSESSED` (limits unset), 30–45 ms per request.
- **After stopping the producer** (with a demo `max_ingest_age_seconds: 10`): `DQ_PIPELINE_STALE` and `DATA_QUALITY_ISSUE`.

To see thermal rules fire, set limits in `config/cooling_state.yaml`, or use `--set-limit` with the CLI.

## III.6 Tests

`uv run pytest tests/test_cooling_metrics.py tests/test_cooling_estimator.py tests/test_cooling_api.py`: 64 tests on controlled fixtures written through the real store. They cover:

- **Normal and rules:** stable/NORMAL, unset limits → PARTIALLY_ASSESSED, rapid and sustained rise with precedence, limit and critical tier, exhaust/delta limits.
- **Workload and airflow:** increasing/decreasing workload, GPU not applicable, airflow drop with rising temperature (and airflow drop alone not flagged), fan/airflow divergence.
- **Data quality:** missing CPU, unavailable cooling efficiency, explicit missing readings, stale asset, stopped pipeline, gaps, duplicate timestamps (identical and conflicting), low coverage, too few points.
- **Scope:** empty store, an empty window, unknown asset/run, data-center aggregation without mixing assets, one silent asset, separate runs, the window parameter.
- **Configuration:** 11 invalid-config cases, missing config file.
- **API:** parameters, 404 / 422 / 500 / 503, a real unreachable PostgreSQL URL (no credential leak), a query failing mid-request, health.

## III.7 Limitations

- **Every limit is unset by design.** Until you configure them, the estimator reports `PARTIALLY_ASSESSED`, never `NORMAL`. The default trend sensitivities (`trend_min_change`) are analysis heuristics, not limits.
- **Missing indicators.** Cooling efficiency, power-supply efficiency, WUE, cooling power and rack- or room-level sensors don't exist in the telemetry. There is no asset → rack → room topology beyond `rack_u_position`, so "data center" means "all assets of the run".
- **Trends are straight-line fits.** A step change inside the window (e.g. a power spike that ended) shows up as a trend; compare `recent_rate_per_minute` and `std_dev`. Observed on SIM-NA-003: an injected power spike ending at 00:11 produced `power … direction=decreasing` while power had in fact been flat since.
- **No hysteresis or memory.** Each request is evaluated independently, so the state can flip on consecutive requests near a threshold.
- **Time semantics.** Event time is simulated. `DQ_PIPELINE_STALE` uses wall-clock ingest time; `DQ_STALE_ASSET` uses event time.
- **Workload.** `workload_direction` combines directions, not magnitudes.
- **The API is unauthenticated.** It's local development only (bind to 127.0.0.1).

---

## Appendix: Phase 1 (dataset → SQLite → simulation)

**Dataset findings** (details: [`docs/dataset/README.md`](docs/dataset/README.md)):
- `green_ai_datacenter.csv` is 100,000 rows × 35 columns with 0 empty cells.
- There are no timestamps and no equipment IDs.
- `failure_risk_level` and `energy_efficiency_class` labels are present.
- Regions: EU 37,972 · **NA 28,801** · APAC 27,133 · ME 6,094.
- **Pitfall:** the North America code is the literal string `NA`, which pandas' default `read_csv` turns into a null. Project code reads with the `csv` module.

**Kaggle download.** The public dataset downloads anonymously with Kaggle CLI 2.x (`uv run python -m data_engineering.ingestion.download_dataset`). If Kaggle asks for authentication, run `uv run kaggle auth login`, or set `KAGGLE_API_TOKEN`, or place a legacy `kaggle.json` in `~/.kaggle/`. Never commit tokens.

**Step by step** (`uv run python -m data_engineering.pipeline` runs all of these, then validation):

```bash
uv run python db/init_db.py                                      # SQLite schema (idempotent)
uv run python -m data_engineering.ingestion.load_source          # CSV -> source_records (idempotent)
uv run python -m data_engineering.processing.filter_region       # NA subset -> table + data/processed CSV
uv run python -m data_engineering.simulation.generate_telemetry  # store a synthetic 24 h run in SQLite
uv run python -m data_engineering.validation.validate_dataset    # 20 checks -> data/processed/validation_report.md
```

**SQLite tables** (`db/schema.sql`):

| Table | Contents |
|---|---|
| `source_records` | every original row, all 35 columns, `provenance='dataset'` |
| `region_filtered_records` (view `filtered_source_records`) | the NA subset |
| `simulation_runs`, `asset_metadata` | runs and their synthetic assets |
| `telemetry_readings` | the full 1,440-slot grid per asset; missing = `is_missing=1` + NULL metrics |
| `anomaly_events`, `anomaly_labels` | injected anomalies, keeping both clean and injected values |

Simulation parameters and assumptions are documented in `config/simulation.yaml`.

**Where things are stored**

| Path | Content | In git? |
|---|---|---|
| `data/raw/hyperscale-data-center-dataset/` | original Kaggle files + `MANIFEST.json` | no |
| `data/datacenter_ops.db` | SQLite (override with `DATACENTER_OPS_DB_PATH`) | no |
| `data/processed/` | NA subset CSV, validation report | no |
| `data/simulation/<run_id>/` | stored-run CSV exports | no |
| `data/telemetry_store.db` | consumer's telemetry store (SQLite, WAL) | no |
| `data/telemetry/` | Parquet history (`region_code=…/event_date=…/part-….parquet`) | no |
| `logs/phase1.jsonl` | structured JSON logs (all components: producer, consumer, exporter) | no |
| Kafka topic data | inside the `dco-kafka` container | n/a |
