---
name: kafka-streaming
description: Apache Kafka event streaming for the data center operations platform — local KRaft broker via Docker Compose, topic/partition/key design, the event envelope (event_id, provenance, event_time vs ingestion_time), Python replay/simulator producers, consumer groups and manual offset commits, retries and dead-letter topics, idempotency and duplicate handling, replay, and Kafka observability. Use whenever writing or reviewing any producer, consumer, topic, Kafka config, docker-compose service, or event schema.
---

# Kafka Streaming

Kafka is the event-streaming layer between the producer (replay or simulator) and every downstream consumer. It is a **durable, replayable log**. It is not the system of record (the raw store is) and it does no transformation (DuckDB does). See `pipeline-architecture` for how this fits ELT.

This skill is **new**. The QuantBridge source had no streaming. It reuses the source's retry rules ("retry only transient errors, never retry bugs") and its idempotent-upsert discipline.

## Local setup

- Single broker, **KRaft mode** (no ZooKeeper), official `apache/kafka` image. Template: `references/docker-compose.kafka.yml`. Copy it to the repo root as `docker-compose.yml` when implementation starts.
- Host clients use `localhost:9092`. Containers on the compose network use `kafka:19092`.
- Auto-create topics is **off**. Create topics explicitly, so partition counts are deliberate:
  ```bash
  docker compose up -d kafka
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic dc.telemetry.raw.v1 --partitions 3 --replication-factor 1
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
  ```
- Not added by default: Schema Registry, Kafka Connect, ksqlDB, a UI. Propose one only for a concrete reason (e.g. Schema Registry once several producers or teams evolve schemas independently; a UI if CLI inspection becomes a bottleneck).

## Python client

Recommended client: **`confluent-kafka`** (librdkafka-based, maintained, supports idempotent producers and manual commits). It is a new dependency, so ask before `uv add confluent-kafka`. Use JSON serialization at first. Validate with the contract models from `data-validation`.

## Implemented producer (read this first)

- Topic **`datacenter.telemetry`** (name chosen by the user; replaces the proposed `dc.telemetry.raw.v1`), 3 partitions, RF 1, 7-day retention, created by `uv run python -m data_engineering.streaming.topic_setup`.
- Producer: `src/data_engineering/streaming/producer.py` (`simulate` / `publish-stored` modes). Key = `asset_id`. Settings: `streaming/settings.py` + `.env.example`.
- Event schema v1.0 (authoritative, supersedes `references/event-envelope.md` where they differ): `src/data_engineering/streaming/telemetry_event.v1.schema.json`. `event_id` = `<run_id>:<asset_id>:<slot_index>`.
- Local broker: root `docker-compose.yml`, `apache/kafka:3.9.1`. Host clients use **`127.0.0.1:9092`**, not `localhost`: on Windows `localhost` resolves to `::1` first and each refused attempt costs ~2 s.
- **Consumer implemented:** `src/data_engineering/streaming/consumer.py`. Group `dco-telemetry-sink`, DLQ `datacenter.telemetry.dlq`. Auto-commit off; commit after the DB transaction and DLQ confirmation; `OffsetTracker` never commits past an unhandled offset. Bounded backoff with jitter, idempotent store on `event_id`. README Part II documents it. The separate raw-sink / validator / anomaly groups listed below are **not** built: one consumer does validation and storage, and Parquet is exported from the database.

## Topics (proposed for later stages)

| Topic | Producer | Content | Key |
|---|---|---|---|
| `dc.telemetry.raw.v1` | replay producer / simulator | Every observation, unchanged envelope | entity key (see below) |
| `dc.telemetry.validated.v1` | validator consumer | Events that passed the contract | same key |
| `dc.telemetry.dlq.v1` | any consumer | Events that failed validation or processing | same key |
| `dc.anomalies.v1` | anomaly consumer | Anomaly records with evidence | entity key |

Naming: `dc.<domain>.<stage>.v<major>`. Bump the major version on breaking schema changes instead of mutating the topic's meaning.

**Partitioning and keys:**
- The key decides ordering. Kafka orders events only within a partition, so key by the entity whose time order matters for detection (e.g. a device/rack/facility ID). **Use only an identifier column that the profile confirms exists.** If none exists, key by a stable source grouping (e.g. source file) and document that per-entity ordering is not available.
- Start with 3 partitions locally. Partitions cap consumer parallelism within a group. Adding partitions later remaps keys, so choose before data lands.

## Event envelope

Every event uses the envelope in `references/event-envelope.md`. Non-negotiables:

- `event_id` is **deterministic**: a hash of a stable source identity (`dataset/file/row_number`, or `generator/seed/sequence`). Re-running a replay produces identical IDs, which is what makes dedup possible.
- `provenance` ∈ {`dataset`, `replay`, `synthetic`}. Synthetic events also carry generator name, version and seed.
- **`event_time`** = when the observation happened, taken from the dataset's timestamp column. **`ingestion_time`** = when the producer sent it. For replay, never set `event_time` to `now()`. If the dataset has no timestamp, `event_time` is synthetic and the envelope must say so (`event_time_source: "synthetic"`).
- `schema_version` on every event.

## Producers

Replay producer:
1. Read source rows in event-time order (per the profile). Map them via `data-transformation` → envelope.
2. Pace by event-time deltas divided by `--speedup`, or `--speedup 0` for as fast as possible. Pacing affects only *when* events are sent, never their `event_time`.
3. Producer config: `enable.idempotence=true`, `acks=all`, a `linger.ms` of a few ms, `compression.type=lz4` or none. Always pass `key`.
4. Use a delivery callback. Count delivered/failed per partition, then `flush()` before exit. A failed delivery after the client's own retries means the run fails.
5. Support `--sample-rows N` and `--run-id`. Log the run start and summary (see `observability-reliability`).

Simulator, only when the decision in `dataset-ingestion` says so:
- Seeded RNG. Parameters derived from profiled distributions or user-provided, and documented in code. Never present simulated values as real.
- Every event `provenance: "synthetic"`, plus `generator: {name, version, seed}`.
- Injected anomalies (for testing detection) are labeled in the event (`synthetic_label`), so detection can be evaluated honestly.

## Consumers

- **One consumer group per responsibility**: `dco-raw-sink`, `dco-validator`, `dco-anomaly`. Each tracks its own offsets and can be reset and replayed independently.
- `enable.auto.commit=false`. **Commit after the side effect is durable** (file written / row inserted / downstream produce acknowledged). This is at-least-once delivery. Duplicates are expected and handled by idempotency below.
- `auto.offset.reset=earliest`, so a new group reads full history.
- Keep processing logic in pure functions (`process(event) -> result`) separate from the poll loop, so it can be unit-tested without Kafka.
- Handle rebalances: commit processed offsets in `on_revoke`. Per-key in-memory state (anomaly windows) must be rebuildable from replay.
- Graceful shutdown on SIGINT/SIGTERM: stop polling, finish the batch, commit, `close()`.

## Retries and dead-letter topic

| Error | Handling |
|---|---|
| Transient (broker unavailable, timeouts, sink file lock) | Retry in-process with bounded exponential backoff, without committing. If exhausted, stop the consumer (non-zero exit) rather than skip |
| Contract/validation failure | **No retry.** Produce to `dc.telemetry.dlq.v1` with headers `error_type`, `error_message`, `failed_stage`, `source_topic`, `source_partition`, `source_offset`, then commit |
| Deserialization failure (not JSON) | DLQ with raw bytes preserved, then commit |
| Bug (unexpected exception in processing) | Do not commit. Log with traceback and exit non-zero. Fix the code and replay |

Do not add retry topics (`.retry.5s`, etc.) until a real need for delayed retry appears.

## Idempotency and duplicates

Duplicates come from producer retries, consumer re-delivery after a crash, and deliberate replays. Handle them at the **sink**, keyed on `event_id`:
- Raw store: name files by `topic-partition-firstOffset-lastOffset`, so reprocessing rewrites the same file (see `storage-design`).
- DuckDB tables: `event_id` primary key. Use `INSERT ... ON CONFLICT DO NOTHING`, or rebuild a partition idempotently.
- Anomaly output: `anomaly_id` = hash(method, entity, metric, window_start, window_end), so re-detection produces the same IDs.

The idempotent producer only prevents duplicates from producer retries within one session. It does not dedupe replays. Exactly-once transactions are not needed initially.

## Event time vs ingestion time

- Windowing, baselines and anomaly timestamps use **event_time**.
- Latency and freshness metrics use **ingestion_time** and consumer processing time.
- Out-of-order and late events: track a per-partition watermark (max event_time seen minus allowed lateness). Late events beyond it are still stored raw, but they are counted and logged, never silently dropped.

## Replay

- Full reprocess: create a new consumer group, or reset offsets with `kafka-consumer-groups.sh --reset-offsets --to-earliest --group <g> --topic <t> --execute`, with consumers stopped.
- Or re-run the producer with the same inputs. Deterministic `event_id`s make the result identical.
- Retention: set topic `retention.ms` deliberately. Kafka is not long-term storage. The raw store is the source for long-range reprocessing.

## Observability

- Consumer lag: `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <g>`.
- Log counts produced, consumed, DLQ'd and committed per partition, plus end-to-end latency (processing time − ingestion_time). Details in `observability-reliability`.

## Failure modes

| Failure | Cause | Response |
|---|---|---|
| `NoBrokersAvailable` / connection refused | Broker down, or wrong listener (`kafka:19092` vs `localhost:9092`) | Check `docker compose ps` and the advertised listeners |
| Consumer sees nothing | Group already committed to the end, or wrong topic | `--describe` the group. Reset offsets or use a new group |
| Ordering anomalies | Key not stable, or partitions added | Fix the key. Recreate the topic and replay |
| DLQ growing | Contract mismatch | Inspect DLQ headers. Fix the contract or mapping, then replay from DLQ or source |
