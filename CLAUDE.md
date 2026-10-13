# Data Center Operations Intelligence Platform

A platform that ingests hyperscale data center operational data, streams it through Apache Kafka, validates and stores it, and detects anomalous operational behavior. A later phase adds AI agents that investigate incidents and produce evidence-based recommendations.

**Status:** implemented and tested:
- Phase 1: dataset → SQLite → synthetic 24 h / 1-minute telemetry.
- The **Kafka producer**: topic `datacenter.telemetry`.
- The **Kafka consumer** and storage layer: SQLite/PostgreSQL telemetry store, dead-letter topic `datacenter.telemetry.dlq`, Parquet history, DuckDB analytics.
- The **Cooling State Estimator** (`src/data_engineering/cooling/`): deterministic, read-only, rules-based. Served at `GET /api/v1/telemetry/cooling-state` by the `app` package (FastAPI).

**Not built:** other API endpoints, SSE, dashboard, diagnostic agents, anomaly detector, cooling optimization or control, model training. Don't start those until the user asks. README.md is the detailed guide (Part II covers the consumer).

## Data source

- Kaggle: `deeplumiere/hyperscale-data-center-dataset` (https://www.kaggle.com/datasets/deeplumiere/hyperscale-data-center-dataset)
- Inspected. Read `docs/dataset/README.md` before using any column. Key facts:
  - One CSV, 100,000 × 35. It describes itself as an *engineered synthetic dataset*.
  - **No timestamps and no equipment IDs.** Rows are independent records, not a time series. `time_of_day` is a 3-value bucket.
  - Labels `failure_risk_level` and `energy_efficiency_class` exist. Units are inferred from column names only.
  - Region `NA` (North America, inferred) = 28,801 rows. **Never let tooling convert the string `NA` to null.**
- Scenario: one hypothetical North American data center. All telemetry is synthetic, seeded from NA source rows.

## Architecture (initial)

1. Dataset ingestion + schema profiling (`dataset-ingestion`)
2. Python **replay producer** (if the data is time-series) or **telemetry simulator** (if not), chosen after profiling
3. **Apache Kafka** (local single-broker KRaft via root `docker-compose.yml`, `apache/kafka:3.9.1`): topic `datacenter.telemetry`, 3 partitions, key = `asset_id`, JSON events per `src/data_engineering/streaming/telemetry_event.v1.schema.json` (v1.0). Producer: `src/data_engineering/streaming/producer.py`
4. **Consumer** `src/data_engineering/streaming/consumer.py`, group `KAFKA_GROUP_ID` (default `dco-telemetry-sink`):
   - validate against the same schema, rejecting bad messages to `datacenter.telemetry.dlq`;
   - one DB transaction per batch;
   - commit offsets only after the DB commit and DLQ confirmation, and only the contiguous handled prefix per partition.
5. Storage:
   - Phase 1 data: `data/datacenter_ops.db`, `db/schema.sql`.
   - Consumer store: a **separate** database, `data/telemetry_store.db` or PostgreSQL. Schemas `db/telemetry_store_{sqlite,postgres}.sql`, initialized by `db/init_telemetry_store.py`. Tables `assets`, `telemetry_readings`, `anomaly_records`, `ingestion_batches`, `parquet_exports`.
   - Parquet history in `data/telemetry/region_code=…/event_date=…/`, exported *from the DB* through the ledger, with deterministic file names.
   - DuckDB reads Parquet in place (`src/data_engineering/analytics/`).
6. Analytics (implemented: DuckDB queries) and incident detection (not built; `anomaly_records` holds only simulator-injected labels)
7. *(Future, not built)* AI-agent investigation and recommendations

Kafka transports and retains events. It is not the transformation layer or the system of record. The telemetry store is the system of record. Delivery is at-least-once; storage is idempotent on `event_id`, never exactly-once.

Stack now: Python 3.13, **uv** (package manager, `pyproject.toml` + `uv.lock`; `requirements.txt` is exported from the lock), SQLite (stdlib), PyYAML, kaggle CLI, **confluent-kafka**, jsonschema, python-dotenv, **pyarrow**, **duckdb**, psycopg 3 (optional extra `postgres`; also in dev), pytest (dev: numpy, for normalization tests only). Project code avoids pandas/numpy. Further libraries need approval before installing. Add infrastructure only with a stated reason.

Kafka gotchas already learned:
- Host clients use `127.0.0.1:9092`, never `localhost`: on Windows, `localhost` resolves to `::1` first and each refused attempt costs ~2 s (it caused metadata timeouts).
- Leave the Kafka record timestamp as publish time. Event time lives in the payload, and retention would delete records stamped with past simulated dates.
- Git Bash needs `MSYS_NO_PATHCONV=1` for `docker compose exec kafka /opt/kafka/bin/...`.
- Handle SIGINT, SIGTERM and SIGBREAK for graceful stop on Windows.

Consumer and storage gotchas:
- `consumer.close()` must run on every exit path. A skipped close keeps the dead member in the group for the session timeout (45 s), so a restart gets no partitions.
- Simulated asset IDs repeat across runs: key assets by `(simulation_run_id, asset_id)` and filter analytics by run.
- SQLite timestamps are fixed-width `YYYY-MM-DDTHH:MM:SS.ffffffZ` (mixed precision sorts wrongly).
- DuckDB `TIMESTAMPTZ` results need pytz; query with `AT TIME ZONE 'UTC'` instead.
- A newly created Kafka topic can briefly be missing from metadata; `topic_setup` polls for it.
- In this harness, shell heredocs containing triple quotes fail to parse. Write multi-line Python patches to a file first.

API structure (`src/app/`, package `app`; the pipeline stays in `data_engineering`):
- `main.py` is the only entry point: create_app() plus a lifespan that builds the cooling config, the StoreFactory and the threadpool limiter.
- `config.py` holds AppSettings (`API_` prefix); database settings stay `TELEMETRY_*`, shared with the consumer.
- `exceptions.py`: raise AppError subclasses; every error renders as `{"error": {code, message, details}, "request_id"}`.
- `database.py` wraps the existing store (no ORM). One connection per request; SQLite uses `check_same_thread=False` because FastAPI runs the sync dependency and the handler in different threads.
- A domain is `router.py`, `schemas.py`, `dependencies.py`, `service.py`, `exceptions.py`, `constants.py`; mount it in `api/v1/router.py`. Shared pieces live in `common/`.
- Add auth/rag/agents/llm/guardrails/... modules only when they are built. No placeholder stubs. Endpoints are currently unauthenticated.
- Handlers that touch the database are sync `def`, never `async def`.

Cooling estimator rules:
- Thresholds live in `config/cooling_state.yaml`. **All equipment limits ship unset (null)**: never invent them. An unset limit makes the rule "not evaluated" and the state `PARTIALLY_ASSESSED`, never `NORMAL`.
- Measurements that don't exist in the telemetry: cooling efficiency, power-supply efficiency, WUE, cooling power. Report them as unavailable.
- GPU utilization of non-GPU assets is a structural 0, so it's "not applicable", not a value.
- Scope every evaluation to one `simulation_run_id`. Never mix assets in one series.

## Skills (`.claude/skills/`)

| Skill | Use for |
|---|---|
| `pipeline-architecture` | Stage design, where code belongs, ELT vs streaming, stage gates |
| `dataset-ingestion` | Download, checksum, profile the dataset. Replay vs simulator decision |
| `kafka-streaming` | Topics, keys, envelope, producers, consumers, offsets, DLQ, idempotency, Docker Compose |
| `data-transformation` | Normalization, row → event mapping, curated SQL, derived metrics |
| `data-validation` | Contracts, per-event validation, batch DQ checks |
| `storage-design` | Telemetry store (SQLite/PostgreSQL), Parquet export ledger, DuckDB over Parquet |
| `anomaly-detection` | Detection methods, anomaly/incident records, evaluation, agent handoff |
| `pipeline-testing` | pytest unit/integration/replay tests |
| `observability-reliability` | JSON logging, metrics, lag, retries, run status |

## Rules

1. **Never invent dataset fields or signals.** Use only columns confirmed by `docs/dataset/profile.json` or `docs/dataset/README.md`. If a needed signal (equipment IDs, failure labels, maintenance events, specific units, temperatures, power, etc.) isn't in the dataset, say so and ask. Don't fabricate or "assume typical" values.
2. **Label provenance on every record:** `dataset` (batch-loaded actual), `replay` (actual value re-emitted via Kafka), or `synthetic` (generated, with generator + seed). Never present synthetic values or results as real.
3. **Event time comes from the data**, never `now()`. `ingestion_time` is separate. A synthetic or assumed-timezone event time is flagged in the envelope.
4. **Inspect before building:** show 5–10 sample rows/events for any new source or mapping and wait for approval.
5. **Ask before installing packages.** State the package, the reason, and alternatives. Use `uv add`, never ad-hoc `pip install`.
6. **Explain before implementing** major logic, schema or flow changes (show before/after for contracts and mappings).
7. **Tests are mandatory** (`pipeline-testing`). Don't claim something works because files exist or a script ran. Report what was actually verified.
8. **No silent failures:** structured JSON logs, explicit run status, DLQ for bad events, keep evidence after failures.
9. Never commit `.env`, credentials, `data/`, or `logs/`. Never log secret values.

## Repository conventions

- Layout: `src/data_engineering/{ingestion,processing,simulation,validation,streaming}`, `db/`, `config/simulation.yaml`, `tests/`, `docs/dataset/` (details in `pipeline-architecture`). Generated data lives under `data/` and is gitignored.
- Provenance in SQLite: `source_records.provenance = 'dataset'`; `telemetry_readings.provenance = 'synthetic'`; `asset_metadata.identifier_origin = 'synthetic'`. Missing readings are rows with `is_missing = 1` and NULL metrics. Injected anomalies are in `anomaly_events` and `anomaly_labels` (clean + injected value).
- All steps are idempotent: source rows keyed by `(source_file, source_row_number)`, simulation `run_id` = hash(config + source sha256).
- Topic names: `dc.<domain>.<stage>.v<major>`. Consumer groups: `dco-<role>`.
- The skills were migrated from an earlier QuantBridge (energy/EIA) project. Its paths and secrets (EIA, Supabase, DLT, GitHub Actions cron) do **not** apply here.

## Commands

```bash
uv sync                                                          # install
uv run python -m data_engineering.pipeline                       # full Phase 1 + validation
uv run python db/init_db.py                                      # schema only
uv run python -m data_engineering.ingestion.download_dataset     # Kaggle download (+ MANIFEST.json)
uv run python -m data_engineering.ingestion.load_source
uv run python -m data_engineering.processing.filter_region
uv run python -m data_engineering.simulation.generate_telemetry
uv run python -m data_engineering.validation.validate_dataset    # -> data/processed/validation_report.md
docker compose up -d --wait kafka                                # local broker on 127.0.0.1:9092
uv run python -m data_engineering.streaming.topic_setup          # create/verify datacenter.telemetry (idempotent)
uv run python -m data_engineering.streaming.producer simulate --assets 3 --duration-minutes 10
uv run python -m data_engineering.streaming.producer publish-stored
uv run python db/init_telemetry_store.py                         # consumer store (TELEMETRY_DB_BACKEND)
uv run python -m data_engineering.streaming.consumer --idle-timeout-s 10   # finite; omit for continuous
uv run python -m data_engineering.analytics.query_telemetry overview        # DuckDB over Parquet
docker compose --profile postgres up -d --wait postgres          # optional PostgreSQL (needs POSTGRES_PASSWORD)
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000          # API: /api/v1/telemetry/cooling-state, /docs
uv run python -m data_engineering.cooling.estimator --asset SIM-NA-001       # snapshot as JSON (CLI)
uv run pytest                                                    # unit tests, no broker needed
TELEMETRY_TEST_POSTGRES_URL=postgresql://... uv run pytest -m postgres   # store tests on PostgreSQL
uv run pytest -m integration                                     # needs the local broker
python -I .claude/skills/dataset-ingestion/scripts/profile_dataset.py data/raw/hyperscale-data-center-dataset --manifest --out docs/dataset/profile.json
```
