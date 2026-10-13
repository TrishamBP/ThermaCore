-- Operational telemetry store fed by the Kafka consumer (PostgreSQL dialect). Schema version 2.
-- Same tables, keys and constraints as db/telemetry_store_sqlite.sql (keep both in sync);
-- differences are only native types: TIMESTAMPTZ, DOUBLE PRECISION, BIGINT, JSONB.
-- Apply with: TELEMETRY_DB_BACKEND=postgres uv run python db/init_telemetry_store.py

CREATE TABLE IF NOT EXISTS schema_migrations (
    version      INTEGER PRIMARY KEY,
    description  TEXT NOT NULL,
    applied_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS parquet_exports (
    export_id     TEXT PRIMARY KEY,
    status        TEXT NOT NULL CHECK (status IN ('writing', 'done')),
    created_at    TIMESTAMPTZ NOT NULL,
    completed_at  TIMESTAMPTZ,
    batch_count   INTEGER NOT NULL CHECK (batch_count >= 0),
    row_count     INTEGER NOT NULL CHECK (row_count >= 0),
    files_json    JSONB,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_batches (
    batch_id               TEXT PRIMARY KEY,
    consumer_group         TEXT NOT NULL,
    source_topic           TEXT NOT NULL,
    started_at             TIMESTAMPTZ NOT NULL,
    persisted_at           TIMESTAMPTZ NOT NULL,
    committed_at           TIMESTAMPTZ,
    status                 TEXT NOT NULL CHECK (status IN ('persisted', 'committed', 'commit_failed')),
    messages_received      INTEGER NOT NULL,
    events_valid           INTEGER NOT NULL,
    rows_inserted          INTEGER NOT NULL,
    rows_deduplicated      INTEGER NOT NULL,
    rows_conflicting       INTEGER NOT NULL,
    anomaly_rows_inserted  INTEGER NOT NULL,
    events_rejected        INTEGER NOT NULL,
    dlq_published          INTEGER NOT NULL DEFAULT 0,
    offsets_json           JSONB NOT NULL,
    export_id              TEXT REFERENCES parquet_exports (export_id)
);
CREATE INDEX IF NOT EXISTS idx_batches_unexported ON ingestion_batches (export_id, persisted_at);

CREATE TABLE IF NOT EXISTS assets (
    simulation_run_id               TEXT NOT NULL,
    asset_id                        TEXT NOT NULL,
    data_center_id                  TEXT NOT NULL,
    region                          TEXT NOT NULL,
    region_code                     TEXT NOT NULL,
    asset_type                      TEXT,
    cooling_type                    TEXT,
    server_age_years                INTEGER,
    rack_u_position                 INTEGER,
    energy_source_primary           TEXT,
    source_failure_risk_level       TEXT,
    source_energy_efficiency_class  TEXT,
    source_maintenance_status       TEXT,
    source_type                     TEXT NOT NULL,
    baseline_source_row_number      INTEGER,
    dataset                         TEXT,
    source_file                     TEXT,
    source_sha256                   TEXT,
    generator                       TEXT,
    generator_version               TEXT,
    seed                            BIGINT,
    first_event_timestamp           TIMESTAMPTZ NOT NULL,
    last_event_timestamp            TIMESTAMPTZ NOT NULL,
    created_at                      TIMESTAMPTZ NOT NULL,
    updated_at                      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (simulation_run_id, asset_id)
);
CREATE INDEX IF NOT EXISTS idx_assets_asset ON assets (asset_id);

CREATE TABLE IF NOT EXISTS telemetry_readings (
    event_id                 TEXT PRIMARY KEY,
    simulation_run_id        TEXT NOT NULL,
    asset_id                 TEXT NOT NULL,
    data_center_id           TEXT NOT NULL,
    region                   TEXT NOT NULL,
    region_code              TEXT NOT NULL,
    event_timestamp          TIMESTAMPTZ NOT NULL,
    event_timestamp_raw      TEXT NOT NULL,
    slot_index               INTEGER NOT NULL,
    reading_status           TEXT NOT NULL CHECK (reading_status IN ('observed', 'missing')),
    schema_version           TEXT NOT NULL,
    source_type              TEXT NOT NULL,
    timestamp_source         TEXT NOT NULL,
    published_from           TEXT NOT NULL,
    workload_intensity       DOUBLE PRECISION,
    cpu_utilization          DOUBLE PRECISION,
    gpu_utilization          DOUBLE PRECISION,
    memory_utilization       DOUBLE PRECISION,
    storage_utilization      DOUBLE PRECISION,
    disk_io_iops             DOUBLE PRECISION,
    network_throughput_gbps  DOUBLE PRECISION,
    network_latency_ms       DOUBLE PRECISION,
    ambient_temperature_c    DOUBLE PRECISION,
    humidity_percentage      DOUBLE PRECISION,
    inlet_temperature_c      DOUBLE PRECISION,
    exhaust_temperature_c    DOUBLE PRECISION,
    fan_speed_rpm            DOUBLE PRECISION,
    airflow_cfm              DOUBLE PRECISION,
    power_consumption_kw     DOUBLE PRECISION,
    pue                      DOUBLE PRECISION,
    payload_sha256           TEXT NOT NULL,
    kafka_topic              TEXT NOT NULL,
    kafka_partition          INTEGER NOT NULL,
    kafka_offset             BIGINT NOT NULL,
    ingested_at              TIMESTAMPTZ NOT NULL,
    ingest_batch_id          TEXT NOT NULL REFERENCES ingestion_batches (batch_id),
    CHECK (reading_status = 'observed' OR coalesce(
        workload_intensity, cpu_utilization, gpu_utilization, memory_utilization, storage_utilization,
        disk_io_iops, network_throughput_gbps, network_latency_ms, ambient_temperature_c,
        humidity_percentage, inlet_temperature_c, exhaust_temperature_c, fan_speed_rpm, airflow_cfm,
        power_consumption_kw, pue) IS NULL),
    FOREIGN KEY (simulation_run_id, asset_id) REFERENCES assets (simulation_run_id, asset_id)
);
CREATE INDEX IF NOT EXISTS idx_readings_asset_time ON telemetry_readings (asset_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_readings_time ON telemetry_readings (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_readings_batch ON telemetry_readings (ingest_batch_id);

CREATE TABLE IF NOT EXISTS anomaly_records (
    anomaly_id             TEXT NOT NULL,
    event_id               TEXT NOT NULL REFERENCES telemetry_readings (event_id),
    simulation_run_id      TEXT NOT NULL,
    asset_id               TEXT NOT NULL,
    event_timestamp        TIMESTAMPTZ NOT NULL,
    anomaly_type           TEXT NOT NULL,
    label_source           TEXT NOT NULL CHECK (label_source = 'simulator_injected'),
    affected_metrics_json  JSONB NOT NULL,
    ingest_batch_id        TEXT NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (anomaly_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_anomaly_asset_time ON anomaly_records (asset_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_anomaly_batch ON anomaly_records (ingest_batch_id);

-- ---- Schema version 2: recommendation analyses (recommendation-only; nothing here controls equipment) ----
-- One row per analysis run of the LangGraph workflow (status, models, latency, token usage, node trace).
CREATE TABLE IF NOT EXISTS recommendation_analyses (
    analysis_id        TEXT PRIMARY KEY,
    simulation_run_id  TEXT NOT NULL,
    asset_id           TEXT NOT NULL,
    window_start       TIMESTAMPTZ,
    window_end         TIMESTAMPTZ,
    status             TEXT NOT NULL CHECK (status IN ('COMPLETED', 'COMPLETED_WITH_FALLBACK', 'FAILED')),
    trace_json         JSONB NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_analyses_asset ON recommendation_analyses (simulation_run_id, asset_id, created_at);

-- One row per recommendation (or inconclusive result). The UNIQUE key prevents duplicates for the
-- same asset, type and observation window.
CREATE TABLE IF NOT EXISTS recommendations (
    recommendation_id  TEXT PRIMARY KEY,
    analysis_id        TEXT NOT NULL REFERENCES recommendation_analyses (analysis_id),
    simulation_run_id  TEXT NOT NULL,
    asset_id           TEXT NOT NULL,
    recommendation_type TEXT NOT NULL,
    status             TEXT NOT NULL CHECK (status IN ('REQUIRES_REVIEW', 'INCONCLUSIVE')),
    severity           TEXT NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    window_start       TIMESTAMPTZ,
    window_end         TIMESTAMPTZ NOT NULL,
    payload_json       JSONB NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL,
    UNIQUE (simulation_run_id, asset_id, recommendation_type, window_end)
);
CREATE INDEX IF NOT EXISTS idx_recommendations_created ON recommendations (created_at);

-- Human review decisions. Recording a decision never executes anything.
CREATE TABLE IF NOT EXISTS recommendation_decisions (
    decision_id        TEXT PRIMARY KEY,
    recommendation_id  TEXT NOT NULL REFERENCES recommendations (recommendation_id),
    decision           TEXT NOT NULL CHECK (decision IN ('APPROVED_FOR_EVALUATION', 'REJECTED', 'DEFERRED')),
    reviewer           TEXT NOT NULL,
    note               TEXT,
    decided_at         TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_rec ON recommendation_decisions (recommendation_id, decided_at);

-- Alerts: WARNING/CRITICAL findings of the cooling state estimator captured when recommendations are
-- generated (one per asset, rule and observation window). Observations/inferences, never confirmed faults.
CREATE TABLE IF NOT EXISTS alerts (
    alert_id           TEXT PRIMARY KEY,
    analysis_id        TEXT NOT NULL REFERENCES recommendation_analyses (analysis_id),
    simulation_run_id  TEXT NOT NULL,
    asset_id           TEXT NOT NULL,
    rule_id            TEXT NOT NULL,
    severity           TEXT NOT NULL CHECK (severity IN ('WARNING', 'CRITICAL')),
    window_start       TIMESTAMPTZ,
    window_end         TIMESTAMPTZ NOT NULL,
    payload_json       JSONB NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL,
    UNIQUE (simulation_run_id, asset_id, rule_id, window_end)
);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts (created_at);
