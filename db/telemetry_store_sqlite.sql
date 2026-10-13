-- Operational telemetry store fed by the Kafka consumer (SQLite dialect). Schema version 1.
-- PostgreSQL equivalent: db/telemetry_store_postgres.sql (keep both in sync).
-- Apply with: uv run python db/init_telemetry_store.py
--
-- This is NOT the Phase 1 database (data/datacenter_ops.db). It lives in its own file
-- (default data/telemetry_store.db) because Phase 1 already has a different telemetry_readings table.
--
-- Timestamps are TEXT in one fixed-width UTC format, 'YYYY-MM-DDTHH:MM:SS.ffffffZ', so text
-- order = chronological order (mixed precision would not sort correctly: '...00.5Z' < '...00Z').

CREATE TABLE IF NOT EXISTS schema_migrations (
    version      INTEGER PRIMARY KEY,
    description  TEXT NOT NULL,
    applied_at   TEXT NOT NULL
);

-- Parquet export ledger: one row per export job (a group of persisted batches).
CREATE TABLE IF NOT EXISTS parquet_exports (
    export_id     TEXT PRIMARY KEY,
    status        TEXT NOT NULL CHECK (status IN ('writing', 'done')),
    created_at    TEXT NOT NULL,
    completed_at  TEXT,
    batch_count   INTEGER NOT NULL CHECK (batch_count >= 0),
    row_count     INTEGER NOT NULL CHECK (row_count >= 0),
    files_json    TEXT,                        -- JSON list of {path, rows}
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT
);

-- One row per consumer batch whose database transaction committed.
CREATE TABLE IF NOT EXISTS ingestion_batches (
    batch_id               TEXT PRIMARY KEY,
    consumer_group         TEXT NOT NULL,
    source_topic           TEXT NOT NULL,
    started_at             TEXT NOT NULL,
    persisted_at           TEXT NOT NULL,
    committed_at           TEXT,
    status                 TEXT NOT NULL CHECK (status IN ('persisted', 'committed', 'commit_failed')),
    messages_received      INTEGER NOT NULL,
    events_valid           INTEGER NOT NULL,
    rows_inserted          INTEGER NOT NULL,
    rows_deduplicated      INTEGER NOT NULL,
    rows_conflicting       INTEGER NOT NULL,
    anomaly_rows_inserted  INTEGER NOT NULL,
    events_rejected        INTEGER NOT NULL,
    dlq_published          INTEGER NOT NULL DEFAULT 0,
    offsets_json           TEXT NOT NULL,     -- {"<partition>": [first_offset, last_offset], ...}
    export_id              TEXT REFERENCES parquet_exports (export_id)
);
CREATE INDEX IF NOT EXISTS idx_batches_unexported ON ingestion_batches (export_id, persisted_at);

-- Asset inventory. Simulated asset IDs (SIM-NA-001, ...) are only unique within one
-- simulation run (another seed maps SIM-NA-001 to a different baseline row), so the key
-- is (simulation_run_id, asset_id).
CREATE TABLE IF NOT EXISTS assets (
    simulation_run_id               TEXT NOT NULL,
    asset_id                        TEXT NOT NULL,
    data_center_id                  TEXT NOT NULL,
    region                          TEXT NOT NULL,
    region_code                     TEXT NOT NULL,
    asset_type                      TEXT,      -- event asset_attributes.server_type
    cooling_type                    TEXT,
    server_age_years                INTEGER,
    rack_u_position                 INTEGER,
    energy_source_primary           TEXT,
    source_failure_risk_level       TEXT,      -- static labels of the baseline source row
    source_energy_efficiency_class  TEXT,
    source_maintenance_status       TEXT,
    source_type                     TEXT NOT NULL,
    baseline_source_row_number      INTEGER,
    dataset                         TEXT,
    source_file                     TEXT,
    source_sha256                   TEXT,
    generator                       TEXT,
    generator_version               TEXT,
    seed                            INTEGER,
    first_event_timestamp           TEXT NOT NULL,
    last_event_timestamp            TEXT NOT NULL,
    created_at                      TEXT NOT NULL,
    updated_at                      TEXT NOT NULL,
    PRIMARY KEY (simulation_run_id, asset_id)
);
CREATE INDEX IF NOT EXISTS idx_assets_asset ON assets (asset_id);

-- One row per telemetry event (per asset per slot). event_id is the deduplication key.
CREATE TABLE IF NOT EXISTS telemetry_readings (
    event_id                 TEXT PRIMARY KEY,
    simulation_run_id        TEXT NOT NULL,
    asset_id                 TEXT NOT NULL,
    data_center_id           TEXT NOT NULL,
    region                   TEXT NOT NULL,
    region_code              TEXT NOT NULL,
    event_timestamp          TEXT NOT NULL,    -- normalized UTC
    event_timestamp_raw      TEXT NOT NULL,    -- exactly as received
    slot_index               INTEGER NOT NULL,
    reading_status           TEXT NOT NULL CHECK (reading_status IN ('observed', 'missing')),
    schema_version           TEXT NOT NULL,
    source_type              TEXT NOT NULL,
    timestamp_source         TEXT NOT NULL,
    published_from           TEXT NOT NULL,
    workload_intensity       REAL,
    cpu_utilization          REAL,
    gpu_utilization          REAL,
    memory_utilization       REAL,
    storage_utilization      REAL,
    disk_io_iops             REAL,
    network_throughput_gbps  REAL,
    network_latency_ms       REAL,
    ambient_temperature_c    REAL,
    humidity_percentage      REAL,
    inlet_temperature_c      REAL,
    exhaust_temperature_c    REAL,
    fan_speed_rpm            REAL,
    airflow_cfm              REAL,
    power_consumption_kw     REAL,
    pue                      REAL,
    payload_sha256           TEXT NOT NULL,    -- detects conflicting duplicates
    kafka_topic              TEXT NOT NULL,
    kafka_partition          INTEGER NOT NULL,
    kafka_offset             INTEGER NOT NULL,
    ingested_at              TEXT NOT NULL,
    ingest_batch_id          TEXT NOT NULL REFERENCES ingestion_batches (batch_id),
    -- Missing readings keep NULL metrics (never 0); observed readings have every metric.
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

-- Explicit anomaly labels carried by events (labels.injected_anomalies): one row per
-- (anomaly, event). Only the simulator's deliberate injections exist; no detector, no severity.
CREATE TABLE IF NOT EXISTS anomaly_records (
    anomaly_id             TEXT NOT NULL,
    event_id               TEXT NOT NULL REFERENCES telemetry_readings (event_id),
    simulation_run_id      TEXT NOT NULL,
    asset_id               TEXT NOT NULL,
    event_timestamp        TEXT NOT NULL,
    anomaly_type           TEXT NOT NULL,
    label_source           TEXT NOT NULL CHECK (label_source = 'simulator_injected'),
    affected_metrics_json  TEXT NOT NULL,
    ingest_batch_id        TEXT NOT NULL,
    created_at             TEXT NOT NULL,
    PRIMARY KEY (anomaly_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_anomaly_asset_time ON anomaly_records (asset_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_anomaly_batch ON anomaly_records (ingest_batch_id);
