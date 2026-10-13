# Idempotent UPSERT Patterns

Preserved from the QuantBridge `storage-supabase` skill and `patterns/pipeline-patterns.md` (Pattern 4). Use these when idempotent loading into a SQL table is needed.

## DuckDB (current)

```sql
BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS analytics.anomalies (
    anomaly_id   VARCHAR PRIMARY KEY,
    -- ...typed columns...
);

CREATE TEMP TABLE staging_anomalies AS
SELECT * FROM read_parquet('<batch>.parquet')
QUALIFY row_number() OVER (PARTITION BY anomaly_id ORDER BY detected_at) = 1;   -- dedup staging

INSERT INTO analytics.anomalies
SELECT * FROM staging_anomalies
ON CONFLICT (anomaly_id) DO UPDATE SET
    score = EXCLUDED.score,
    detected_at = EXCLUDED.detected_at;

DROP TABLE staging_anomalies;
COMMIT;   -- on any error: ROLLBACK
```

## PostgreSQL / Supabase (future serving layer only)

```sql
BEGIN;
CREATE TABLE IF NOT EXISTS <target> (...);
CREATE TEMP TABLE staging_<t> (LIKE <target>);
COPY staging_<t> FROM STDIN;              -- or driver bulk insert

DELETE FROM staging_<t>
WHERE ctid NOT IN (SELECT MIN(ctid) FROM staging_<t> GROUP BY <pk_cols>);

INSERT INTO <target> SELECT * FROM staging_<t>
ON CONFLICT (<pk_cols>) DO UPDATE SET <non_pk_col> = EXCLUDED.<non_pk_col>, ...;

DROP TABLE staging_<t>;
COMMIT;
```

Guard rails (unchanged from the source):
- The whole load is one transaction. Any error → `ROLLBACK`. No partial writes.
- Dedup staging on the primary key before merging.
- Never retry a failed load inside the same transaction. Roll back, then fail or rerun.
- Credentials come from environment variables. Never log them.
