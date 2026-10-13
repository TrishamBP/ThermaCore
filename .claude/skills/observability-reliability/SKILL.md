---
name: observability-reliability
description: Structured JSON logging, run summaries, metrics (throughput, DLQ rate, consumer lag, end-to-end latency, event-time skew), failure handling and retry policy, and graceful shutdown for producers, consumers, and batch jobs. Use when adding logging to any pipeline module, handling errors or retries, checking pipeline health or lag, or deciding whether a run succeeded.
---

# Observability and Reliability

This merges the QuantBridge `logging` skill and `patterns/failure-handling.md`. Kept from the source: structured JSON logs, mirrored to console, no silent failures, retry only transient errors, keep evidence. Added: streaming metrics and lag.

## Structured logging

- Python `logging` (stdlib) with a JSON formatter. File handler + console handler. One JSONL file per component: `logs/<component>.jsonl` (gitignored).
- Entry shape:

```json
{
  "timestamp": "2026-10-13T09:15:02.123Z",
  "level": "INFO",
  "component": "replay_producer | raw_sink | validator | anomaly_detector | transform",
  "run_id": "replay-20261013-0915",
  "message": "batch committed",
  "context": { "topic": "dc.telemetry.raw.v1", "partition": 1, "offsets": [1200, 1699], "count": 500 },
  "error_type": null,
  "traceback": null
}
```

- Never log credentials or full payloads at INFO. Use DEBUG for sampled payloads.
- If the log file can't be opened, fall back to console-only and say so on the console.

## Minimum log points

| Component | Must log |
|---|---|
| Producer | start (flags, run_id, source file + sha256), progress every N events, delivery failures, summary (produced per partition, failed, duration) |
| Consumer | start (group, topics), partition assignment/revocation, each commit (partition, offsets, count), DLQ sends (reason), retries, shutdown summary |
| Batch job | start, each validation check result, rows written per table, completion status |
| Any failure | exception class, message, traceback, stage, and topic/partition/offset or table |

## Metrics (log-derived at first, no metrics stack yet)

- Throughput: events/sec produced and consumed.
- **Consumer lag** per group: `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <group>`.
- DLQ rate: DLQ count / consumed count.
- End-to-end latency: consumer processing time − `ingestion_time`.
- Event-time skew: processing time − `event_time`. This is only meaningful for live data. For replay it reflects the replay offset, so report it but don't alert on it.
- Late-event count (beyond the watermark).

Add Prometheus/Grafana or similar only when log-derived metrics are insufficient (e.g. a long-running demo needs live dashboards).

## Run status

Every run ends with an explicit status line: `SUCCESS`, `PARTIAL` (skipped stage, DLQ > 0, or a count mismatch), or `FAILED`. The absence of errors does not count as success.

## Retry policy

| Layer | Condition | Strategy |
|---|---|---|
| Kaggle download | Network / 5xx | Retry with backoff (max 3), then fail |
| Kafka producer | Retriable broker errors | Handled by the client (idempotent producer). A final delivery failure fails the run |
| Consumer side effects | Transient I/O (file lock, broker unavailable) | Bounded exponential backoff without committing. When exhausted, exit non-zero |
| Validation / contract | Any | **Never retry.** DLQ |
| Bugs (unexpected exceptions) | Any | **Never retry.** Don't commit. Exit non-zero, fix, replay |
| DuckDB transaction | Any error | `ROLLBACK`. Don't retry within the transaction |

## Reliability practices

- Graceful shutdown on SIGINT/SIGTERM: stop polling, flush or commit, close the client, write the summary.
- Keep evidence after failures: raw Parquet, DuckDB files, logs and DLQ contents are never deleted automatically.
- Use the stage-gate checklists in `pipeline-architecture/references/stage-gates.md`.

## Future (not implemented)

Alert notifications (Slack/email webhook), data-freshness SLAs (`max(ingestion_time)` vs expected), monitoring tables. Implement them only when asked.
