---
name: anomaly-detection
description: Detect anomalous data center operational behavior from replayed or simulated telemetry — streaming per-entity baselines (rolling/robust z-score) in a Kafka consumer, batch detection in DuckDB, anomaly and incident records with traceable evidence and provenance, and honest evaluation. Use when designing or implementing anomaly/incident detection, choosing thresholds or methods, defining the anomaly event schema, or preparing evidence for future AI-agent investigation.
---

# Anomaly Detection

This skill is **new**. It grows out of the "Anomaly Alerts (Value-Based)" section of QuantBridge `patterns/failure-handling.md` (delta vs z-score/percent threshold → alert records in a dedicated table), which was never implemented there.

## Preconditions

- The dataset profile and contract exist. Detection runs only on metrics that are **actually in the contract**.
- Check whether the dataset has labels (failures, incidents, maintenance). This determines how detection can be evaluated (see Evaluation).
- `event_time` semantics are known: native frequency, gaps, ordering per entity.

## Methods: start simple, escalate with evidence

| Order | Method | Use when |
|---|---|---|
| 1 | **Physical/operational limits** | The user or the dataset docs provide documented limits. Never invent thresholds from general data center knowledge |
| 2 | **Rolling robust z-score** per (entity, metric): median and MAD over an event-time window | Default for streaming. Robust to the outliers it is trying to find |
| 3 | Rate-of-change / gap detection | Sudden jumps, flatlines (stuck sensor), missing data |
| 4 | Seasonal baseline (same hour-of-day / day-of-week) | Only if the profile shows enough history and periodicity |
| 5 | Multivariate (e.g. Isolation Forest) | Only if 1–4 miss known cases, there are enough features, and the user approves the new dependency |

Every method declares its parameters (window, threshold, minimum warm-up samples) in config. Each anomaly records which parameters produced it.

## Streaming detector (`dco-anomaly` consumer group)

- Consumes `dc.telemetry.validated.v1`. Per-key state lives in memory, keyed by the Kafka key, so a partition's state belongs to one consumer.
- Windows use **event_time**. Skip scoring until warm-up is reached (state that it's warming up; don't call it normal).
- Handles late or out-of-order events with an allowed-lateness watermark. Late events are counted, not silently scored against the wrong window.
- State is rebuilt on restart by replaying from an earlier offset or from the raw store. Detection must be deterministic for the same input.
- Emits to `dc.anomalies.v1`. The raw sink lands it, and `analytics.anomalies` is upserted by `anomaly_id`.

## Batch detector (DuckDB)

The same method is implemented in SQL over `curated.telemetry` for backtests and full-history runs. Streaming and batch results on the same data should match. Test this (`pipeline-testing`).

## Anomaly record

```json
{
  "anomaly_id": "sha256(method|entity|metric|window_start|window_end)",
  "event_type": "anomaly.detected",
  "provenance": "replay | synthetic",
  "detected_at": "<processing time>",
  "event_time_start": "...", "event_time_end": "...",
  "entity": { "<entity_col>": "..." },
  "metric": "<metric>", "unit": "<unit>",
  "observed": 0.0, "baseline": 0.0, "spread": 0.0, "score": 0.0,
  "method": { "name": "robust_zscore", "window": "PT1H", "threshold": 4.0, "min_samples": 30 },
  "evidence": {
    "event_ids": ["..."],
    "kafka": { "topic": "dc.telemetry.validated.v1", "partition": 0, "offsets": [100, 160] },
    "query": "SELECT ... FROM curated.telemetry WHERE ..."
  },
  "severity": "info | warning | critical"
}
```

- `provenance` is inherited. Anomalies on synthetic data are `synthetic`, and reports must say so.
- `severity` mapping is configured and agreed with the user, not inferred.

## Incidents

- Group anomalies into an incident by entity and time proximity (e.g. overlapping or within a gap window), and across metrics of the same entity.
- Store the incident in `analytics.incidents`: `incident_id`, time span, entities, anomaly_ids, summary statistics. Use **descriptive statements only**. Root cause is unknown at this stage.

## Future AI-agent handoff (not implemented)

Incident records are the input contract for future investigation agents. Design for it now:
- Every claim an agent makes must be traceable to `evidence` (event IDs, offsets, a reproducible SQL query).
- Agents must distinguish observed values, derived values and synthetic data, and must not cite signals the dataset does not contain (e.g. no "fan failure" without a fan signal).
- Recommendations must be phrased as hypotheses with supporting and missing evidence, never as confirmed root causes.

## Evaluation

| Situation | How to evaluate |
|---|---|
| The dataset has incident or failure labels | Precision, recall and detection delay against the labels, at the event-time level |
| Synthetic data with injected anomalies | Score against `synthetic_label`. Report it as synthetic-only performance |
| No labels at all | Report counts, score distributions and examples for human review. **Do not claim accuracy** |

## Failure modes

| Failure | Cause | Response |
|---|---|---|
| Alert storm | Threshold too low, or a baseline that hasn't warmed up | Enforce warm-up. Tune on held-out history. Report the rate per entity |
| Nothing detected | Threshold too high, or data too smooth | Inject synthetic anomalies to verify sensitivity. Label them synthetic |
| Streaming ≠ batch results | Window semantics differ (event vs processing time) | Fix it to event-time in both. Test parity |
| State lost on restart | In-memory state only | Replay from an offset that covers the window length |
