# API reference

All routes are defined under `src/app/` and mounted in `src/app/api/v1/router.py` under the prefix
`/api/v1`. This list was checked against the generated OpenAPI schema: 29 operations. The same handlers
are also served under the deprecated prefix `/api` (hidden from OpenAPI). Interactive documentation is at
`/docs` when `API_DOCS_ENABLED=true`.

Examples marked **(real, shortened)** are actual responses from the local store during the documentation
pass (simulated data, deterministic agent mode). Values are simulated telemetry, not measurements of real
equipment.

## Contents

- [Conventions](#conventions)
- [System](#system) · [Dashboard](#dashboard) · [Telemetry and assets](#telemetry-and-assets) ·
  [Cooling model and optimizer](#cooling-model-and-optimizer) · [Recommendations and alerts](#recommendations-and-alerts) ·
  [Copilot](#copilot) · [Digital twin](#digital-twin)
- [Planned (not implemented)](#planned-not-implemented)

---

## Conventions

**Authentication.** Off by default. With `API_AUTH_TOKEN` set, every route except `GET /health` requires
`Authorization: Bearer <token>`. A missing or wrong token gets `401 unauthorized`; the comparison is
constant-time (`app/common/security/auth.py: require_operator`).

**Asset scope.** `API_ALLOWED_ASSETS` (JSON list). Requests naming an out-of-scope asset get
`403 asset_not_authorized`. Lists, aggregates, alerts and recommendations silently exclude out-of-scope
assets; the twin marks them `RESTRICTED`; SSE events for them are not delivered.

**IDs.** `asset_id`, `run_id`, layout `object_id`: `^[A-Za-z0-9._:-]{1,64}$`. Recommendation and analysis
IDs: `^(rec|ana)-[0-9a-f]{20}$`. Alert IDs: `^alr-[0-9a-f]{20}$`. Conversation IDs: `^conv-[0-9a-f]{20}$`.

**Times.** ISO 8601 **with a timezone** (`2026-01-01T13:45:00Z`). Naive datetimes give `422`. Ranges are
`start` exclusive, `end` inclusive, in simulated event time.

**Default run.** When `run_id` is omitted, the API uses the simulation run whose assets were updated most
recently (`store.latest_run_id()`). Asset IDs repeat across runs, so pass `run_id` for reproducible results.

**Unknown parameters** are rejected (`extra="forbid"`).

**Error shape** (every non-2xx response, including 404 for unknown routes and 405s):

```json
{"error": {"code": "validation_error", "message": "Request validation failed",
           "details": [{"loc": ["query", "as_of"], "msg": "...", "type": "value_error"}]},
 "request_id": "4c401e17a3ba485791a656eac3ccf978"}
```

| Status | Codes |
|---|---|
| 401 | `unauthorized` |
| 403 | `asset_not_authorized`, `training_disabled` |
| 404 | `not_found`, `unknown_asset`, `unknown_run`, `unknown_recommendation`, `unknown_analysis`, `unknown_alert`, `unknown_layout_object` |
| 409 | `training_in_progress` |
| 422 | `validation_error`, `invalid_parameter`, `insufficient_data` |
| 500 | `internal_error` (generic message; details only in logs) |
| 503 | `database_unavailable`, `store_not_initialized`, `model_not_trained`, `model_artifact_invalid`, `layout_not_configured`, `layout_invalid`, `stream_capacity_reached` |

**Headers.** `X-Request-ID` is accepted if it matches `[A-Za-z0-9._-]{1,64}`, otherwise generated; it is
always echoed back. Security headers: `nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`,
`Cross-Origin-Resource-Policy: same-origin`, plus `Content-Security-Policy: default-src 'none'` and
`Cache-Control: no-store` on API paths.

**CORS.** Only for `API_CORS_ORIGINS`, and only for `GET`. Browser clients cannot call the POST routes
until `app/middleware.py` allows them.

**Units.** Each metric's unit is in the response (`degC`, `percent`, `ratio_0_1`, `rpm`, `cfm`, `kW`,
`ratio`, …; `telemetry_query.UNITS`). `power_consumption_kw` is per-server IT power, not cooling or facility
power. GPU utilization is `null` for non-GPU servers.

---

## System

### `GET /api/v1/health`

No auth. Opens a store connection without requiring the schema.
`src/app/api/v1/router.py: health`.

```json
{"status": "ok", "database": "sqlite", "schema_version": 2}
```
`status` is `degraded` when the schema version is not 2. `503 database_unavailable` when the store
cannot be opened. It does not check the model or the LLM provider.

---

## Dashboard

### `GET /api/v1/dashboard/overview`

`src/app/dashboard/router.py`, `service.py: DashboardService.overview`. Read-only connection.

| Query | Type | Default | Validation |
|---|---|---|---|
| `asset_id` | str | all assets in scope | ID pattern; 403 if out of scope |
| `run_id` | str | latest run | ID pattern |
| `start_time` | datetime | `end_time − API_DASHBOARD_TREND_MINUTES` | must precede `end_time`; span ≤ `API_MAX_TELEMETRY_RANGE_HOURS` |
| `end_time` | datetime | newest reading of the run | — |

Response fields: `simulation_run_id`, `scope`, `window`, `latest_telemetry_timestamp`,
`assets {known, with_observed_reading, fresh, stale[], without_reading[]}`, `kpis.<metric>
{unit, assets_reporting, mean, min, max, max_asset_id[, sum for power][, note]}`, `thermal_risk`
(estimator states/severities/findings by rule), `anomalies` (injected labels, explicitly not
detections, plus alert counts), `trends {points[], slopes}`, `data_quality`, `unavailable_measurements`,
`semantics` (the aggregation rules as text), `warnings`.

```bash
curl "http://127.0.0.1:8000/api/v1/dashboard/overview?run_id=sim-c9944cc35156"
```
**(real, shortened)**
```json
{"simulation_run_id": "sim-c9944cc35156",
 "window": {"start_time": "2026-01-01T22:59:00Z", "end_time": "2026-01-01T23:59:00Z", "trend_interval_minutes": 5},
 "assets": {"known": 20, "with_observed_reading": 20, "fresh": 20, "stale": [], "without_reading": []},
 "kpis": {"inlet_temperature_c": {"unit": "degC", "assets_reporting": 20, "mean": 20.8728, "min": 19.869,
                                  "max": 22.664, "max_asset_id": "SIM-NA-012"}, "...": "..."}}
```
Errors: 403, 404 `unknown_run`/`unknown_asset`, 422 (`invalid_parameter` for range problems), 503.

---

## Telemetry and assets

### `GET /api/v1/telemetry/cooling-state`

Cooling State Estimator snapshot. `src/app/telemetry/router.py`, `service.py`;
response model `data_engineering.cooling.models.CoolingStateSnapshot`.

| Query | Default | Validation |
|---|---|---|
| `asset_id` | data-center scope | ID pattern |
| `run_id` | latest run | ID pattern |
| `window_minutes` | `config/cooling_state.yaml: window_minutes` (15) | 1–1440 |
| `as_of` | newest reading | timezone required |

Returns `state` (`INSUFFICIENT_DATA`, `HIGH_THERMAL_LOAD`, `RAPID_TEMPERATURE_RISE`,
`APPROACHING_THERMAL_LIMIT`, `POTENTIAL_AIRFLOW_ISSUE`, `DATA_QUALITY_ISSUE`, `LIGHT_LOAD`,
`PARTIALLY_ASSESSED`, `NORMAL`), `severity`, per-metric statistics, `findings[]` with evidence,
`rules_not_evaluated[]`, `data_quality`, and per-asset states at data-center scope. An empty store gives
`200` with `INSUFFICIENT_DATA`. Rules and an annotated example:
[PIPELINE_GUIDE Part III](PIPELINE_GUIDE.md#part-iii-cooling-state-estimator).

```bash
curl "http://127.0.0.1:8000/api/v1/telemetry/cooling-state?asset_id=SIM-NA-013&run_id=sim-c9944cc35156&as_of=2026-01-01T13:45:00Z"
```
Errors: 404 `unknown_asset`/`unknown_run`, 422, 503.

### `GET /api/v1/telemetry` and `GET /api/v1/assets/{asset_id}/telemetry`

History for charts. `src/app/assets/router.py`, `service.py: AssetService.telemetry`,
`data_engineering/telemetry_query.py`.

| Query | Default | Validation |
|---|---|---|
| `asset_id` (first route only; path in the second) | all assets in scope | ID pattern |
| `run_id` | latest run | — |
| `start_time` / `end_time` | `end − 60 min` / newest reading | span ≤ `API_MAX_TELEMETRY_RANGE_HOURS` |
| `interval` | `raw` | `raw`, `1m`, `5m`, `15m`, `1h` (epoch-aligned UTC buckets) |
| `metrics` | core cooling metrics | comma-separated names from the 16 telemetry metrics + `temperature_delta_c`; no duplicates |
| `limit` | 1000 | 1–50,000, capped by `API_MAX_TELEMETRY_POINTS` |
| `cursor` | — | `next_cursor` from the previous page; single asset only |

Each series reports `points`, `truncated`, `readings`, `duplicates_collapsed`, `missing_readings`,
`gaps_over_tolerance`, `largest_gap_seconds`. Buckets carry `{mean, min, max, count}` per metric;
`count: 0` means no data. The total points across assets are capped (`warnings` says when).

```bash
curl "http://127.0.0.1:8000/api/v1/assets/SIM-NA-013/telemetry?run_id=sim-c9944cc35156&interval=5m&metrics=inlet_temperature_c,workload_intensity"
```

### `GET /api/v1/assets`

`run_id`, `limit` (1–1000, default 200). Each item has type, cooling type, first/last event time, latest
observed time, and `fresh`, meaning the latest observed reading is within `stale_after_seconds` of the run's
newest reading.

### `GET /api/v1/assets/{asset_id}`

`run_id` optional. Returns the public asset fields (including provenance: generator, version, seed), the
latest observed reading with units and notes, the estimator state and findings, `unavailable_measurements`
and `warnings` (no reading; pipeline not delivering, by wall clock).

---

## Cooling model and optimizer

All in `src/app/cooling/`. None of these routes can actuate anything.

### `POST /api/v1/cooling/predict`

| Body | Type | Validation |
|---|---|---|
| `asset_id` | str | required, ID pattern; scope-checked |
| `run_id` | str? | ID pattern |
| `as_of` | datetime? | forecast origin; default newest reading |
| `horizons_minutes` | int[]? | 1–10 items; subset of trained horizons (5, 10) |

Always `200` for a known asset. `status: AVAILABLE | UNAVAILABLE`, `reasons[]` from `MODEL_NOT_TRAINED`,
`MODEL_ARTIFACT_INVALID`, `FEATURE_SCHEMA_MISMATCH`, `MODEL_STALE`, `HORIZON_NOT_SUPPORTED`,
`INSUFFICIENT_TELEMETRY`.

**(real, shortened)** for `{"asset_id": "SIM-NA-013", "run_id": "sim-c9944cc35156", "horizons_minutes": [10, 30]}`:
```json
{"status": "AVAILABLE", "reasons": [], "observed_at": "2026-01-01T23:59:00Z", "latest_inlet_temperature_c": 19.869,
 "forecasts": [{"horizon_minutes": 10, "forecast_timestamp": "2026-01-02T00:09:00Z",
                "predicted_inlet_temperature_c": 19.8676, "persistence_inlet_temperature_c": 19.869,
                "kind": "model_prediction",
                "evaluation": {"offline_test_rmse_c": 0.0656, "offline_test_mae_c": 0.0518,
                               "offline_persistence_rmse_c": 0.0647, "rmse_skill_vs_persistence": -0.0145,
                               "test_rows": 6051}}],
 "unsupported_horizons": [30],
 "model": {"version": "crm-20261013T120438Z-1443761922fc8cc7", "label": "forecasting_baseline", "...": "..."},
 "uncertainty": {"method": "offline test-split RMSE per horizon (simulated data); not a calibrated prediction interval"},
 "recommendation_only": true}
```
Errors: 403, 404, 422, 503 `database_unavailable`.

### `GET /api/v1/cooling-model/predict`

Older contract, kept. Query `asset_id` (required), `run_id`, `as_of`. Returns `forecasts[]`,
`model_label`, `data_quality {status OK|DEGRADED, notes}`, `model_validity_warnings`,
`extrapolated_features`. A missing model gives `503 model_not_trained` / `model_artifact_invalid`
instead of a status field; too little data gives `422 insufficient_data`.

### `GET /api/v1/cooling-model/evaluate`

No database access. Returns the active artifact's `metadata.json` content: version, label, target,
horizons, training runs (used and skipped, with reasons), split cut-offs, per-horizon validation/test
metrics, segment reports, validity checks and `limitations`. 503 without a model.

### `POST /api/v1/cooling-model/train`

Body `{"run_ids": [...]}` (optional, ≤ 50). **Disabled by default:** `403 training_disabled` unless
`API_MODEL_TRAINING_ENABLED=true`. `409 training_in_progress` while another run holds the lock. Side
effects: writes a new artifact directory, atomically replaces `ACTIVE`, and reloads the registry. It is
CPU-heavy and runs inside the request.

### `POST /api/v1/cooling-optimizer/recommend`

Body `{asset_id, run_id?, as_of?}`. Evaluates the configured grid (`optimizer.setting_min/max/search_step`).
`503` without a model.

**(real, shortened)**
```json
{"status": "UNAVAILABLE",
 "reasons": [{"code": "RESPONSE_MODEL_NOT_VALIDATED", "detail": "the model is a 'forecasting_baseline'; ... in only 0 of 35 assets (needs >= 80%)"},
             {"code": "ENERGY_OBJECTIVE_MISSING", "detail": "no measured cooling-system power exists in the telemetry ..."},
             {"code": "T_SAFE_NOT_CONFIGURED", "detail": "optimizer.t_safe_c is not set"},
             {"code": "ACTUATOR_BOUNDS_NOT_CONFIGURED", "detail": "..."}],
 "recommendation_only": true, "actuation": "none: this system only recommends; it never sends commands to equipment",
 "controllable_setting": "fan_speed_rpm", "current_setting": 2489.073, "recommended_setting": null,
 "forecast_under_current_conditions": [{"horizon_minutes": 5, "predicted_inlet_temperature_c": 19.8681, "...": "..."}],
 "candidates": [], "energy_objective": {"available": false, "source": null}}
```
Statuses: `RECOMMENDATION_AVAILABLE`, `NO_FEASIBLE_SETTING`, `CONSTRAINTS_ONLY`, `UNAVAILABLE`.

### `POST /api/v1/cooling/optimize`

Body `{asset_id, run_id?, as_of?, candidate_settings: {"fan_speed_rpm": [3000, 3200]}}`. 1–50 finite
values per setting; only the configured controllable setting is accepted (otherwise
`422 invalid_parameter`). Never returns 503 for a missing model: the reason appears in `reasons`. Adds
`conclusion: RECOMMENDATION | INCONCLUSIVE` and a note that candidate values are not approved limits.

---

## Recommendations and alerts

`src/app/recommendations/`. The workflow is in `src/data_engineering/recommendations/`
([AGENT_ARCHITECTURE.md](AGENT_ARCHITECTURE.md)).

### `POST /api/v1/recommendations/generate`

Body `{asset_id, run_id?, as_of?}`. Omit `as_of` for a live analysis; pass it for a historical window
(then wall-clock pipeline staleness is not treated as a problem of that window). Runs the LangGraph
workflow synchronously. **Side effects:** one `recommendation_analyses` row, recommendations (deduplicated
per run/asset/type/window end) and alerts, in one transaction. Duration: not measured in this
documentation pass (the endpoint writes to the store, so it was not smoke-tested); earlier live LLM runs
observed specialist calls of 8–18 s each.

Response: `analysis_id`, `status` (`COMPLETED` | `COMPLETED_WITH_FALLBACK`), `observation_window`,
`recommendations[]` (full `Recommendation` objects: type, status, severity, title, recommended_action,
evidence[], predicted_temperature, energy_impact_status, uncertainty, conditions that invalidate it,
constraints checked, validation warnings, missing evidence, hypotheses, explanation),
`duplicates_of_existing[]`, `execution` (mode, models, plan, per-agent status, not-applicable types with
reasons, node trace with latency and tokens), `human_approval_required: true`,
`automatic_control_executed: false`.

Errors: 403, 404 `unknown_asset`/`unknown_run`, 422, 503 (store unavailable, checked before the run).

### `GET /api/v1/recommendations`

Query: `asset_id`, `run_id`, `type`, `status` (`REQUIRES_REVIEW`|`INCONCLUSIVE`), `severity`,
`start_time`/`end_time` (filter the observation window end), `limit` (1–200, default 50), `cursor`.
Newest first; keyset pagination on `(created_at, recommendation_id)`. Each item has `review_status` (latest
decision or `PENDING_REVIEW`).

### `GET /api/v1/recommendations/{recommendation_id}`

The stored recommendation payload, `review_status`, `created_at`, `decisions[]`.

### `POST /api/v1/recommendations/{recommendation_id}/decisions`

Body `{decision: APPROVED_FOR_EVALUATION | REJECTED | DEFERRED, reviewer: "^[\w .@-]+$" (1–100), note?: ≤2000}`.
**Side effect:** appends one decision row. Nothing is executed (`automatic_control_executed: false`).
`reviewer` is not tied to the bearer token. The endpoint is not idempotent: each call appends.

```bash
curl -X POST http://127.0.0.1:8000/api/v1/recommendations/rec-0123456789abcdef0123/decisions \
     -H "Content-Type: application/json" -d '{"decision": "DEFERRED", "reviewer": "operator-1", "note": "wait for next window"}'
```

### `GET /api/v1/recommendations/analyses/{analysis_id}`

Execution trace of one analysis: plan, agents, node statuses, latency, token counts, models.

### `GET /api/v1/recommendations/agents/status`

`mode` (`llm`|`deterministic`), `llm_enabled`, `api_key_configured` (boolean only), model IDs, reasoning
effort, timeout, active cooling model version and label, `recommendation_only: true`.

**(real)** `{"mode": "deterministic", "llm_enabled": false, "api_key_configured": true, "main_model": "accounts/fireworks/models/glm-5p3-flash", "subagent_model": "accounts/fireworks/models/deepseek-v4p1-flash", "reasoning_effort": "low", "timeout_s": 60.0, "cooling_model": {"version": "crm-20261013T120438Z-1443761922fc8cc7", "label": "forecasting_baseline"}, "recommendation_only": true}`

### `GET /api/v1/alerts`, `GET /api/v1/alerts/{alert_id}`

Query: `asset_id`, `run_id`, `rule_id` (`^[A-Z_]{3,64}$`), `severity` (`WARNING`|`CRITICAL`),
`start_time`/`end_time`, `limit`, `cursor`. An alert is a WARNING/CRITICAL estimator finding of an
analysed window, persisted **by `POST /recommendations/generate`**. Nothing creates alerts continuously.
Each payload says `"not a confirmed equipment fault"`.

---

## Copilot

### `POST /api/v1/copilot/chat`

`src/app/copilot/router.py`; engine in `src/data_engineering/copilot/`.

| Body | Validation |
|---|---|
| `message` | 1–2000 chars |
| `conversation_id` | `conv-` + 20 hex, optional; unknown/expired IDs start a new conversation |
| `asset_id` | optional context; scope-checked (403) |
| `run_id` | optional |
| `time_range {start, end}` | optional; `start < end` |

Response: `conversation_id`, `answer`, `clarification_needed`, `interpretation` (intents, asset_ids,
all_assets, start/end, `time_basis`, source, run), `sections` (measured / calculated / predictions /
hypotheses / recommendations / limitations), `evidence[]` (`E1`, `E2`, … with kind, metric, value, unit,
source), `metrics` (tool tables), `predictions`, `recommendations`, `optimizer`, `warnings`,
`agent_execution_summary` (mode, answer source, tool statuses, specialists, models, tokens, node trace),
`read_only: true`, `automatic_control_executed: false`.

**(real, shortened, deterministic mode)** for "Which assets show rising inlet temperatures despite stable
workloads in the last 2 hours?":
```json
{"answer": "For all assets in scope over the 120 minutes ending at the newest reading (simulated event time): Calculated: 0 of 20 assets show a rising inlet trend with a stable workload. Calculated over 20 assets: highest mean inlet SIM-NA-012 (22.6128 degC), lowest SIM-NA-013 (19.7824 degC). ...",
 "interpretation": {"intents": ["trend", "workload", "thermal", "history"], "all_assets": true,
                    "start_time": "2026-01-01T21:59:00+00:00", "end_time": "2026-01-01T23:59:00+00:00",
                    "time_basis": "the 120 minutes ending at the newest reading (simulated event time)", "source": "deterministic"}}
```
A control request ("Set the fan speed of SIM-NA-013 to 2000 rpm now please") gets an answer that begins
"This copilot is read-only and recommendation-only: it cannot change fan speeds…", followed by the
optimizer's `UNAVAILABLE` reasons. **Side effect:** only the bounded in-memory conversation.

---

## Digital twin

`src/app/digital_twin/`. Details: [DIGITAL_TWIN.md](DIGITAL_TWIN.md); frontend contract:
[frontend_integration.md](frontend_integration.md).

| Route | Query / body | Returns |
|---|---|---|
| `GET /digital-twin/layout` | `room_id`, `include_servers` (true), `include_cooling_equipment` (true), `layout_version` | Layout meta (`layout_version`, `demonstration`, `notice`, units) + `objects[]` with position/rotation/dimensions/parent/asset_id/`telemetry_available`. `{"unchanged": true, "objects": []}` when `layout_version` matches |
| `GET /digital-twin/layout/{object_id}` | — | One object with `parents[]` and `children[]` ids; 404 `unknown_layout_object` |
| `GET /digital-twin/state` | `run_id`, `room_id`, `asset_type=server`, `asset_ids` (comma-separated, ≤4000 chars; asset or object ids), `since`, `include_derived_metrics` | Per mapped asset: `values.<metric> {value, unit, kind}`, `freshness`, `thermal_status`, `active_alerts`, `open_recommendations`, `status` (`OK`/`STALE`/`NO_DATA`/`NO_TELEMETRY`/`RESTRICTED`); `aggregates[]` for rack/aisle/room/facility; `state_cursor` |
| `GET /digital-twin/overlays` | `type` (one of 8), `run_id`, `room_id`, `asset_type`, `timestamp` (historical) | `items[] {object_id, asset_id, value, unit, status, observed_at, data_source, is_predicted, warnings}`, `bands` |
| `GET /digital-twin/assets/{object_id}` | `run_id`, `start_time`, `end_time`, `metrics`, `include_forecast`, `include_recommendations` | Inspection panel: object, parents, asset detail, 5-min history, forecast (`/cooling/predict` semantics), recent alerts and recommendations. Racks/rooms: `kind: aggregated` with children |
| `POST /digital-twin/analyze` | `{object_id or asset_id, question (1–2000), analysis_type?, run_id?, time_range?}` | Copilot answer for the object's assets: `summary`, `sections`, `evidence`, `predictions`, `recommendations`, `execution`, `conversation_id` |
| `GET /digital-twin/stream` | header `Last-Event-ID`, query `max_events` (1–10000) | `text/event-stream`: `telemetry_update`, `asset_status_change`, `alert_created`, `recommendation_created`, `layout_changed`, `heartbeat`, `resync_required`; 503 `stream_capacity_reached` above `API_TWIN_MAX_SUBSCRIBERS` |

**(real)** first SSE frame:
```
id: 1
event: heartbeat
data: {"id": 1, "event": "heartbeat", "asset_id": null, "timestamp": "2026-10-13T13:30:03.296446Z", "data": {"subscribed": true, "poll_interval_s": 10.0}}
```

**Known defect (verified during documentation):** `POST /digital-twin/analyze` on a rack, with a question
that contains a topology word ("Why is this **rack** warm?"), returns `clarification_needed: true`. The
twin resolves the rack's five servers, but the copilot's parser (`copilot/understanding.py`, the
`TOPOLOGY_WORDS` check) asks for clarification whenever such a word appears, even when asset IDs are
present. The same question without the word ("Why is it warm?") is answered for all five servers.

---

## Planned (not implemented)

These routes do **not** exist. They are listed so they are not mistaken for working ones.

| Capability | Status / closest existing feature |
|---|---|
| Ingest telemetry over HTTP | Not built; Kafka is the only ingestion path |
| Acknowledge/resolve alerts; close recommendations | Not built; decisions are the only lifecycle step |
| DuckDB analytics over HTTP | CLI only (`python -m data_engineering.analytics.query_telemetry`) |
| Async analysis jobs (`202` + polling) | Not built; `/recommendations/generate` is synchronous |
| Any equipment command or setpoint write | Deliberately not built |
| Per-user authentication, roles, tenants | Not built; one optional shared bearer token |
