# Frontend integration guide (dashboard, copilot, 3D digital twin)

No frontend exists in this repository yet. This guide is the contract for one. All routes are under
`/api/v1` and return JSON. Errors always look like this:

```json
{"error": {"code": "...", "message": "...", "details": [...]}, "request_id": "..."}
```

When `API_AUTH_TOKEN` is set, send `Authorization: Bearer <token>`. `/health` is exempt.
Browsers need `API_CORS_ORIGINS`. CORS currently allows GET only, so add POST before calling the POST
routes from a browser.

## 1. Stable object-ID contract (Three.js)

- Every layout object has a stable `id` (`srv-013`, `rack-21`, `aisle-2`, `room-1`, `crah-1`).
  Telemetry-linked objects also have an `asset_id` (`SIM-NA-013`).
- Store both on the mesh: `mesh.userData = {id: obj.id, assetId: obj.asset_id}`.
- On click, resolve with `userData.id`. **Never use screen or mesh positions to identify an asset.**
- Every state, overlay and stream payload carries `object_id` and `asset_id`, so updates map directly to
  meshes.

## 2. Load sequence

1. **Layout.** `GET /digital-twin/layout` returns `objects[]`, each with `position{x,y,z}` (metres, object
   centre), `rotation`, `dimensions{width,height,depth}`, `parent_id`, `type` and `telemetry_available`.
   - Build groups from `parent_id`: facility → room → aisle → rack → server; `cooling_unit` sits under a
     room.
   - Keep `layout_version`. Later calls with `?layout_version=<v>` return `{"unchanged": true}` when
     nothing changed.
   - `demonstration: true` means the layout is illustrative. Show the `notice` text in the UI.
2. **Initial state.** `GET /digital-twin/state` returns one batch for every mapped asset.
   - Per asset: `values.<metric> = {value, unit, kind}`, where `kind` is `measured`, `calculated` or
     `unavailable`.
   - Also per asset: `freshness` (FRESH / STALE / NO_DATA), `thermal_status` (WITHIN_LIMIT /
     APPROACHING_LIMIT / ABOVE_LIMIT / UNKNOWN), `active_alerts` and `open_recommendations`.
   - `aggregates[]` holds rack, aisle and room roll-ups; the `method` field says how they were computed.
   - `status: RESTRICTED` marks an asset outside the operator's scope; `NO_TELEMETRY` marks an object
     with no telemetry.
   - **Render null as "no data", never as 0.**
3. **Colours.** `GET /digital-twin/overlays?type=temperature` returns `items[]` of
   `{object_id, asset_id, value, unit, status, is_predicted, observed_at, data_source}` plus `bands`.
   - Map `status` to colours on the client.
   - Types: `temperature`, `predicted_temperature`, `workload`, `thermal_risk`, `energy` (server IT
     power, not cooling power), `fan_excess` (fan speed vs comparable peers: value = excess fraction,
     status ELEVATED / WITHIN_PEER_RANGE / BELOW_PEERS / INSUFFICIENT_PEERS; relative only), `airflow` (magnitude only, no direction), `cooling_efficiency` (always
     UNAVAILABLE: not measured) and `data_quality`.
   - Use `timestamp=` for historical views.
   - Draw predicted values (`is_predicted: true`) differently from measured ones, for example dashed or
     with a label.
4. **Live updates.** `GET /digital-twin/stream` is Server-Sent Events.
   - Events: `telemetry_update`, `asset_status_change`, `alert_created`, `recommendation_created`,
     `layout_changed`, `heartbeat` and `resync_required`.
   - Payload: `{id, event, asset_id, timestamp, data}`.
   - On reconnect, send `Last-Event-ID` and missed events are replayed. If they're too old you get
     `resync_required`: re-fetch `/state`. On `layout_changed`, re-fetch `/layout`.
   - Native `EventSource` cannot send an Authorization header. With `API_AUTH_TOKEN`, use a fetch-based
     SSE client. Without streaming, poll `/state?since=<state_cursor>` once a minute (telemetry is
     1-minute data).
5. **Click → inspection panel.** `GET /digital-twin/assets/{object_id}` (an asset ID works too) returns
   `object`, `parents[]`, `detail` (latest reading and cooling state), `history` (5-minute buckets),
   `forecast` (`status` AVAILABLE/UNAVAILABLE), `alerts[]` and `recommendations[]`.
   - Racks and rooms return `kind: "aggregated"` with child summaries.
6. **Ask about the selection.** `POST /digital-twin/analyze` with
   `{"object_id": "rack-21", "question": "...", "analysis_type": "thermal"}`.
   - The response has `summary` and `sections` (measured / calculated / predictions / hypotheses /
     recommendations / limitations), plus `evidence[]`, `predictions[]`, `recommendations[]`,
     `warnings[]` and `execution`.

## 3. Dashboard and copilot screens

- **Dashboard.** `GET /dashboard/overview` returns `kpis.<metric>` (`mean/min/max`, `assets_reporting`,
  `unit`, `note`) plus `thermal_risk`, `anomalies`, `trends.points` (chart series), `data_quality`,
  `warnings` and `semantics` (aggregation rules).
- **Charts.** `GET /assets/{id}/telemetry?interval=5m&metrics=inlet_temperature_c,workload_intensity`.
  Each bucket has `{mean,min,max,count}`; `count: 0` means "no data". Page with `next_cursor`.
- **Copilot.** `POST /copilot/chat` with `{"message": "...", "conversation_id": null}`. Keep the returned
  `conversation_id` for follow-ups.
  - Show `interpretation.time_basis` and `asset_ids` above the answer, so the operator sees what was
    understood.
  - `clarification_needed: true` means the answer is a question back to the operator.
  - Render `metrics` tables, `predictions`, `recommendations[].link` and `warnings` (data or model
    insufficiency).
  - Suggested starter questions:
    - "Which assets show rising inlet temperatures despite stable workloads in the last 2 hours?"
    - "What is the current inlet temperature and workload of SIM-NA-013?"
    - "Forecast the inlet temperature of SIM-NA-013."
    - "What should we investigate first?"
    - "Show existing recommendations for SIM-NA-018."
- **Recommendations.**
  - `GET /recommendations?status=REQUIRES_REVIEW&limit=20` (cursor pagination) and
    `GET /recommendations/{id}`.
  - `POST /recommendations/generate` with `{asset_id, as_of?}`. With LLMs this can take tens of seconds,
    so show progress.
  - `POST /recommendations/{id}/decisions` with `{decision: APPROVED_FOR_EVALUATION|REJECTED|DEFERRED,
    reviewer, note}`. This records a review only; it never sends a command.
- **Alerts.** `GET /alerts` and `GET /alerts/{id}`.

## 4. What the UI must not imply

- No endpoint changes equipment, and the UI should not offer "apply" buttons.
- Energy savings in kW are never computed: no cooling-power measurement exists. Fan-energy waste is
  shown only as a relative indicator ("+18 % vs peers", affinity-law ratio). Label it relative, not
  as kW or a saving.
- The layout is a demonstration. There is no CFD, and the airflow overlay is magnitude only.
