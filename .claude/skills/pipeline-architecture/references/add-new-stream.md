# Example: Adding a New Stream End to End

This is adapted from the QuantBridge `examples/add-new-pipeline.md` (IEA API → DLT → Supabase). Here the same discipline is applied to: dataset file → producer → Kafka → consumers → storage.

> Column names below are **placeholders** (`<timestamp_col>`, `<entity_col>`, `<metric_cols>`). Replace them with real names from `docs/dataset/` after profiling. Never invent them.

| Step | Skill | Gate (see `stage-gates.md`) | Rule |
|---|---|---|---|
| 1. Profile the source file | `dataset-ingestion` | Pre-Ingestion | Inspect before assuming |
| 2. Write/extend the data contract | `data-validation` | — | Show contract for approval |
| 3. Map rows → event envelope | `data-transformation` | Pre-Produce | Sample events approved |
| 4. Create topics | `kafka-streaming` | — | Partitions/key justified |
| 5. Producer (replay or simulator) | `kafka-streaming` | Pre-Produce | Deterministic `event_id`, provenance set |
| 6. Raw-sink consumer | `storage-design` | Post-Land | Append-only, offset-named files |
| 7. Validator consumer + DLQ | `data-validation` | On-Failure | Invalid → DLQ with reason |
| 8. Curated transformations | `storage-design`, `data-transformation` | Pre-Write | Idempotent rebuild |
| 9. Detection (optional) | `anomaly-detection` | — | Evidence attached |
| 10. Tests + smoke run | `pipeline-testing` | Post-Run | Tests mandatory |

## Step 3: mapping (sketch)

```python
# src/data_engineering/events/mapping.py
def row_to_event(row: dict, *, source_file: str, row_number: int, run_id: str) -> dict:
    return {
        "event_id": make_event_id(source_file, row_number),        # deterministic
        "schema_version": "1",
        "event_type": "telemetry.observation",
        "provenance": "replay",                                    # dataset | replay | synthetic
        "event_time": to_utc_iso(row["<timestamp_col>"]),          # from the dataset, never now()
        "ingestion_time": None,                                     # set by producer at send time
        "source": {"dataset": "deeplumiere/hyperscale-data-center-dataset",
                   "file": source_file, "row_number": row_number},
        "entity": {"<entity_col>": row["<entity_col>"]},           # only if such a column exists
        "payload": {k: row[k] for k in METRIC_COLUMNS},            # names from the contract
        "run": {"run_id": run_id},
    }
```

## Step 10: smoke run (target commands, once implemented)

```bash
docker compose up -d kafka
uv run python -m data_engineering.producers.replay_producer --sample-rows 100
uv run python -m data_engineering.consumers.raw_sink --once
uv run python -m data_engineering.consumers.validator --once
uv run pytest tests/unit
uv run pytest -m integration tests/integration
```

## Ship checklist

- The contract in `contracts/` is updated, and `schema_version` is bumped if a change breaks compatibility.
- Topic list/config is recorded (in `kafka-streaming` notes or in code).
- Tests cover mapping, validation, idempotency and replay determinism.
- `CLAUDE.md` commands are updated if new entrypoints were added.
