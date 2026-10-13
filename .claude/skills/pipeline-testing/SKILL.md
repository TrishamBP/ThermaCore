---
name: pipeline-testing
description: Testing strategy for the data center operations pipeline — pytest unit tests for mapping/validation/detection logic, contract tests, idempotency and replay-determinism tests, Kafka integration tests against the local Docker broker, and DuckDB data-quality tests. Use when adding or changing any pipeline code, before claiming a stage works, or when debugging data quality.
---

# Pipeline Testing

Adapted from the QuantBridge `testing` skill. That skill used standalone `uv run python src/tests/test_*.py` scripts against a DuckDB artifact. Here tests use **pytest** and cover streaming behavior. The core rule is kept: **no pipeline change is done until its tests exist and pass.** A file existing, or a script running without error, does not show that it works.

`pytest` is a dev dependency. Ask before adding it: `uv add --dev pytest`.

## Layout

```
tests/unit/          # no Kafka, no network; fast; run always
tests/integration/   # need `docker compose up -d kafka`; marked @pytest.mark.integration
tests/fixtures/      # small hand-checked sample rows COPIED from the dataset (labelled as such) or clearly synthetic
```

Register the marker in `pyproject.toml`:
```toml
[tool.pytest.ini_options]
markers = ["integration: needs local Kafka (docker compose up -d kafka)"]
addopts = "-m 'not integration'"
```

## What to test

| Area | Tests |
|---|---|
| Mapping (`data-transformation`) | Row → envelope: field names, UTC conversion, units carried, nulls preserved, provenance set |
| Event identity | The same input gives the same `event_id`. Different rows give different IDs |
| Contract (`data-validation`) | Valid events pass. Each rule has a failing case. Unknown `schema_version` is rejected |
| Validator routing | Invalid → DLQ record with the correct headers. Valid → validated topic. Test the pure `process()` function with no broker |
| Idempotency | Processing the same batch twice gives the same raw file name/contents and no duplicate curated rows |
| Replay determinism | Two replays of the same fixture produce identical event IDs and identical anomaly IDs |
| Detection (`anomaly-detection`) | Injected spike/flatline is detected. Warm-up is respected. Streaming result equals batch SQL result on the same fixture |
| Batch DQ | Row count > 0, key nulls = 0, `event_id` unique in curated, timestamps in range, units present |
| Integration | Produce N fixture events → consumers run → raw store has N unique `event_id`s. Committed offsets equal the end offsets. Kill and restart the consumer mid-batch → no loss, duplicates deduped |

## Integration test practices

- Use unique topic names and consumer group IDs per test run (suffix with a UUID), so tests don't interfere with each other or with dev data.
- Bound every poll loop with a timeout. Never wait forever.
- Clean up test topics at the end (best effort).

## Commands (target, once code exists)

```bash
uv run pytest                                  # unit tests
docker compose up -d kafka
uv run pytest -m integration                   # integration tests
```

## Test data rules

- Fixtures copied from the dataset keep their original values and state their source file and row.
- Synthetic fixtures are named and commented as synthetic.
- Never assert on values that "seem realistic". Assert on values in the fixture.
