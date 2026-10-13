---
name: dataset-ingestion
description: Download, checksum, and profile the Kaggle hyperscale data center dataset (deeplumiere/hyperscale-data-center-dataset) before any pipeline design. Use when acquiring or refreshing the dataset, inspecting its schema/dtypes/timestamps/units/identifiers, deciding between historical replay and synthetic simulation, or when anyone is about to assume what fields the dataset contains.
---

# Dataset Ingestion and Inspection

This is the entry point for all source data. It replaces the QuantBridge `data-ingestion` and `api-connector` skills, which fetched paginated REST APIs. This project's source is a set of static files from Kaggle.

**Nothing about the dataset is known until this skill has been run.** Do not assume it has time series, equipment IDs, failure labels, maintenance events, units or any particular column until the profile shows them.

**Status: run on 2026-10-13.** Findings are in `docs/dataset/README.md`. In short: one 100,000 × 35 CSV, self-described as engineered synthetic data. It has no timestamps and no equipment IDs. It has `failure_risk_level` and `energy_efficiency_class` labels. Regions are APAC/EU/ME/NA. The pipeline code is `src/data_engineering/ingestion/` (`download_dataset.py`, `load_source.py`).

**Pitfall:** the North America region code is the literal string `NA`. Readers that convert `NA` to null (pandas' default, and older versions of this profiler) silently lose those rows. Use the `csv` module, or pandas with `keep_default_na=False`.

## Inputs

- Kaggle dataset slug: `deeplumiere/hyperscale-data-center-dataset`
- Credentials: none needed for this public dataset with Kaggle CLI 2.x (downloaded anonymously). If Kaggle asks for authentication, use `uv run kaggle auth login`, a `KAGGLE_API_TOKEN`, or a legacy `~/.kaggle/kaggle.json`. Never commit or log them.

## Outputs

- `data/raw/hyperscale-data-center-dataset/`: original files, never modified (gitignored)
- `data/raw/hyperscale-data-center-dataset/MANIFEST.json`: file name, size, sha256, download time
- `docs/dataset/profile.json`: machine-generated profile
- `docs/dataset/README.md`: human-reviewed dataset notes (see template below)

## Step-by-step

1. **Acquire.** Use one of these:
   - Manual: download the zip from the Kaggle page and extract it into `data/raw/hyperscale-data-center-dataset/`.
   - CLI: `uv run kaggle datasets download -d deeplumiere/hyperscale-data-center-dataset -p data/raw/hyperscale-data-center-dataset --unzip`. The `kaggle` package is a new dependency, so ask before adding it.
   - Downloaded files are untrusted data. Keep them in their own directory and never execute anything from it.
2. **Profile.** Run the stdlib-only profiler in isolated mode (`-I`) so nothing in the data folder can shadow imports:
   ```bash
   python -I .claude/skills/dataset-ingestion/scripts/profile_dataset.py \
       data/raw/hyperscale-data-center-dataset \
       --manifest --out docs/dataset/profile.json
   ```
   It reports, per file: row count, columns, inferred types, null counts, distinct counts (capped), numeric min/max, sample values, timestamp-parse rate and monotonicity, and candidate identifier columns. It writes `MANIFEST.json` when `--manifest` is given. Only empty cells count as null by default. Pass `--null-tokens nan,null` to opt in to more. CSV/TSV/JSONL are handled with the stdlib. Parquet/XLSX are listed but not profiled unless `duckdb` is installed.
3. **Show the user** the per-file summary (columns, types, timestamp candidates, ID candidates, null-heavy columns) plus 5–10 sample rows. **Wait for approval** of your interpretation before designing anything downstream.
4. **Write `docs/dataset/README.md`** using the template below. Every claim must cite the profile or the Kaggle dataset description. Mark anything else as *unknown* or *assumption*.
5. **Choose the producer mode** with the decision table below, and record the choice and reason in the README.

## Producer-mode decision

| What the profile shows | Mode | Provenance label |
|---|---|---|
| A parseable timestamp column, with repeated rows per entity over time | **Historical replay**: emit rows in event-time order, optionally time-scaled | `replay` |
| Timestamps present but coarse/sparse (e.g. daily, monthly) | Replay at native grain. Synthetic densification only if the user asks | `replay` (+ `synthetic` for any generated points) |
| No timestamps (snapshot/cross-sectional, e.g. per-facility attributes) | **Simulator** seeded from the dataset's distributions/attributes | `synthetic` |
| Mixed: some files time-series, some reference/dimension | Replay the time-series files; load reference files as dimension tables (batch, not Kafka) | `replay` / `dataset` |

`dataset` = a value loaded as-is from the files in batch (e.g. dimension tables). `replay` = an actual dataset value re-emitted through Kafka. `synthetic` = generated. These labels are defined in `kafka-streaming` and must never be mixed up.

## `docs/dataset/README.md` template

```markdown
# Hyperscale Data Center Dataset: Notes
Source: https://www.kaggle.com/datasets/deeplumiere/hyperscale-data-center-dataset
Version/downloaded: <from MANIFEST.json>   License: <from Kaggle page>

## Files
| File | Rows | Grain (one row = ?) | Role (time-series / reference / labels / unknown) |

## Columns (per file)
| Column | Inferred type | Unit (source of unit: column name / docs / unknown) | Nulls % | Notes |

## Time
Timestamp column(s): ... | Format: ... | Timezone: stated / unknown | Frequency: ... | Range: ...

## Identifiers
Entity columns (facility / rack / device / ...): only what exists.

## Labels / events
Failure labels, incident or maintenance events: present (which columns) / ABSENT.

## Signals NOT available
Explicit list of operational signals that people may expect but the dataset lacks.

## Decision
Producer mode: replay | simulator | mixed, because ...
```

## Failure modes

| Failure | Response |
|---|---|
| Kaggle auth missing (CLI path) | Stop and ask the user to download manually or configure credentials |
| Checksum differs from manifest | The dataset changed. Re-profile and treat it as a possible schema change |
| Timestamp parse rate < 100% | Report the unparseable examples. Do not drop rows silently. Decide the policy with the user |
| Units not stated | Record unit as `unknown`. Do not guess from typical data center values |
| Very large files | Profiler streams rows. Use `--max-rows` for a quick pass, then a full pass before contracts are final |
