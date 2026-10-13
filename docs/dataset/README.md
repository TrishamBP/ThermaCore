# Hyperscale Data Center Dataset: Notes

Source: https://www.kaggle.com/datasets/deeplumiere/hyperscale-data-center-dataset (slug `deeplumiere/hyperscale-data-center-dataset`)
License: MIT (reported by the Kaggle CLI at download). Downloaded 2026-10-13. Checksums: `data/raw/hyperscale-data-center-dataset/MANIFEST.json`.
Machine profile: `docs/dataset/profile.json` (from `.claude/skills/dataset-ingestion/scripts/profile_dataset.py`).

Everything below comes from the downloaded files. Inferences are marked **(inferred)** and assumptions **(assumption)**.

## Files

| File | Size | Content |
|---|---|---|
| `green_ai_datacenter.csv` | 21.3 MB | **100,000 rows x 35 columns**. The only record-level data |
| `dataset_metadata.json` | 1.7 KB | Name "Enhanced Green AI Data Center Telemetry", row/column counts, pandas dtypes per column, description |
| `dataset_summary.txt` | 8.3 KB | `describe()` output (counts, unique, mean, std, quartiles) |
| `correlation_matrix.csv` | 15.5 KB | Pearson correlations between the 27 numeric columns |

The metadata's own description: *"Engineered synthetic dataset optimized for KNIME/Tableau ML experiments (Classification, Regression, Clustering)."* **The source rows are themselves synthetic.** In this project they are labeled `provenance = 'dataset'`, meaning *as downloaded*, not *real measurements*.

## Grain

One row = one independent record describing a server's configuration and operating snapshot. There is **no timestamp, no server/rack/device identifier, and no column unique per row**. Rows cannot be linked over time. 100,000 rows are 100,000 independent samples, not 100,000 chronological observations.

## Columns

Null counts: **0 empty cells in every column**. Units are **(inferred)** from column-name suffixes. The dataset does not document units.

| Group | Columns (dtype) | Observed range / values |
|---|---|---|
| Categorical | `server_type` (Compute, GPU, Storage, Edge), `cooling_type` (Air, Liquid, Hybrid), `datacenter_region` (APAC, EU, ME, NA), `time_of_day` (Night, Off-Peak, Peak), `maintenance_status` (Normal, Recent, Pending), `energy_source_primary` (Grid, Solar, Wind, Hydro) | — |
| Asset attributes | `server_age_years` int, `rack_u_position` int, `virtual_machines_count` int | 1–11, 1–42, 0–99 |
| Utilization | `workload_intensity` (0.1–1.0, unitless), `cpu_utilization`, `gpu_utilization`, `memory_utilization`, `storage_utilization` (0–100, % **(inferred)**) | GPU utilization is 0 for every non-GPU server |
| I/O & network | `disk_io_iops`, `network_throughput_gbps`, `network_latency_ms` | 1,514–14,462; 1.3–75.1; 5–300 |
| Environment & cooling | `ambient_temperature_c`, `humidity_percentage`, `inlet_temperature_c`, `exhaust_temperature_c`, `fan_speed_rpm`, `airflow_cfm`, `cooling_efficiency` | −0.3–42.3 °C; 11–90 %; 18.3–24.9 °C; 20.1–60.0 °C; 1,500–5,462; 68–330; 0.835–1.0 |
| Power & efficiency | `power_supply_efficiency`, `ups_battery_health`, `power_consumption_kw`, `pue`, `wue` | 0.87–0.97; 40–100; 20–935 kW; 1.05–1.263; 0.1–2.5 |
| Carbon & cost | `carbon_intensity_gco2_kwh`, `renewable_energy_ratio`, `carbon_emission_kg`, `compute_cost_usd` | 118–642; 0–1; 0–254; 2.3–94.7 |
| Labels | `energy_efficiency_class` (Low, Medium, High), `failure_risk_level` (Low, Medium, High) | **present** |

## Time

- Timestamp columns: **none**.
- `time_of_day` is a 3-value categorical bucket, not a timestamp. Mean `workload_intensity` is 0.55 in all three buckets for NA rows, so the bucket carries no workload signal.

## Regions

| `datacenter_region` | Rows |
|---|---|
| EU | 37,972 |
| **NA** | **28,801** |
| APAC | 27,133 |
| ME | 6,094 |

- `NA` = North America **(inferred)** from the naming convention alongside APAC/EU/ME. The dataset does not define its codes.
- **Pitfall:** pandas' default `read_csv` (and many tools) turns the string `NA` into a null, which would silently drop every North American record. Project code reads with the `csv` module. With pandas, use `keep_default_na=False`.

## Strong relationships (whole dataset, |r| > 0.85)

`server_age_years`↔`power_supply_efficiency` −1.00, `fan_speed_rpm`↔`airflow_cfm` +0.995, `workload_intensity`↔`cpu_utilization` +0.97, `cooling_efficiency`↔`pue` −0.96, `server_age_years`↔`ups_battery_health` −0.93, `gpu_utilization`↔`power_consumption_kw` +0.92, `storage_utilization`↔`disk_io_iops` +0.86, `server_age_years`↔`inlet_temperature_c` +0.86. The near-perfect correlations reflect how the dataset was engineered.

## Signals NOT available

- Timestamps or any ordering. Per-asset time series.
- Equipment, server, rack, row or facility identifiers.
- Incident, failure or maintenance *events* (only static `maintenance_status` and `failure_risk_level` labels).
- Documented units, operating limits, or region code definitions.

## Decision (Phase 1)

**Producer mode: synthetic simulator** (`dataset-ingestion` decision table: "no timestamps → simulator"). Each simulated asset is seeded from one NA source row. All telemetry, asset IDs, missing slots and anomalies are generated and labeled `synthetic`. See `config/simulation.yaml` for parameters and assumptions.
