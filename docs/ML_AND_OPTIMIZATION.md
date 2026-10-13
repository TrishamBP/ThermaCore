# ML forecasting and cooling optimization

Code: `src/data_engineering/cooling_model/`. Config: `config/cooling_model.yaml` (override with
`COOLING_MODEL_CONFIG`). API: `src/app/cooling/`. Measured results of the active artifact:
[cooling_model_report.md](cooling_model_report.md). Every number in this document comes from that
artifact's `metadata.json`, on **simulated** data.

## Contents

1. [XGBoost inlet forecaster](#1-xgboost-inlet-forecaster)
2. [Cooling optimization and safety](#2-cooling-optimization-and-safety)

---

## 1. XGBoost inlet forecaster

### 1.1 Target and horizons

- **Target:** `y_h(t) = inlet(t + h) − inlet(t)` in °C, with one model per horizon, `h ∈ {5, 10}` minutes
  (`model.horizons_minutes`).
- **Prediction returned:** `inlet(t) + ŷ_h(t)`, next to the persistence forecast `inlet(t)`.
- **Why the change and not the level:** levels differ mostly *between* assets, and persistence already
  captures them. Modelling the change forces the model to add information beyond "same as now". It also
  makes R² on levels uninformative: both model and persistence score about 0.995. The honest score is the
  R² of the predicted change (`evaluation.compare: r2_of_change`).
- `build_targets` looks up `inlet(t + h)` by **exact timestamp**. If that reading is missing, the target
  is NaN and the row is unused for that horizon.

### 1.2 Features (`features.py`, 45 columns in the active artifact)

| Group | Config key | Columns |
|---|---|---|
| Current values at t | `features.current` | inlet, exhaust, `temperature_delta_c`, workload, CPU, GPU, memory, storage, fan speed, airflow, server power, PUE (12) |
| Lags (value exactly k min before t) | `lags_minutes.inlet_temperature_c: [1, 2, 3, 5, 10, 15]` | 6 |
| Rates `(x_t − x_{t−k}) / k` | `rates_minutes` | inlet 1/5/15, delta 5, workload 5, CPU 5, fan 1/5, airflow 5, power 5 (10) |
| Rolling over `(t − w, t]` | `rolling_minutes.inlet_temperature_c: [5, 15]` | mean/std/min/max × 2 (8) |
| Time of day | `time_of_day: true` | `hour_sin`, `hour_cos` of event-time hour (2) |
| One-hot | `categorical: [asset_type, cooling_type]` | 4 server types + 3 cooling types (7), categories from the training split only |

**Leakage rules enforced in code:**

- Every feature at t uses only readings with timestamp ≤ t.
- Lags are found with `np.searchsorted` on microsecond timestamps and accepted only on an exact match.
  A gap gives NaN, never the value of a neighbouring row ("row offset" lags would silently shift across
  gaps).
- Missing readings are NaN (XGBoost's native missing handling), never 0.
- GPU utilization of non-GPU servers (a structural 0 in the telemetry) is set to NaN.
- Same-timestamp duplicates collapse to the latest ingested row, the same rule the estimator uses.

**Lookback:** `FeatureConfig.lookback_minutes` = the largest lag/rate/window = 15. Inference reads
`(as_of − 16 min, as_of]`.

### 1.3 Chronological split and purging

`training.py: split_masks`, on the event-time axis of all selected series together:

```
t0, t1 = min, max event time
train:      t < t0 + 0.70 (t1 − t0)  AND  t + 10 min < train_end        (purge targets crossing the boundary)
validation: train_end ≤ t < t0 + 0.85 (t1 − t0)  AND  t + 10 min < val_end
test:       t ≥ val_end
```

Active artifact: train until 16:47:18, validation until 20:23:09, 600 rows purged. Rows per horizon:
5 min 32,327 / 6,060 / 6,199; 10 min 32,297 / 6,063 / 6,051.

**Why never random.** Consecutive minutes of one asset are almost identical (AR(1) with φ = 0.97). In a
random split, nearly every test row has its neighbours in training, so the model is effectively tested on
interpolation and looks far better than it will be on the future. Purging removes rows whose 10-minute
target lies in the next split, because their *label* contains future information.

**Run selection** (`dataset.py: select_training_runs`): runs shorter than 60 minutes are skipped, and only
one run per simulator seed is kept (the longest, then the one with most assets), because same-seed runs
replay identical series. The active artifact used 4 runs (seeds 42, 7, 8, 99; 46,320 asset-minutes) and
skipped the duplicate seed-42 runs.

**Caveat of the shared time axis.** All runs start at 2026-01-01T00:00Z, so runs shorter than ~16.8 h fall
entirely into the training split. The test split then contains only the two 24-hour runs, and only the
hours 20:23–23:59. Test results therefore cover late-evening conditions of two seeds. A per-run
chronological split, or a held-out seed, would test generalization differently.

### 1.4 Model and training

`xgb.train` (native API) with `objective reg:squarederror`, `eta 0.05`, `max_depth 5`, `subsample 0.8`,
`colsample_bytree 0.8`, `min_child_weight 5`, `lambda 1.0`, `tree_method hist`, `seed 42`, `nthread 4`, up to
800 rounds with early stopping after 50 rounds without validation improvement. Best iterations in the
active artifact: 37 (5 min) and 19 (10 min). The models stop early because there is little to learn.
Training refuses to run with fewer than 2,000 training rows per horizon or an empty validation/test split.

### 1.5 Evaluation

`evaluation.py`: MAE and RMSE in °C, R² on levels, R² of the change, and **RMSE skill vs persistence**
`1 − RMSE_model / RMSE_persistence` (> 0 means better than "no change").

| Horizon | Model MAE / RMSE | Persistence MAE / RMSE | RMSE skill | R² of change | Assets where model MAE < persistence |
|---|---|---|---|---|---|
| 5 min | 0.0390 / 0.0500 | 0.0383 / 0.0480 | −4.0 % | −0.08 | 16 of 20 |
| 10 min | 0.0518 / 0.0656 | 0.0514 / 0.0647 | −1.4 % | −0.03 | 11 of 20 |

- **Pooled, the model is slightly worse than persistence.** On validation it was ahead by only +0.7 %.
  Segment reports (asset type, workload tercile, inlet tercile, injected anomaly) show small positive
  skill for GPU and Storage servers, the coolest tercile and high workload, and negative skill elsewhere.
  The injected-anomaly segment had fewer than 30 test rows and was not evaluated.
- **Why persistence is the right baseline.** For short horizons on a slowly varying, autocorrelated
  signal, "the next value equals the current value" is very hard to beat. Without it, an RMSE of 0.05 °C
  sounds excellent; with it, you see that it carries no information.
- **Why so little skill.** By construction, the simulator's inlet is a workload-driven level plus AR(1)
  noise. The noise innovations are unpredictable, and the level changes slowly.
- **Per-asset caveat.** `per_asset_mae` groups by `asset_id` only. Asset IDs repeat across runs, so it
  reports 20 groups for 30 test series.

### 1.6 Validity: forecasting is not intervention

A forecaster answers *"what will inlet be if the current operating policy continues?"* An optimizer needs
*"what would inlet be if I changed fan speed to u?"*. That is a causal, interventional question. In
observational data, fan speed and inlet both follow workload (confounding), so a model can rank
`fan_speed_rpm` high by gain (it does, at h=10) while knowing nothing about cooling. Shifting fan speed by
±100 rpm in the trained model changes predictions by about 1e-5 °C.

`validity.py: setting_response` is a necessary (not sufficient) check, run on the **training split only**,
per asset series:

```
step_u(t)   = residual of [u(t) − u(t−1)] regressed on [workload(t) − workload(t−1)]     (movement not explained by workload)
Δinlet_h(t) = β·step_u(t) + γ·[workload(t+h) − workload(t)] + δ·inlet(t) + κ·[inlet(t) − inlet(t−1)] + c
```

β's t-statistic uses **Newey-West** standard errors (lag h + 10), because overlapping h-step targets and
autocorrelated noise make plain OLS errors far too small. A setting is `validated` only if:

- it is a model feature;
- the median independent-variation share `std(step_u) / std(Δu)` is ≥ 0.2;
- at every horizon, β has the expected sign (−1: more fan cools the inlet) with |t| ≥ 3.0 in ≥ 80 % of
  asset series.

**Result:** about 97 % of fan movement is independent of workload, so the test has power. Yet **0 of 35**
series show a significant cooling response at either horizon, and the median coefficient is ≈ 0. This
matches the simulator, where inlet never depends on fan speed. On synthetic test data with a built-in fan
effect, the check recovers it (−0.00156 °C/rpm vs −0.00166 expected; `tests/test_cooling_model.py`).
Therefore the label is `forecasting_baseline`, and hypothetical settings are never fed into the model.

### 1.7 Artifacts, versioning and inference lifecycle

```
models/cooling_response/
├── ACTIVE                                   # name of the version in use (replaced atomically)
└── crm-<UTC timestamp>-<data fingerprint[:8]><config hash[:8]>/
    ├── model_h5.ubj, model_h10.ubj          # XGBoost native binary (no pickle)
    └── metadata.json                        # features, categories, feature ranges, params, xgboost version,
                                             # training data (runs used/skipped), split, evaluation, validity
```

- `artifacts.save_artifact` writes to `.tmp-<version>`, renames it, then replaces `ACTIVE` with
  `os.replace`, so a crashed training run never becomes active.
- `load_artifact` validates required metadata keys, the artifact schema version, the XGBoost **major**
  version, the presence of each horizon's file, and that each booster's feature count matches the
  metadata. Failures raise `InvalidModelArtifactError` → `MODEL_ARTIFACT_INVALID`.
- The API loads the active model **once** at startup (`ModelRegistry.load` in the lifespan). It never
  retrains on its own. `POST /cooling-model/train` (off by default) retrains under a lock and reloads.
- `/cooling/*` also refuses an artifact whose `feature_config` differs from the current YAML
  (`FEATURE_SCHEMA_MISMATCH`), or one older than `API_MODEL_MAX_AGE_DAYS` (`MODEL_STALE`).
- The artifacts are committed to the repository (about 1.2 MB), so a fresh clone can serve forecasts.
  Training is reproducible with a fixed seed. The version name includes the training time, so retraining
  creates a new directory.

### 1.8 Missing features at inference

Missing current values or lags stay NaN and XGBoost routes them down learned default branches. The
response marks `data_quality.status = DEGRADED` with notes. A missing **latest inlet** reading means no
forecast (`INSUFFICIENT_TELEMETRY` / 422), because the prediction is anchored on it. Inputs outside the
training [min, max] are listed in `extrapolated_features`, and recommendations using such a forecast
become `INCONCLUSIVE`.

### 1.9 Uncertainty

Only offline test RMSE/MAE per horizon is reported (`uncertainty.method` says so: "not a calibrated
prediction interval"). There is no per-prediction interval. Options, in order of effort: conformal
prediction intervals from validation residuals per horizon and segment; quantile objectives
(`reg:quantileerror`); ensembles over seeds. Each should be evaluated by empirical coverage on a
chronologically later period.

### 1.10 Retraining and drift (not implemented)

There is no drift monitor or retraining schedule. What would be needed: feature distribution monitoring
against `metadata.feature_ranges` (the extrapolation warning is a first, per-request version); rolling
skill vs persistence on newly arrived data (the target becomes observable h minutes later); retraining
triggered by sustained skill loss; and champion/challenger promotion through the `ACTIVE` pointer.

---

## 2. Cooling optimization and safety

### 2.1 Formulation (implemented in `optimizer.py`)

Decision variable: `u` = `optimizer.controllable_setting` = `fan_speed_rpm` for one asset.

```
minimize    E_cooling(u)
subject to  T̂_inlet(u, t + h) ≤ T_safe                     ∀ h ∈ trained horizons
            u_min ≤ u ≤ u_max                                (optimizer.setting_min / setting_max)
            q_0.01(u_train) ≤ u ≤ q_0.99(u_train)            (supported_range_quantiles: no extrapolated counterfactuals)
```

- **Candidates:** a grid from `u_min` to `u_max` at `search_step`, plus the current setting
  (`generate_candidates`), or operator-supplied values (`/cooling/optimize`).
- **Evaluation:** `counterfactual_features` replaces `u` and its own rate features in the latest feature
  vector. Other coupled inputs (airflow, which tracks fan speed) are **not** adjusted. That is one more
  reason this path requires a validated model.
- **Selection:** `select_setting` picks the lowest-energy feasible candidate. Ties go to the current
  setting, then the smallest change, then the lower setting.
- **Statuses:** `RECOMMENDATION_AVAILABLE`, `NO_FEASIBLE_SETTING`, `CONSTRAINTS_ONLY`, `UNAVAILABLE`.

### 2.2 Why it returns `UNAVAILABLE` today

| Reason code | Blocking? | Cause |
|---|---|---|
| `MODEL_NOT_TRAINED` | yes | No active artifact |
| `RESPONSE_MODEL_NOT_VALIDATED` | yes | `fan_speed_rpm` failed the validity check |
| `T_SAFE_NOT_CONFIGURED` | yes | `optimizer.t_safe_c: null` |
| `ACTUATOR_BOUNDS_NOT_CONFIGURED` | yes | `setting_min/max` (and `search_step` for the grid) `null` |
| `ENERGY_OBJECTIVE_MISSING` | no, but prevents a recommendation | `measured_cooling_power_column: null` |
| `ENERGY_COLUMN_NOT_IN_TELEMETRY` / `ENERGY_RESPONSE_MODEL_MISSING` | no, same | Column named but absent / present but no response model |

`energy_fn` is `None` in the code, so even with a validated model and configured constraints the result
is `CONSTRAINTS_ONLY`: feasible settings listed, none recommended. The tests run the full path on synthetic
data where fan speed does cool the inlet (`tests/test_cooling_optimizer.py`), so the logic is verified
even though real data never reaches it.

### 2.3 Why server power is not a proxy for cooling power

`power_consumption_kw` is per-server IT power, driven by compute load. Cooling power depends on fan affinity
laws (fan power ∝ speed³), CRAH/chiller efficiency, outdoor conditions and setpoints, none of which is in
the data. Worse, the two can move in opposite directions: lower fan speed → higher component temperature →
higher leakage → *higher* IT power, while cooling power falls. Optimizing IT power would reward the wrong
action. Any energy claim needs metered cooling power on the same timeline.

### 2.4 Uncertainty and safety margins

With an intervention-validated model, the constraint should be applied to an upper prediction bound, not the
point forecast: `T̂ + z·σ_h ≤ T_safe − margin`. Here σ_h would come from calibrated intervals (§1.9). This
is **not implemented**: the current check uses the point forecast. It is acceptable only because the path
cannot be reached on this data. The estimator separately applies the configured approach margin
(`inlet_approach_margin_c`, 2 °C reference value) for warnings.

### 2.5 Fail-closed design and human approval

- Every missing prerequisite is a named reason. Nothing is replaced by a "typical" value.
- `recommendation_only: true` is enforced by the config loader; every response says
  `"actuation": "none: ..."`.
- Recommendation types `COOLING_EFFICIENCY` and `ENERGY_WASTE` can only be `INCONCLUSIVE` on this data. A
  candidate setting reaches a recommendation only if the optimizer result is `RECOMMENDATION_AVAILABLE`,
  the candidate is feasible, in bounds and in support, and the model is `intervention_validated`
  (`validator.validate_candidates`).
- Auditability: each analysis stores the full execution trace; each recommendation stores its evidence,
  checked constraints and missing evidence; each human decision is an append-only row.

### 2.6 Prerequisites for closed-loop control

None of these exist. All would be needed, after a safety review:

1. **Measured cooling power** (fan, CRAH/CRAC, chiller) aligned with the telemetry, and a validated
   energy-response model.
2. **Causal response data:** approved, controlled step tests (small fan-speed steps under stable
   workload, randomized timing, held-out assets/periods), followed by an intervention-validated model and
   calibrated uncertainty.
3. **Site-verified limits:** T_safe from equipment specifications and operators, actuator bounds, rate
   limits, minimum airflow.
4. **A separate safety layer** outside the optimizer: hard limits enforced by the control system itself,
   watchdogs, automatic revert on stale telemetry or sensor disagreement, rate-of-change limits.
5. **Staged rollout:** shadow mode (recommend and compare) → human-approved single changes on one rack →
   bounded automatic adjustments with instant rollback. Each stage gated on measured outcomes.

### 2.7 Limits of optimizing from observational telemetry

Observational data reflects the policy that produced it. If fans were always set by the existing control
loop in response to temperature, the data cannot separate "fan causes cooling" from "temperature causes
fan". Settings never used historically have no support. And the optimum of a model trained on correlations
can sit exactly where the correlation breaks. That is why the code requires both the validity check and
the support-range constraint, and why passing the check would still not be sufficient without
controlled experiments.
