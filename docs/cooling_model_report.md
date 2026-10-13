# Cooling response model: what this dataset can and cannot establish

Artifact `crm-20261013T120438Z-1443761922fc8cc7` (models/cooling_response, committed; reproduce with
`uv run python -m data_engineering.cooling_model.training`). Every number below comes from that artifact's
`metadata.json`. **All data is simulated.**

## Data used
- Four runs, one per simulator seed (sim-c9944cc35156, sim-db28de0560f5, sim-037f565f10e4, sim-b85066662da9):
  46,320 asset-minutes. The duplicate seed-42 runs were skipped because they replay identical series.
- Chronological split on event time: train until 16:47:18, validation until 20:23:09, test after that.
  600 rows were purged because their 10-minute target crossed a boundary.
- The rows per split are about 32.3k train, 6.1k validation and 6.1k test, for each horizon.

## Forecast accuracy (test split; target = inlet change over h minutes)

| Horizon | Model MAE / RMSE (°C) | Persistence MAE / RMSE (°C) | RMSE skill | Assets where model MAE < persistence |
|---|---|---|---|---|
| 5 min  | 0.0390 / 0.0500 | 0.0383 / 0.0480 | −4.0 % | 16 of 20 |
| 10 min | 0.0518 / 0.0656 | 0.0514 / 0.0647 | −1.4 % | 11 of 20 |

- **Pooled, the model does not beat persistence on the test split.** On validation it is ahead by only +0.7 %.
- R² on levels (0.995) means nothing here. Persistence scores the same, because R² on levels mostly measures the
  differences between assets. The R² of the *predicted change* is −0.08 at 5 min and −0.03 at 10 min.
- The model has skill in some segments only:
  - Ahead of persistence: GPU and Storage servers, the coolest inlet tercile and high workload, all at about +0.5 to +1 %.
  - Behind persistence: Compute and Edge servers, medium workload and the warm tercile.
- The injected-anomaly segment of the test split has fewer than 30 rows, so it was not evaluated.
- Why so little skill: the simulator's inlet is an AR(1)-like process around a workload-driven level. Its
  minute-to-minute noise is unpredictable by construction, so there is little left for a model to learn.

## Can it predict the effect of changing fan speed? No.
The check behind this conclusion (`validity.py`):

- **Method.** For each asset, it asks whether *step changes* in fan speed that workload changes don't explain are
  followed by the expected inlet response (cooler with more fan) at 5 and 10 minutes. It uses Newey-West
  t-statistics on the training split only.
- **Real data.**
  - About 97 % of fan steps are independent of workload, so there is enough variation to test.
  - Yet **0 of 35 assets** show a significant cooling response at either horizon.
  - The median coefficient is about 0: +1.8e-5 °C/rpm at 5 min and +1.3e-6 at 10 min.
  - This matches the simulator code, where inlet temperature never depends on fan speed.
- **Check of the check.** On synthetic test data where fan speed does cool the inlet, it recovers the built-in
  effect: −0.00156 °C/rpm, against an expected −0.00166.
- **Gain is not causation.** `fan_speed_rpm` is among the top features by gain at h=10, yet shifting it by
  ±100 rpm changes the prediction by only about 1e-5 °C.

The model is therefore labelled **`forecasting_baseline`**. It forecasts under the conditions that were
observed. It is not used to evaluate hypothetical settings.

## What is missing for an energy optimizer

| Needed | Status |
|---|---|
| Measured cooling-system power (fans, CRAH/CRAC, chiller) | **Absent.** `power_consumption_kw` is per-server IT power and is not used as cooling power. |
| Cooling efficiency, setpoints, actuator commands | Absent from the schema |
| Safe inlet limit (T_safe) and actuator bounds for the optimizer | Not configured. Nothing is invented (`optimizer.*: null`). |
| An intervention-validated thermal response | Not supported by this data (see above) |

So `POST /api/v1/cooling-optimizer/recommend` returns `UNAVAILABLE` with the reasons
`RESPONSE_MODEL_NOT_VALIDATED`, `ENERGY_OBJECTIVE_MISSING`, `T_SAFE_NOT_CONFIGURED` and
`ACTUATOR_BOUNDS_NOT_CONFIGURED`. It still returns the forecast under the current conditions.

The optimizer logic is complete and tested: candidate grid, actuator bounds, the supported range, the T_safe
constraint, lowest-energy selection and tie-breaks. The tests run it on synthetic data where fan speed has a real
effect. Even then, its best status is `CONSTRAINTS_ONLY`: it lists the feasible settings without claiming an
energy optimum, because no energy-response model exists.

## What would be needed
1. Measured cooling power, at the server fans or the room/CRAH level, aligned to the same timeline.
2. Data in which settings change independently of workload. Ideally that means approved, controlled step
   tests, followed by validation that holds out whole assets or periods.
3. Site-specific T_safe and actuator bounds from the operators or the equipment specifications.

None of the results here are evidence of physical validity or production readiness.
