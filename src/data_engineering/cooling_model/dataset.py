"""Load training series from the telemetry store.

Run selection: the simulator is deterministic per seed, so two runs with the same seed replay
identical series (verified: the 2-asset seed-42 test runs equal the first minutes of the 20-asset
seed-42 run). Using both would put the same readings in training and test. Therefore exactly one
run per seed is used: the longest (then the one with most assets).
"""

from dataclasses import dataclass, field
from datetime import timedelta

import numpy as np

from data_engineering.cooling_model.features import Series, series_from_rows


@dataclass
class TrainingSeries:
    run_id: str
    series: Series
    injected_anomaly: np.ndarray        # bool per row: an injected anomaly label exists at that time


@dataclass
class RunSelection:
    used: list[dict] = field(default_factory=list)
    skipped: list[dict] = field(default_factory=list)


def select_training_runs(store, min_series_minutes: int, run_ids: list[str] | None = None) -> RunSelection:
    summary = store.runs_summary()
    if run_ids is not None:
        unknown = set(run_ids) - {r["simulation_run_id"] for r in summary}
        if unknown:
            raise LookupError(f"unknown simulation runs {sorted(unknown)}")
        summary = [r for r in summary if r["simulation_run_id"] in run_ids]
    selection = RunSelection()
    by_seed: dict = {}
    for r in summary:
        minutes = (r["last_event"] - r["first_event"]).total_seconds() / 60 + 1
        info = {"run_id": r["simulation_run_id"], "seed": r["seed"], "assets": r["assets"], "minutes": round(minutes, 1)}
        if minutes < min_series_minutes:
            selection.skipped.append({**info, "reason": f"shorter than {min_series_minutes} minutes"})
            continue
        by_seed.setdefault(r["seed"], []).append(info)
    for seed, runs in sorted(by_seed.items(), key=lambda kv: str(kv[0])):
        runs.sort(key=lambda x: (-x["minutes"], -x["assets"], x["run_id"]))
        selection.used.append(runs[0])
        selection.skipped += [{**x, "reason": f"same seed as {runs[0]['run_id']} (identical series)"} for x in runs[1:]]
    return selection


def load_run_series(store, run_id: str) -> list[TrainingSeries]:
    anomalies: dict[str, list[int]] = {}
    for a in store.anomaly_event_times(run_id):
        anomalies.setdefault(a["asset_id"], []).append(int(round(a["event_timestamp"].timestamp() * 1_000_000)))
    out = []
    for asset in store.run_assets(run_id):
        rows = store.readings_window(run_id, asset["asset_id"],
                                     asset["first_event_timestamp"] - timedelta(seconds=1),
                                     asset["last_event_timestamp"])
        series = series_from_rows(rows, asset)
        mask = np.isin(series.t_us, np.array(anomalies.get(asset["asset_id"], []), dtype=np.int64))
        out.append(TrainingSeries(run_id, series, mask))
    return out
