"""Fan over-speed screening by peer comparison (energy-waste indicator, RELATIVE ONLY: no kW).

Question answered: is this server's fan running faster than comparable servers under comparable
conditions? There is no measured cooling power, so the result is relative:

  excess_fraction        = fan / peer_median - 1                 (measured rpm vs peers)
  relative_fan_power     = (fan / peer_median) ** 3               (fan affinity law, dimensionless)

Comparable = other assets of the same run in the same peer group (default: same cooling_type AND
asset_type; liquid-cooled servers run ~40 % slower fans than air-cooled ones in this data), in windows
whose mean workload and mean inlet temperature are within the configured tolerances of the target
window. Windows are [end - window, end) blocks over the lookback, per asset (never mixed).

Status: ELEVATED (excess >= min_excess_fraction AND above the peers' 90th percentile), WITHIN_PEER_RANGE,
BELOW_PEERS, INSUFFICIENT_PEERS, INSUFFICIENT_DATA.

Limits of the method (also returned): correlation-level comparison, not a causal estimate; it cannot
say a lower fan speed is safe (no validated fan->inlet response); the affinity-law ratio assumes the same
fan model and system curve; peer differences may come from hardware, firmware/fan curves, sensors or
airflow restrictions. It flags something to investigate, never a setting to apply.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import numpy as np

from data_engineering.telemetry_query import dedupe, iso

LIMITATIONS = [
    "Relative indicator only: no cooling-power measurement exists, so no kW or energy savings are computed.",
    "Peer comparison shows association, not causation; it does not show that a lower fan speed is thermally safe "
    "(the fan->inlet response is not validated).",
    "The affinity-law power ratio assumes the same fan model and system curve as the peers.",
    "Higher fan speed than peers can come from hardware, fan-curve/firmware settings, sensor faults or airflow "
    "restrictions; investigate before any change.",
]


@dataclass(frozen=True)
class FanPeerConfig:
    lookback_minutes: int = 1440
    window_minutes: int = 15
    group_by: tuple[str, ...] = ("cooling_type", "asset_type")
    workload_tolerance: float = 0.10           # absolute, workload_intensity is 0..1
    inlet_tolerance_c: float = 1.0
    min_reference_windows: int = 8
    min_reference_assets: int = 2              # other assets contributing comparable windows
    min_excess_fraction: float = 0.15
    reference_percentile: float = 90.0
    min_window_coverage: float = 0.8           # observed readings / expected readings per window
    max_rows: int = 60000


@dataclass
class Profile:
    asset_id: str
    group: tuple
    window_end: datetime
    workload: float
    inlet: float
    fan: float
    readings: int


@dataclass
class PeerResult:
    asset_id: str
    status: str
    group: dict
    window: dict | None = None
    asset_fan_rpm: float | None = None
    asset_workload: float | None = None
    asset_inlet_c: float | None = None
    peer_median_fan_rpm: float | None = None
    peer_p90_fan_rpm: float | None = None
    excess_fraction: float | None = None
    relative_fan_power: float | None = None
    reference_windows: int = 0
    reference_assets: list[str] = field(default_factory=list)
    persistence: dict | None = None
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = dict(self.__dict__)
        return {k: (round(v, 4) if isinstance(v, float) else v) for k, v in d.items()}


def profiles(rows: list[dict], assets: dict[str, dict], end: datetime, cfg: FanPeerConfig) -> list[Profile]:
    """Per-asset window means of workload, inlet and fan (only windows with enough observed readings)."""
    rows, _ = dedupe(rows)
    w = timedelta(minutes=cfg.window_minutes)
    need = cfg.window_minutes * cfg.min_window_coverage
    buckets: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        if r["reading_status"] != "observed" or r["asset_id"] not in assets:
            continue
        k = int((end - r["event_timestamp"]) / w - 1e-9)       # 0 = the window ending at `end`
        buckets[(r["asset_id"], k)].append(r)
    out = []
    for (aid, k), group in buckets.items():
        vals = [(g["workload_intensity"], g["inlet_temperature_c"], g["fan_speed_rpm"]) for g in group]
        vals = [v for v in vals if None not in v]
        if len(vals) < need:
            continue
        arr = np.asarray(vals, float)
        a = assets[aid]
        out.append(Profile(aid, tuple(a.get(g) for g in cfg.group_by), end - k * w, float(arr[:, 0].mean()),
                           float(arr[:, 1].mean()), float(arr[:, 2].mean()), len(vals)))
    return out


def compare(target: Profile, pool: list[Profile], cfg: FanPeerConfig, group_names: tuple[str, ...]) -> PeerResult:
    refs = [p for p in pool if p.asset_id != target.asset_id and p.group == target.group
            and abs(p.workload - target.workload) <= cfg.workload_tolerance
            and abs(p.inlet - target.inlet) <= cfg.inlet_tolerance_c]
    res = PeerResult(target.asset_id, "INSUFFICIENT_PEERS", dict(zip(group_names, target.group)),
                     {"start": iso(target.window_end - timedelta(minutes=cfg.window_minutes)),
                      "end": iso(target.window_end)},
                     asset_fan_rpm=target.fan, asset_workload=target.workload, asset_inlet_c=target.inlet,
                     reference_windows=len(refs), reference_assets=sorted({p.asset_id for p in refs}))
    if len(refs) < cfg.min_reference_windows or len(res.reference_assets) < cfg.min_reference_assets:
        res.reasons.append(f"{len(refs)} comparable peer windows from {len(res.reference_assets)} other asset(s) "
                           f"(needs >= {cfg.min_reference_windows} from >= {cfg.min_reference_assets}) in the same "
                           f"{'/'.join(group_names)} group at similar workload and inlet temperature")
        return res
    fans = np.array([p.fan for p in refs])
    median, p90 = float(np.median(fans)), float(np.percentile(fans, cfg.reference_percentile))
    res.peer_median_fan_rpm, res.peer_p90_fan_rpm = median, p90
    res.excess_fraction = target.fan / median - 1
    res.relative_fan_power = (target.fan / median) ** 3
    if res.excess_fraction >= cfg.min_excess_fraction and target.fan > p90:
        res.status = "ELEVATED"
        res.reasons.append(f"fan {res.excess_fraction:+.1%} vs peer median and above the peers' "
                           f"{cfg.reference_percentile:g}th percentile")
    elif target.fan < float(np.percentile(fans, 100 - cfg.reference_percentile)):
        res.status = "BELOW_PEERS"
    else:
        res.status = "WITHIN_PEER_RANGE"
    return res


def _load(store, run_id: str, as_of: datetime, cfg: FanPeerConfig):
    assets = {a["asset_id"]: a for a in store.run_assets(run_id)}
    start = as_of - timedelta(minutes=cfg.lookback_minutes)
    rows = store.run_readings_window(run_id, start, as_of, cfg.max_rows)
    truncated = len(rows) >= cfg.max_rows
    return assets, profiles(rows, assets, as_of, cfg), truncated


def analyze(store, run_id: str, as_of: datetime, cfg: FanPeerConfig, asset_ids: list[str] | None = None) -> dict:
    """Latest-window comparison for each asset (default: all), plus how often each was ELEVATED over the lookback."""
    assets, pool, truncated = _load(store, run_id, as_of, cfg)
    by_asset: dict[str, list[Profile]] = defaultdict(list)
    for p in pool:
        by_asset[p.asset_id].append(p)
    results = []
    for aid in sorted(asset_ids or assets):
        if aid not in assets:
            continue
        own = sorted(by_asset.get(aid, []), key=lambda p: p.window_end)
        latest = own[-1] if own and own[-1].window_end == as_of else None
        if latest is None:
            results.append(PeerResult(aid, "INSUFFICIENT_DATA",
                                      dict(zip(cfg.group_by, (assets[aid].get(g) for g in cfg.group_by))),
                                      reasons=["no window with enough observed readings ending at the analysis time"]))
            continue
        res = compare(latest, pool, cfg, cfg.group_by)
        history = [compare(p, pool, cfg, cfg.group_by).status for p in own]
        rated = [s for s in history if s not in ("INSUFFICIENT_PEERS", "INSUFFICIENT_DATA")]
        res.persistence = {"windows_compared": len(rated), "windows_elevated": rated.count("ELEVATED"),
                           "share_elevated": round(rated.count("ELEVATED") / len(rated), 3) if rated else None}
        results.append(res)
    return {"as_of": iso(as_of), "lookback_minutes": cfg.lookback_minutes, "window_minutes": cfg.window_minutes,
            "peer_group_by": list(cfg.group_by), "results": [r.to_dict() for r in results],
            "truncated": truncated, "method": __doc__.split("\n\n")[1].strip(), "limitations": LIMITATIONS}
