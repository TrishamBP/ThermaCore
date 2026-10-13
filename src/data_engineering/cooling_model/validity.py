"""Thermal-response validity checks: can the data support "what if the setting were u"?

A forecaster can be accurate while knowing nothing about cooling: if fan speed and inlet
temperature both follow workload, any fan/temperature association is confounded. The check here
asks whether STEP CHANGES of a setting that are NOT explained by workload changes are followed by
inlet changes with the physically expected sign, consistently across assets and horizons:

    inlet(t+h) - inlet(t) ~ step_u(t) + [workload(t+h) - workload(t)] + inlet(t) + inlet rate(t) + 1   (per asset)

where step_u is the residual of u(t) - u(t-1) after regressing it on workload(t) - workload(t-1).
A step (not the level) is what a recommendation would apply, and the level's past effect is
already absorbed into inlet(t). The t-statistic uses Newey-West standard errors (overlapping
h-step targets and autocorrelated noise make plain OLS errors far too small). Only the TRAINING
split is used.

Passing this check is necessary, not sufficient, for causal claims; failing it means the model must
not be used for counterfactual evaluation of that setting.
"""

import numpy as np

from data_engineering.cooling_model.features import US_PER_MINUTE, Series


def _ols(X: np.ndarray, y: np.ndarray):
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    return beta, y - X @ beta


def newey_west_t(X: np.ndarray, y: np.ndarray, coef_index: int, lags: int) -> tuple[float, float]:
    """(coefficient, Newey-West t-statistic) of column coef_index."""
    beta, u = _ols(X, y)
    n = len(y)
    xtx_inv = np.linalg.pinv(X.T @ X)
    xu = X * u[:, None]
    s = xu.T @ xu
    for lag in range(1, min(lags, n - 1) + 1):
        w = 1 - lag / (lags + 1)
        g = xu[lag:].T @ xu[:-lag]
        s += w * (g + g.T)
    cov = xtx_inv @ s @ xtx_inv
    se = float(np.sqrt(max(cov[coef_index, coef_index], 0.0)))
    return float(beta[coef_index]), (float(beta[coef_index]) / se if se > 0 else 0.0)


def _future(series: Series, field: str, h: int) -> np.ndarray:
    keys = series.t_us + h * US_PER_MINUTE
    idx = np.searchsorted(series.t_us, keys)
    idx_c = np.clip(idx, 0, len(series) - 1)
    ok = (idx < len(series)) & (series.t_us[idx_c] == keys)
    out = np.full(len(series), np.nan)
    out[ok] = series.values[field][idx_c[ok]]
    return out


def setting_response(series_list: list[Series], setting: str, horizons, expected_sign: int,
                     min_independent_share: float, min_abs_t: float, min_asset_share: float,
                     min_rows: int = 60) -> dict:
    per_asset, shares = [], []
    for s in series_list:
        u, w, inlet = s.values[setting], s.values["workload_intensity"], s.values["inlet_temperature_c"]
        du, dw1 = u - _future(s, setting, -1), w - _future(s, "workload_intensity", -1)
        di1 = inlet - _future(s, "inlet_temperature_c", -1)
        ok = np.isfinite(du) & np.isfinite(dw1)
        if ok.sum() < min_rows or np.std(du[ok]) == 0:
            continue
        _, resid = _ols(np.c_[dw1[ok], np.ones(ok.sum())], du[ok])
        step = np.full(len(s), np.nan)
        step[ok] = resid
        share = float(np.std(resid) / np.std(du[ok]))
        shares.append(share)
        entry = {"asset_id": s.asset.get("asset_id"), "independent_variation_share": round(share, 4), "horizons": {}}
        for h in horizons:
            dy = _future(s, "inlet_temperature_c", h) - inlet
            dw = _future(s, "workload_intensity", h) - w
            rows = np.isfinite(dy) & np.isfinite(dw) & np.isfinite(step) & np.isfinite(inlet) & np.isfinite(di1)
            if rows.sum() < min_rows:
                continue
            X = np.c_[step[rows], dw[rows], inlet[rows], di1[rows], np.ones(rows.sum())]
            coef, t = newey_west_t(X, dy[rows], 0, lags=h + 10)
            entry["horizons"][str(h)] = {"coef_per_unit_step": coef, "nw_t": round(t, 2), "rows": int(rows.sum())}
        per_asset.append(entry)

    verdict, reasons = True, []
    median_share = float(np.median(shares)) if shares else 0.0
    if not per_asset:
        verdict = False
        reasons.append(f"{setting}: no asset has enough readings with step changes")
    if median_share < min_independent_share:
        verdict = False
        reasons.append(f"{setting}: step changes not explained by workload are only {median_share:.0%} of its changes "
                       f"(needs >= {min_independent_share:.0%}); its effect cannot be separated from workload")
    summary = {}
    for h in horizons:
        stats = [a["horizons"][str(h)] for a in per_asset if str(h) in a["horizons"]]
        good = [x for x in stats if np.sign(x["coef_per_unit_step"]) == expected_sign and abs(x["nw_t"]) >= min_abs_t]
        share_good = len(good) / len(stats) if stats else 0.0
        coefs = [x["coef_per_unit_step"] for x in stats]
        summary[str(h)] = {"assets": len(stats), "assets_expected_sign_and_significant": len(good),
                           "share": round(share_good, 3),
                           "median_coef_per_unit_step": float(np.median(coefs)) if coefs else None,
                           "assets_with_expected_sign": sum(1 for c in coefs if np.sign(c) == expected_sign)}
        if share_good < min_asset_share:
            verdict = False
            reasons.append(f"{setting} @ {h} min: independent step changes are followed by the expected inlet response "
                           f"(sign {expected_sign:+d}, |NW t| >= {min_abs_t}) in only {len(good)} of {len(stats)} "
                           f"assets (needs >= {min_asset_share:.0%})")
    return {"setting": setting, "validated": verdict, "median_independent_variation_share": round(median_share, 4),
            "by_horizon": summary, "reasons": reasons, "per_asset": per_asset}


def variable_profile(series_list: list[Series], fields) -> dict:
    """Within-asset variability per field; flags constant / near-constant / missing variables."""
    out = {}
    for f in fields:
        stds, distinct, present = [], [], 0
        for s in series_list:
            v = s.values[f][np.isfinite(s.values[f])]
            if len(v):
                present += 1
                stds.append(float(v.std()))
                distinct.append(len(np.unique(np.round(v, 6))))
        if not present:
            out[f] = {"status": "absent", "note": "no finite values (e.g. GPU on CPU-only servers)"}
            continue
        med_std, med_distinct = float(np.median(stds)), int(np.median(distinct))
        status = "constant" if med_distinct <= 1 else "near_constant" if med_distinct <= 5 or med_std < 1e-3 else "varies"
        out[f] = {"status": status, "assets_with_values": present, "median_within_asset_std": round(med_std, 5),
                  "median_distinct_values": med_distinct}
    return out
