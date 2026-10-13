"""Forecast metrics and segment evaluation (pure numpy)."""

import numpy as np


def metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """MAE, RMSE (deg C) and R^2 on inlet temperature levels. Empty input -> nulls."""
    y_true, y_pred = np.asarray(y_true, float), np.asarray(y_pred, float)
    if len(y_true) == 0:
        return {"n": 0, "mae": None, "rmse": None, "r2": None}
    err = y_pred - y_true
    ss_tot = float(((y_true - y_true.mean()) ** 2).sum())
    return {"n": int(len(y_true)), "mae": float(np.abs(err).mean()), "rmse": float(np.sqrt((err ** 2).mean())),
            "r2": None if ss_tot == 0 else float(1 - (err ** 2).sum() / ss_tot)}


def _r2(y, p) -> float | None:
    ss_tot = float(((y - y.mean()) ** 2).sum()) if len(y) else 0.0
    return None if ss_tot == 0 else float(1 - ((p - y) ** 2).sum() / ss_tot)


def compare(y_true, model_pred, baseline_pred) -> dict:
    """Model vs persistence. R^2 on levels is dominated by differences BETWEEN assets, so the
    R^2 of the predicted CHANGE (relative to the latest inlet reading) is reported as well:
    persistence predicts no change and scores ~0; only real forecasting skill scores above 0."""
    y_true, model_pred, baseline_pred = (np.asarray(x, float) for x in (y_true, model_pred, baseline_pred))
    m, b = metrics(y_true, model_pred), metrics(y_true, baseline_pred)
    change = y_true - baseline_pred
    m["r2_of_change"] = _r2(change, model_pred - baseline_pred) if len(change) else None
    b["r2_of_change"] = _r2(change, np.zeros_like(change)) if len(change) else None
    skill = None
    if m["rmse"] is not None and b["rmse"]:
        skill = 1 - m["rmse"] / b["rmse"]          # > 0: better than persistence
    return {"model": m, "persistence": b, "rmse_skill_vs_persistence": skill}


def segment_report(y_true, model_pred, baseline_pred, labels: np.ndarray, min_rows: int = 30) -> dict:
    """compare() per segment label; segments with fewer than min_rows rows are reported as too small."""
    out = {}
    labels = np.asarray(labels)
    for label in sorted({str(x) for x in labels}):
        mask = labels.astype(str) == label
        if mask.sum() < min_rows:
            out[label] = {"n": int(mask.sum()), "note": f"fewer than {min_rows} rows; not evaluated"}
        else:
            out[label] = compare(y_true[mask], model_pred[mask], baseline_pred[mask])
    return out


def bin_labels(values: np.ndarray, edges: list[float], names: list[str]) -> np.ndarray:
    """Label each value by the bin it falls in (edges from the TRAINING split); NaN -> 'unknown'."""
    labels = np.full(len(values), "unknown", dtype=object)
    finite = np.isfinite(values)
    idx = np.searchsorted(np.asarray(edges), values[finite], side="right")
    labels[finite] = np.asarray(names, dtype=object)[idx]
    return labels
