"""Resolve (run, assets) for a request with the operator's asset scope applied. Shared by several domains."""

from app.common.security.auth import AssetScope
from app.telemetry.exceptions import AssetNotFoundError, RunNotFoundError


def resolve_run(store, run_id: str | None) -> str:
    run_id = run_id or store.latest_run_id()
    if run_id is None:
        raise RunNotFoundError("the telemetry store has no simulation runs")
    return run_id


def run_assets(store, run_id: str | None, scope: AssetScope, asset_id: str | None = None) -> tuple[str, dict[str, dict]]:
    """-> (run_id, {asset_id: asset row}) restricted to the scope (and to asset_id when given)."""
    scope.check(asset_id)
    run_id = resolve_run(store, run_id)
    rows = store.run_assets(run_id)
    if not rows:
        raise RunNotFoundError(f"simulation run {run_id!r} not found")
    assets = {a["asset_id"]: a for a in rows if scope.permits(a["asset_id"])}
    if asset_id is not None:
        if asset_id not in assets:
            raise AssetNotFoundError(f"asset {asset_id!r} not found in simulation run {run_id!r}")
        assets = {asset_id: assets[asset_id]}
    return run_id, assets
