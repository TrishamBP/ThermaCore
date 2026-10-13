"""Recommendation-only cooling optimizer: candidates, constraints, objective, unavailable states."""

from datetime import timedelta

import pytest

from cooling_helpers import RUN, T0
from cooling_model_helpers import seed_world, settings
from data_engineering.cooling_model.optimizer import (
    evaluate_candidates,
    generate_candidates,
    recommend,
    select_setting,
)
from data_engineering.cooling_model.predictor import CoolingResponseModel
from data_engineering.cooling_model.training import train
from data_engineering.storage.sqlite_store import SqliteTelemetryStore


# ------------------------------------------------------------------ pure selection logic

def test_candidates_cover_bounds_and_include_current():
    assert generate_candidates(2550.0, 2000, 3000, 250) == [2000, 2250, 2500, 2550.0, 2750, 3000]
    assert generate_candidates(None, 2000, 2900, 400) == [2000, 2400, 2800, 2900]   # max always included
    assert 5000.0 not in generate_candidates(5000.0, 2000, 3000, 500)               # out-of-bounds current
    with pytest.raises(ValueError):
        generate_candidates(None, 3000, 2000, 100)


def linear_world(u):          # fake model: more fan -> cooler; at most two horizons
    return {5: 30.0 - 0.002 * (u - 2000), 10: 30.5 - 0.002 * (u - 2000)}


def test_temperature_constraint_and_supported_range():
    evs = evaluate_candidates([2000, 2500, 3000, 3500], 2500, linear_world, t_safe_c=29.0,
                              bounds=(2000, 3500), supported=(2000, 3000))
    by = {e.setting: e for e in evs}
    assert by[2000].satisfies_t_safe is False and not by[2000].feasible          # 30.5 > 29 at 10 min
    assert by[2500].satisfies_t_safe is False                                   # 29.5 at 10 min
    assert by[3000].feasible and by[3000].predicted_inlet_c == {"5": 28.0, "10": 28.5}
    assert by[3500].within_supported_range is False and by[3500].predicted_inlet_c == {}   # never evaluated
    assert any("unsupported counterfactual" in n for n in by[3500].notes)
    assert by[2500].is_current


def test_lowest_energy_feasible_candidate_is_selected():
    evs = evaluate_candidates([2000, 2500, 3000, 3500], 3500, linear_world, 29.6, (2000, 3500), (2000, 3500),
                              energy_fn=lambda u: u / 1000)
    choice, reason = select_setting(evs, 3500)
    assert choice.setting == 2500 and "lowest" in reason      # 2000 violates T_safe, 2500 is cheapest feasible


def test_ties_prefer_current_then_smallest_change():
    flat = lambda u: 1.0  # noqa: E731  (no substantiated energy difference)
    evs = evaluate_candidates([2000, 2500, 3000], 2500, linear_world, 40.0, (2000, 3000), (2000, 3000), flat)
    assert select_setting(evs, 2500)[0].setting == 2500
    evs = evaluate_candidates([2000, 2400, 3000], 2500, linear_world, 40.0, (2000, 3000), (2000, 3000), flat)
    choice, reason = select_setting(evs, 2500)
    assert choice.setting == 2400 and "smallest change" in reason


def test_infeasible_when_every_candidate_is_too_hot():
    evs = evaluate_candidates([2000, 3000], 2000, linear_world, 20.0, (2000, 3000), (2000, 3000), lambda u: u)
    choice, reason = select_setting(evs, 2000)
    assert choice is None and "no candidate" in reason and not any(e.feasible for e in evs)


def test_no_energy_objective_means_no_selection():
    evs = evaluate_candidates([2000, 3000], 2000, linear_world, 40.0, (2000, 3000), (2000, 3000), energy_fn=None)
    assert all(e.feasible and e.estimated_cooling_energy is None for e in evs)
    assert select_setting(evs, 2000)[0] is None


# ------------------------------------------------------------------ recommend() with trained models

@pytest.fixture(scope="module")
def models(tmp_path_factory):
    out = {}
    for kind in ("observational", "causal"):
        d = tmp_path_factory.mktemp(kind)
        store = SqliteTelemetryStore(d / "t.db")
        store.initialize()
        seed_world(store, kind)
        train(store, settings(d / "models"))
        out[kind] = (CoolingResponseModel.load(d / "models"), store)
    yield out
    for _, store in out.values():
        store.close()


AS_OF = T0 + timedelta(minutes=500)


def codes(result):
    return [r["code"] for r in result["reasons"]]


def test_shipped_config_on_forecasting_baseline_is_unavailable(models):
    model, store = models["observational"]
    r = recommend(model, store, RUN, "T-00", settings(store.path.parent).optimizer, AS_OF)
    assert r["status"] == "UNAVAILABLE" and r["recommended_setting"] is None and r["candidates"] == []
    assert codes(r) == ["RESPONSE_MODEL_NOT_VALIDATED", "ENERGY_OBJECTIVE_MISSING", "T_SAFE_NOT_CONFIGURED",
                        "ACTUATOR_BOUNDS_NOT_CONFIGURED"]
    assert r["current_setting"] is not None and len(r["forecast_under_current_conditions"]) == 2
    assert r["recommendation_only"] is True and r["actuation"].startswith("none")


def test_baseline_model_is_never_used_for_counterfactuals_even_when_configured(models):
    model, store = models["observational"]
    cfg = settings(store.path.parent, t_safe_c=40.0, setting_min=2000.0, setting_max=4000.0, search_step=500.0).optimizer
    r = recommend(model, store, RUN, "T-00", cfg, AS_OF)
    assert r["status"] == "UNAVAILABLE" and "RESPONSE_MODEL_NOT_VALIDATED" in codes(r) and r["candidates"] == []
    assert r["current_conditions_check"]["satisfied"] is True        # observed-condition check still reported


def test_validated_model_without_energy_objective_lists_feasible_candidates_only(models):
    model, store = models["causal"]
    cfg = settings(store.path.parent, t_safe_c=40.0, setting_min=2000.0, setting_max=4000.0, search_step=500.0).optimizer
    r = recommend(model, store, RUN, "T-00", cfg, AS_OF)
    assert r["status"] == "CONSTRAINTS_ONLY" and r["recommended_setting"] is None
    assert codes(r) == ["ENERGY_OBJECTIVE_MISSING"] and r["energy_objective"]["available"] is False
    assert any(c["feasible"] for c in r["candidates"])
    assert all(c["estimated_cooling_energy"] is None for c in r["candidates"])


def test_validated_model_counterfactuals_follow_the_learned_response(models):
    model, store = models["causal"]
    cfg = settings(store.path.parent, t_safe_c=40.0, setting_min=2000.0, setting_max=4000.0, search_step=1000.0).optimizer
    r = recommend(model, store, RUN, "T-00", cfg, AS_OF)
    evaluated = [c for c in r["candidates"] if c["predicted_inlet_c"]]
    assert len(evaluated) >= 2
    lo, hi = min(evaluated, key=lambda c: c["setting"]), max(evaluated, key=lambda c: c["setting"])
    assert hi["predicted_inlet_c"]["5"] < lo["predicted_inlet_c"]["5"]   # more fan -> cooler, as in the world


def test_infeasible_t_safe(models):
    model, store = models["causal"]
    cfg = settings(store.path.parent, t_safe_c=5.0, setting_min=2000.0, setting_max=4000.0, search_step=500.0).optimizer
    r = recommend(model, store, RUN, "T-00", cfg, AS_OF)
    assert r["status"] == "NO_FEASIBLE_SETTING" and r["recommended_setting"] is None
    assert r["current_conditions_check"]["satisfied"] is False


def test_out_of_support_candidates_are_rejected(models):
    model, store = models["causal"]
    cfg = settings(store.path.parent, t_safe_c=40.0, setting_min=500.0, setting_max=6000.0, search_step=500.0).optimizer
    r = recommend(model, store, RUN, "T-00", cfg, AS_OF)
    outside = [c for c in r["candidates"] if not c["within_supported_range"]]
    assert {500.0, 6000.0} <= {c["setting"] for c in outside}
    assert all(not c["feasible"] and c["predicted_inlet_c"] == {} for c in outside)


def test_server_power_is_not_accepted_as_cooling_power(models):
    model, store = models["causal"]
    cfg = settings(store.path.parent, t_safe_c=40.0, setting_min=2000.0, setting_max=4000.0, search_step=500.0,
                   measured_cooling_power_column="power_consumption_kw").optimizer
    r = recommend(model, store, RUN, "T-00", cfg, AS_OF)
    # A telemetry column is named, but no validated energy-response model exists: still no energy pick.
    assert r["status"] == "CONSTRAINTS_ONLY" and "ENERGY_RESPONSE_MODEL_MISSING" in codes(r)
    assert r["recommended_setting"] is None


def test_no_model(models):
    _, store = models["observational"]
    r = recommend(None, store, RUN, "T-00", settings(store.path.parent).optimizer)
    assert r["status"] == "UNAVAILABLE" and codes(r) == ["MODEL_NOT_TRAINED"]


def test_optimizer_has_no_actuation_path():
    import inspect

    import data_engineering.cooling_model.optimizer as opt
    source = inspect.getsource(opt)
    for forbidden in ("requests", "socket", "subprocess", "modbus", "bacnet", "Producer("):
        assert forbidden not in source
