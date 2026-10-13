"""Pure time-series functions: sorting, duplicates, gaps, missing values, elapsed-time rates."""

from datetime import datetime, timedelta, timezone

import pytest

from data_engineering.cooling.metrics import (
    Reading, derived_delta, least_squares_slope, prepare_series, series_stats)

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
ING = datetime(2026, 10, 1, tzinfo=timezone.utc)


def r(minute, value=None, status="observed", ingested=0, **values):
    vals = {"x": value, **values}
    return Reading(T0 + timedelta(minutes=minute), ING + timedelta(seconds=ingested),
                   status, vals if status == "observed" else {k: None for k in vals})


def prep(readings, start=-0.5, end=10, interval=60, tol=1.5):
    return prepare_series(readings, T0 + timedelta(minutes=start), T0 + timedelta(minutes=end), interval, tol)


def stats(s, end=10, min_points=3, min_change=0.5, recent=5):
    return series_stats(s, "x", T0 + timedelta(minutes=end), recent_minutes=recent, min_points=min_points,
                        min_change=min_change)


def test_readings_are_sorted_before_anything_else():
    s = prep([r(3, 3.0), r(1, 1.0), r(2, 2.0)], end=3)
    assert [x.values["x"] for x in s.readings] == [1.0, 2.0, 3.0]
    st = stats(s, end=3)
    assert (st.first, st.latest, st.change) == (1.0, 3.0, 2.0)


def test_rate_uses_elapsed_time_not_row_count():
    # value = 2 x minutes, but sampled irregularly: a row-count slope would be wrong
    s = prep([r(m, 2.0 * m) for m in (0, 1, 2, 6, 7, 10)], end=10)
    assert stats(s).trend_per_minute == pytest.approx(2.0)
    assert least_squares_slope([(T0, 1.0), (T0 + timedelta(seconds=30), 2.0)]) == pytest.approx(2.0)


def test_missing_values_are_excluded_not_zero():
    s = prep([r(0, 10.0), r(1, status="missing"), r(2, 20.0), r(3, None)], end=3)
    st = stats(s, end=3, min_points=2)
    assert (st.observed_points, st.null_points) == (2, 2)
    assert st.mean == 15.0 and st.minimum == 10.0                # a zero would have pulled these down
    assert (s.present_points, s.observed_points, s.missing_readings) == (4, 3, 1)


def test_duplicate_timestamps_keep_the_latest_ingested_row():
    s = prep([r(1, 5.0, ingested=1), r(1, 9.0, ingested=5), r(1, 7.0, ingested=3), r(2, 1.0)], end=2)
    assert len(s.readings) == 2 and s.readings[0].values["x"] == 9.0
    assert (s.duplicate_timestamps, s.conflicting_duplicates) == (2, 2)
    same = prep([r(1, 5.0), r(1, 5.0), r(2, 1.0)], end=2)
    assert (same.duplicate_timestamps, same.conflicting_duplicates) == (1, 0)


def test_gaps_are_found_from_actual_timestamps():
    s = prep([r(m, 1.0) for m in (2, 3, 4, 8, 9)], start=0, end=12)
    positions = [(g.position, g.seconds) for g in s.gaps]
    assert positions == [("leading", 120.0), ("interior", 240.0), ("trailing", 180.0)]
    assert s.expected_points == 12 and s.observed_points == 5
    assert s.coverage == pytest.approx(5 / 12)


def test_irregular_short_intervals_are_counted():
    s = prep([r(0, 1.0), r(1, 1.0), r(1.25, 1.0), r(2, 1.0)], start=-1, end=2)
    assert s.irregular_intervals == 1


def test_window_bounds_are_start_exclusive_end_inclusive():
    s = prep([r(m, float(m)) for m in range(0, 12)], start=0, end=10)
    assert [x.values["x"] for x in s.readings] == [float(m) for m in range(1, 11)]


def test_trend_direction_and_minimum_points():
    rising = prep([r(m, 0.2 * m) for m in range(11)])
    assert stats(rising).direction == "increasing"                # change 2.0 >= 0.5
    flat = prep([r(m, 1.0 + 0.01 * m) for m in range(11)])
    assert stats(flat).direction == "stable"                      # change 0.1 < 0.5
    falling = prep([r(m, 5 - 0.2 * m) for m in range(11)])
    assert stats(falling).direction == "decreasing"
    few = stats(prep([r(9, 1.0), r(10, 9.0)]), min_points=3)
    assert few.sufficient is False and few.trend_per_minute is None and few.direction is None
    assert few.latest == 9.0                                       # latest is still reported


def test_recent_rate_uses_only_the_recent_subwindow():
    values = [0.0] * 6 + [1.0, 2.0, 3.0, 4.0, 5.0]                # flat, then +1/min for the last 5 minutes
    st = stats(prep([r(m, v) for m, v in enumerate(values)]), recent=5)
    assert st.recent_rate_per_minute == pytest.approx(1.0)
    assert st.trend_per_minute < 1.0
    sparse = stats(prep([r(0, 0.0), r(1, 0.0), r(2, 0.0), r(9, 1.0), r(10, 2.0)]), recent=5)
    assert sparse.recent_rate_per_minute is None                  # < 3 points in the sub-window


def test_derived_delta_only_where_both_values_exist():
    readings = [r(0, None, a=30.0, b=20.0), r(1, None, a=31.0, b=None), r(2, status="missing", a=1, b=1)]
    d = derived_delta(prep(readings, end=2), "a", "b", "delta")
    assert [x.values["delta"] for x in d.readings] == [10.0, None, None]
