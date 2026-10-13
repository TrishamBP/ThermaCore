"""Time-series preparation and statistics (pure functions, no database access).

Rules applied here:
- readings are sorted by event timestamp before anything else;
- rows sharing a timestamp are collapsed deterministically: the most recently ingested row wins
  (ties: the last one in input order); differing duplicates are counted as conflicts;
- missing readings (reading_status 'missing' or a NULL value) are excluded from statistics and
  never treated as 0;
- rates use the actual elapsed time between readings (least squares against minutes), not the
  number of rows, so gaps and irregular spacing do not distort them.
"""

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass(frozen=True)
class Reading:
    timestamp: datetime          # event time, timezone-aware UTC
    ingested_at: datetime        # wall-clock time the consumer stored it
    status: str                  # 'observed' | 'missing'
    values: dict                 # metric -> float | None


@dataclass(frozen=True)
class Gap:
    start: datetime              # last reading before the gap (or the window start)
    end: datetime                # first reading after the gap (or the evaluation time)
    seconds: float
    position: str                # 'leading' | 'interior' | 'trailing'


@dataclass
class PreparedSeries:
    readings: list[Reading]                  # sorted, one per timestamp
    expected_points: int
    present_points: int                      # distinct timestamps (observed or explicitly missing)
    observed_points: int                     # status 'observed'
    missing_readings: int                    # explicit 'missing' readings
    duplicate_timestamps: int                # rows dropped because their timestamp repeated
    conflicting_duplicates: int              # ... and whose content differed from the kept row
    gaps: list[Gap] = field(default_factory=list)
    irregular_intervals: int = 0             # intervals shorter than expected / tolerance
    median_interval_seconds: float | None = None

    @property
    def coverage(self) -> float:
        return min(1.0, self.observed_points / self.expected_points) if self.expected_points else 0.0

    @property
    def missing_fraction(self) -> float:
        return self.missing_readings / self.present_points if self.present_points else 0.0


def prepare_series(readings: list[Reading], window_start: datetime, window_end: datetime,
                   expected_interval_s: int, gap_tolerance_factor: float) -> PreparedSeries:
    """Readings with window_start < timestamp <= window_end, sorted, deduplicated, gap-checked."""
    in_window = [r for r in readings if window_start < r.timestamp <= window_end]
    ordered = sorted(enumerate(in_window), key=lambda ir: (ir[1].timestamp, ir[1].ingested_at, ir[0]))
    kept: list[Reading] = []
    duplicates = conflicts = 0
    for _, reading in ordered:
        if kept and kept[-1].timestamp == reading.timestamp:
            duplicates += 1
            if (kept[-1].status, kept[-1].values) != (reading.status, reading.values):
                conflicts += 1
            kept[-1] = reading                       # later ingested_at (or later in input) wins
        else:
            kept.append(reading)

    window_s = (window_end - window_start).total_seconds()
    tolerance = expected_interval_s * gap_tolerance_factor
    gaps, irregular = [], 0
    if kept:
        if (kept[0].timestamp - window_start).total_seconds() > tolerance:
            gaps.append(Gap(window_start, kept[0].timestamp,
                            (kept[0].timestamp - window_start).total_seconds(), "leading"))
        intervals = []
        for a, b in zip(kept, kept[1:]):
            seconds = (b.timestamp - a.timestamp).total_seconds()
            intervals.append(seconds)
            if seconds > tolerance:
                gaps.append(Gap(a.timestamp, b.timestamp, seconds, "interior"))
            elif seconds < expected_interval_s / gap_tolerance_factor:
                irregular += 1
        if (window_end - kept[-1].timestamp).total_seconds() > tolerance:
            gaps.append(Gap(kept[-1].timestamp, window_end,
                            (window_end - kept[-1].timestamp).total_seconds(), "trailing"))
        median = sorted(intervals)[len(intervals) // 2] if intervals else None
    else:
        median = None
        if window_s > 0:
            gaps.append(Gap(window_start, window_end, window_s, "leading"))

    observed = sum(1 for r in kept if r.status == "observed")
    return PreparedSeries(
        readings=kept, expected_points=int(window_s // expected_interval_s), present_points=len(kept),
        observed_points=observed, missing_readings=len(kept) - observed, duplicate_timestamps=duplicates,
        conflicting_duplicates=conflicts, gaps=gaps, irregular_intervals=irregular,
        median_interval_seconds=median)


@dataclass
class SeriesStats:
    observed_points: int
    null_points: int
    sufficient: bool                         # observed_points >= min_points
    latest: float | None = None
    latest_timestamp: datetime | None = None
    latest_age_seconds: float | None = None  # relative to the evaluation time (event time)
    first: float | None = None
    first_timestamp: datetime | None = None
    mean: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    std_dev: float | None = None
    change: float | None = None              # latest - first observed value
    elapsed_minutes: float | None = None     # between first and latest observed value
    trend_per_minute: float | None = None    # least squares over the window
    recent_rate_per_minute: float | None = None   # least squares over the recent sub-window
    direction: str | None = None             # 'increasing' | 'decreasing' | 'stable' | None (unknown)


def least_squares_slope(points: list[tuple[datetime, float]]) -> float | None:
    """Slope in units per minute against actual elapsed time; None if undefined."""
    if len(points) < 2:
        return None
    t0 = points[0][0]
    xs = [(t - t0).total_seconds() / 60.0 for t, _ in points]
    ys = [v for _, v in points]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    var = sum((x - mx) ** 2 for x in xs)
    if var == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / var


def series_stats(series: PreparedSeries, metric: str, evaluated_at: datetime, *, recent_minutes: int,
                 min_points: int, min_change: float | None) -> SeriesStats:
    points = [(r.timestamp, float(r.values[metric])) for r in series.readings
              if r.status == "observed" and r.values.get(metric) is not None
              and math.isfinite(float(r.values[metric]))]
    stats = SeriesStats(observed_points=len(points), null_points=series.present_points - len(points),
                        sufficient=len(points) >= min_points)
    if not points:
        return stats
    values = [v for _, v in points]
    stats.first_timestamp, stats.first = points[0]
    stats.latest_timestamp, stats.latest = points[-1]
    stats.latest_age_seconds = (evaluated_at - points[-1][0]).total_seconds()
    stats.mean = sum(values) / len(values)
    stats.minimum, stats.maximum = min(values), max(values)
    if len(values) >= 2:
        stats.std_dev = math.sqrt(sum((v - stats.mean) ** 2 for v in values) / len(values))
        stats.change = stats.latest - stats.first
        stats.elapsed_minutes = (stats.latest_timestamp - stats.first_timestamp).total_seconds() / 60.0
    if stats.sufficient:
        stats.trend_per_minute = least_squares_slope(points)
        recent_start = evaluated_at - timedelta(minutes=recent_minutes)
        recent = [p for p in points if p[0] > recent_start]
        if len(recent) >= 3:
            stats.recent_rate_per_minute = least_squares_slope(recent)
        if stats.trend_per_minute is not None and min_change is not None and stats.elapsed_minutes:
            projected = stats.trend_per_minute * stats.elapsed_minutes
            stats.direction = ("stable" if abs(projected) < min_change
                               else "increasing" if projected > 0 else "decreasing")
    return stats


def derived_delta(series: PreparedSeries, minuend: str, subtrahend: str, name: str) -> PreparedSeries:
    """A series of minuend - subtrahend, only where both were observed at the same timestamp."""
    readings = []
    for r in series.readings:
        a, b = r.values.get(minuend), r.values.get(subtrahend)
        # rounded to 6 decimals: inputs have <= 3, so this only removes float noise (5.143000000000001)
        value = None if r.status != "observed" or a is None or b is None else round(float(a) - float(b), 6)
        readings.append(Reading(r.timestamp, r.ingested_at, r.status, {name: value}))
    return PreparedSeries(readings, series.expected_points, series.present_points, series.observed_points,
                          series.missing_readings, series.duplicate_timestamps, series.conflicting_duplicates,
                          series.gaps, series.irregular_intervals, series.median_interval_seconds)
