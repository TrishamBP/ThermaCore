"""Telemetry domain constants."""

# asset_id / simulation_run_id as produced by the pipeline (e.g. SIM-NA-013, sim-c9944cc35156)
ID_PATTERN = r"^[A-Za-z0-9._:-]{1,64}$"
WINDOW_MINUTES_MIN = 1
WINDOW_MINUTES_MAX = 1440
