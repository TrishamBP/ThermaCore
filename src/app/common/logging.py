"""Logging for the API: the project's structured JSON logger (data_engineering.logging_utils).

All components (pipeline, consumer, estimator, API) write the same JSON lines to the console
and logs/phase1.jsonl, so one log stream covers a request end to end.
"""

import logging

from data_engineering.logging_utils import get_logger as _get_logger


def get_logger(component: str) -> logging.Logger:
    """Logger named dco.api.<component>."""
    return _get_logger(f"api.{component}")
