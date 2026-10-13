"""Structured JSON logging to console and logs/phase1.jsonl."""

import json
import logging
import os
import sys
from datetime import datetime, timezone

from data_engineering.paths import LOG_DIR

_configured = False


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }
        context = getattr(record, "context", None)
        if context:
            entry["context"] = context
        if record.exc_info:
            entry["error_type"] = record.exc_info[0].__name__
            entry["traceback"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def get_logger(component: str) -> logging.Logger:
    global _configured
    if not _configured:
        root = logging.getLogger("dco")
        level = os.environ.get("LOG_LEVEL", "INFO").upper()
        root.setLevel(level if level in {"DEBUG", "INFO", "WARNING", "ERROR"} else "INFO")
        formatter = JsonFormatter()
        console = logging.StreamHandler(sys.stderr)
        console.setFormatter(formatter)
        root.addHandler(console)
        try:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(LOG_DIR / "phase1.jsonl", encoding="utf-8")
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)
        except OSError as exc:
            root.warning("file logging unavailable; console only", extra={"context": {"error": str(exc)}})
        _configured = True
    return logging.getLogger(f"dco.{component}")
