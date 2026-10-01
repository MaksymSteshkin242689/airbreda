"""Structured JSON logging shared by every AirBreda service.

One JSON object per line on stdout. ``docker logs`` collects it, and any log backend
(CloudWatch Logs Insights, Loki, or ``jq`` on a laptop) can filter on ``event``, ``source``,
``site_id`` … without regex. The course's alert rule — "BAD_DATA_THRESHOLD_EXCEEDED appeared" —
is a one-line query against this output.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Merge the structured payload passed via ``extra={"event_data": {...}}`` with the
    standard level/time fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "level": record.levelname,
            "logger": record.name,
        }
        data = getattr(record, "event_data", None)
        if isinstance(data, dict):
            payload.update(data)
        else:
            payload["message"] = record.getMessage()
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, level: int, **fields: Any) -> None:
    """``log_event(log, logging.INFO, event="fetch_success", source="NDW", site_id="hrl")``"""
    logger.log(level, fields.get("event", ""), extra={"event_data": fields})
