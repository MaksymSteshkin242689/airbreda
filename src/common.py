"""Shared plumbing for the AirBreda services: structured logging, database access,
schema bootstrap and the ingestion-status bookkeeping behind /health.

Everything here is deliberately small. Three containers import it, so it must stay
dependency-light (stdlib + psycopg only).
"""
from __future__ import annotations

import json
import logging
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import psycopg

# Relative to the working directory: the project root locally, /app in the containers.
SCHEMA_PATH = Path(os.getenv("SCHEMA_PATH", "db/schema.sql"))
BAD_DATA_THRESHOLD_PER_HOUR = 10


# --------------------------------------------------------------------------- logging
class JsonFormatter(logging.Formatter):
    """Emit one JSON object per line. Each log call passes its payload as a dict in
    ``extra={"event_data": {...}}``; we merge it with level/time so every line is queryable
    by field in CloudWatch, Loki, or plain ``docker logs | jq``."""

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


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, level: int, **fields: Any) -> None:
    """``log_event(log, logging.INFO, event="fetch_success", source="NDW", ...)``"""
    logger.log(level, fields.get("event", ""), extra={"event_data": fields})


# --------------------------------------------------------------------------- database
def db_dsn() -> str:
    return (
        f"host={os.environ['DB_HOST']} port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('DB_NAME', 'airbreda')} user={os.environ['DB_USER']} "
        f"password={os.environ["DB_PASSWORD"]} connect_timeout=10 sslmode={os.getenv("DB_SSLMODE", "require")}"
    )


@contextmanager
def db_conn() -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(db_dsn())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_schema(conn: psycopg.Connection) -> None:
    """Apply schema.sql. Every statement is IF NOT EXISTS, so this is safe on every run."""
    conn.execute(SCHEMA_PATH.read_text())


# --------------------------------------------------------------------------- status / bad data
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def source_key(source: str) -> str:
    """Database key for a source. Logs use the human-readable spelling from the course
    ("NDW", "Luchtmeetnet"); the tables use one canonical lowercase key so that the bad-data
    counter and the last-successful-fetch timestamp always land on the same row."""
    return source.strip().lower()


def record_success(conn: psycopg.Connection, source: str, fetched_at: datetime | None = None) -> None:
    fetched_at = fetched_at or utcnow()
    conn.execute(
        """
        INSERT INTO ingestion_status (source, last_successful_fetch, last_attempt, last_error)
        VALUES (%s, %s, %s, NULL)
        ON CONFLICT (source) DO UPDATE
            SET last_successful_fetch = EXCLUDED.last_successful_fetch,
                last_attempt = EXCLUDED.last_attempt,
                last_error = NULL
        """,
        (source_key(source), fetched_at, fetched_at),
    )


def record_failure(conn: psycopg.Connection, source: str, error: str) -> None:
    conn.execute(
        """
        INSERT INTO ingestion_status (source, last_attempt, last_error)
        VALUES (%s, %s, %s)
        ON CONFLICT (source) DO UPDATE
            SET last_attempt = EXCLUDED.last_attempt, last_error = EXCLUDED.last_error
        """,
        (source_key(source), utcnow(), error[:2000]),
    )


def record_bad_data(
    conn: psycopg.Connection,
    logger: logging.Logger,
    *,
    source: str,
    location: str,
    field: str,
    value: Any,
    reason: str,
    reading_ts: datetime | None = None,
) -> int:
    """Log a DATA_QUALITY_ERROR, persist it, bump the per-source counter and return the number
    of bad-data events for this source in the trailing hour. Crossing
    BAD_DATA_THRESHOLD_PER_HOUR emits a single ERROR-level BAD_DATA_THRESHOLD_EXCEEDED event
    (the alertable log line Day 2 asks for instead of a cloud alarm).

    ``reading_ts`` is the measurement timestamp of the offending reading. When given, the same
    (source, location, field, reading) is counted once, no matter how many cron runs see it —
    a feed that stops updating must not inflate the counter every five minutes."""
    key = source_key(source)
    if reading_ts is not None:
        seen = conn.execute(
            "SELECT 1 FROM bad_data_events WHERE source=%s AND location=%s AND field=%s AND reading_ts=%s LIMIT 1",
            (key, location, field, reading_ts),
        ).fetchone()
        if seen:
            return bad_data_last_hour(conn, key)
    log_event(
        logger, logging.WARNING,
        event="DATA_QUALITY_ERROR", source=source, location=location,
        field=field, value=value, reason=reason,
    )
    conn.execute(
        "INSERT INTO bad_data_events (source, location, field, value, reason, reading_ts) VALUES (%s, %s, %s, %s, %s, %s)",
        (key, location, field, None if value is None else str(value), reason, reading_ts),
    )
    conn.execute(
        """
        INSERT INTO ingestion_status (source, bad_data_count) VALUES (%s, 1)
        ON CONFLICT (source) DO UPDATE SET bad_data_count = ingestion_status.bad_data_count + 1
        """,
        (key,),
    )
    last_hour = bad_data_last_hour(conn, key)
    if last_hour == BAD_DATA_THRESHOLD_PER_HOUR + 1:  # fire once, on the crossing
        log_event(logger, logging.ERROR, event="BAD_DATA_THRESHOLD_EXCEEDED", source=source, count=last_hour)
    return last_hour


def bad_data_last_hour(conn: psycopg.Connection, source: str) -> int:
    row = conn.execute(
        "SELECT count(*) FROM bad_data_events WHERE source = %s AND detected_at >= %s",
        (source_key(source), utcnow() - timedelta(hours=1)),
    ).fetchone()
    return int(row[0]) if row else 0


def read_status(conn: psycopg.Connection) -> dict[str, dict[str, Any]]:
    """Shape consumed by the dashboard's /health."""
    rows = conn.execute(
        "SELECT source, last_successful_fetch, last_attempt, last_error, bad_data_count FROM ingestion_status"
    ).fetchall()
    return {
        source: {
            "last_successful_fetch": last_ok.isoformat() if last_ok else None,
            "last_attempt": last_try.isoformat() if last_try else None,
            "last_error": last_err,
            "bad_data_count": bad,
        }
        for source, last_ok, last_try, last_err, bad in rows
    }
