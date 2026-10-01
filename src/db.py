"""Database access for the AirBreda services.

Two things live here:

* ``transaction()`` — one connection, one transaction, commit on success / rollback on error.
  The services are short cron jobs and a low-traffic API; a connection per unit of work is the
  right size, a pool would be ceremony.
* the *ingestion status* repository — the small set of queries behind ``/health``: last
  successful fetch per source, bad-data counters, and the DATA_QUALITY_ERROR event log. The
  ingestion containers are short-lived, so this table is the only place their state can survive.

Schema changes are versioned migrations in ``db/migrations`` (see ``migrate.py``); nothing here
creates tables.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import psycopg

from observability import log_event
from settings import Settings

BAD_DATA_THRESHOLD_PER_HOUR = 10


@contextmanager
def transaction(settings: Settings) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(settings.dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def source_key(source: str) -> str:
    """Canonical lowercase key for a source. Logs keep the human spelling from the course
    ("NDW", "Luchtmeetnet"); the tables use one key so that the bad-data counter and the
    last-successful-fetch timestamp always land on the same row."""
    return source.strip().lower()


# --------------------------------------------------------------------------- ingestion status
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
    **log_fields: Any,
) -> int:
    """Log a DATA_QUALITY_ERROR, persist it, bump the per-source counter and return the number
    of bad-data events for this source in the trailing hour. Crossing
    BAD_DATA_THRESHOLD_PER_HOUR emits a single ERROR-level BAD_DATA_THRESHOLD_EXCEEDED event —
    the alertable log line Day 2 asks for instead of a cloud alarm.

    ``reading_ts`` is the measurement timestamp of the offending reading. When given, the same
    (source, location, field, reading) is counted once no matter how many cron runs see it: a
    feed that stops updating must not inflate the counter every five minutes."""
    key = source_key(source)
    if reading_ts is not None:
        seen = conn.execute(
            "SELECT 1 FROM bad_data_events WHERE source=%s AND location=%s AND field=%s AND reading_ts=%s LIMIT 1",
            (key, location, field, reading_ts),
        ).fetchone()
        if seen:
            return bad_data_last_hour(conn, key)

    log_event(logger, logging.WARNING, event="DATA_QUALITY_ERROR", source=source, location=location,
              field=field, value=value, reason=reason,
              timestamp=reading_ts.isoformat() if reading_ts else None, **log_fields)
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
    """Per-source status in the shape the dashboard's /health returns."""
    rows = conn.execute(
        """
        SELECT s.source, s.last_successful_fetch, s.last_attempt, s.last_error, s.bad_data_count,
               (SELECT count(*) FROM bad_data_events e
                 WHERE e.source = s.source AND e.detected_at >= now() - interval '24 hours') AS bad_last_24h
        FROM ingestion_status s
        """
    ).fetchall()
    return {
        source: {
            "last_successful_fetch": last_ok.isoformat() if last_ok else None,
            "last_attempt": last_try.isoformat() if last_try else None,
            "last_error": last_err,
            "bad_data_count": bad,
            "bad_data_last_24h": bad_24h,
        }
        for source, last_ok, last_try, last_err, bad, bad_24h in rows
    }
