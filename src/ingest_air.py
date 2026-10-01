"""ingest_air.py — Luchtmeetnet NO₂ ingestion for station NL10240 (Breda-Tilburgseweg).

Fetches hourly NO₂ readings from the live RIVM Luchtmeetnet open API, detects stale or null
values, and upserts everything into ``sensor_readings`` (PostgreSQL). Runs as a short-lived
container on an hourly cron on the AirBreda VM.

CAP trade-off of the sensor network (required comment, Day 1):
    The Luchtmeetnet network is an AP system. When a station loses connectivity the API still
    answers for that hour — availability wins — but the value may be null or a carried-over
    (stale) reading, i.e. the response is not guaranteed to be consistent with what the sensor
    actually measured. Nobody wants a public air-quality API that returns HTTP 503 whenever one
    field modem drops; the cost is that consumers must treat every reading as *possibly* stale.
    In a production pipeline a missing/null value must therefore be (a) detected, (b) written to
    the time series anyway so the gap is visible, and (c) flagged so downstream consumers (the
    dashboard, model training) can exclude or discount it. That is exactly what
    ``flag_stale_or_null`` + the ``is_flagged`` column do. Silently dropping the row would hide
    the outage; silently keeping it would pass a sensor fault off as a measurement. When the
    station later publishes the real value for that hour, the upsert heals the null in place.

Polling interval (required comment, Day 2):
    Luchtmeetnet publishes one validated value per hour, so polling more often than hourly
    only re-downloads the same 50 rows. The cron runs at :05 past the hour to give the API a
    few minutes to publish the previous hour. Each run fetches the last page of 50 readings
    (~2 days), so a missed cron run is self-healing on the next one — idempotent upserts make
    re-fetching free (at-least-once delivery + ``ON CONFLICT``).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

from db import record_bad_data, record_failure, record_success, transaction
from observability import get_logger, log_event
from settings import Settings
from sites import STATION_ID

COMPONENT = "NO2"
SOURCE = "luchtmeetnet"
API_URL = f"https://api.luchtmeetnet.nl/open_api/stations/{STATION_ID}/measurements"
REQUEST_TIMEOUT_S = 20
MAX_ATTEMPTS = 3
STALE_RUN_LENGTH = 3  # identical value for 3+ consecutive hourly timestamps → stale/interpolated
HOUR = timedelta(hours=1)

log = get_logger("ingest_air")


# --------------------------------------------------------------------------- fetch
def fetch_measurements(page: int = 1, start: datetime | None = None, end: datetime | None = None) -> list[dict]:
    """Call the live API for one page of NO₂ measurements. Retries transient failures with
    backoff; raises after MAX_ATTEMPTS so the cron run exits non-zero and /health shows it."""
    params: dict[str, str | int] = {"formula": COMPONENT, "page": page,
                                    "order_by": "timestamp_measured", "order_direction": "desc"}
    if start:
        params["start"] = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    if end:
        params["end"] = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    last_exc: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_S)
            resp.raise_for_status()
            return resp.json().get("data", [])
        except (requests.RequestException, ValueError) as exc:  # network, HTTP 5xx, bad JSON
            last_exc = exc
            log_event(log, logging.WARNING, event="fetch_retry", source="Luchtmeetnet",
                      attempt=attempt, error=str(exc))
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Luchtmeetnet unreachable after {MAX_ATTEMPTS} attempts: {last_exc}")


def fetch_window(backfill_hours: int, now: datetime) -> list[dict]:
    """Latest page, or — for a backfill — walk back through history in 50-row pages."""
    if not backfill_hours:
        return fetch_measurements(page=1)
    cutoff = now - timedelta(hours=backfill_hours)
    records: list[dict] = []
    page = 1
    while True:
        batch = fetch_measurements(page=page, start=cutoff, end=now)
        records.extend(batch)
        if not batch or len(batch) < 50:
            return records
        page += 1
        time.sleep(0.5)  # fair use: 100 requests / 5 min


def to_frame(records: list[dict]) -> pd.DataFrame:
    """API records → tidy frame with columns station_id, timestamp, component, value."""
    if not records:
        return pd.DataFrame(columns=["station_id", "timestamp", "component", "value"])
    df = pd.DataFrame(records).rename(columns={"formula": "component", "timestamp_measured": "timestamp"})
    df["station_id"] = STATION_ID
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df[["station_id", "timestamp", "component", "value"]]


# --------------------------------------------------------------------------- transform
def filter_no2_readings(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only NO₂ rows. Null values are kept on purpose: a null is a data-quality signal
    (see the CAP comment above), not noise to discard."""
    return df[df["component"] == COMPONENT].reset_index(drop=True)


def flag_stale_or_null(df: pd.DataFrame, previous: list[tuple[datetime, float | None]] | None = None) -> pd.DataFrame:
    """Add ``is_flagged``: True where value is null, or where the value is identical for
    STALE_RUN_LENGTH *consecutive hours* (a frozen sensor / interpolated gap). A gap in the
    timestamps resets the run — three equal values hours apart are a coincidence, not a freeze.

    ``previous`` holds the (timestamp, value) pairs immediately before the frame, oldest first,
    taken from the database, so a run that straddles two fetches is still caught."""
    df = df.sort_values("timestamp").reset_index(drop=True)
    run: list[tuple[datetime, float | None]] = list(previous or [])
    flags: list[bool] = []
    for ts, value in zip(df["timestamp"], df["value"]):
        ts = ts.to_pydatetime()
        value = None if pd.isna(value) else float(value)
        if run and ts - run[-1][0] != HOUR:
            run = []  # not consecutive → start a new run
        if value is None:
            flags.append(True)
        else:
            tail = run[-(STALE_RUN_LENGTH - 1):]
            frozen = len(tail) == STALE_RUN_LENGTH - 1 and all(v == value for _, v in tail)
            flags.append(frozen)
        run.append((ts, value))
    df["is_flagged"] = flags
    return df


# --------------------------------------------------------------------------- load
def previous_from_db(conn, before: datetime, n: int = STALE_RUN_LENGTH - 1) -> list[tuple[datetime, float | None]]:
    rows = conn.execute(
        "SELECT timestamp, value FROM sensor_readings WHERE station_id=%s AND component=%s AND timestamp < %s "
        "ORDER BY timestamp DESC LIMIT %s",
        (STATION_ID, COMPONENT, before, n),
    ).fetchall()
    return [(ts, val) for ts, val in reversed(rows)]


def upsert_readings(conn, df: pd.DataFrame) -> int:
    """Idempotent write on (station_id, timestamp, component). A re-fetched hour is a no-op,
    except in one case: a row first stored as NULL (station had no value yet) is *healed* when
    the API later publishes the value — the only update the table ever sees. Returns the number
    of rows inserted or healed."""
    written = 0
    with conn.cursor() as cur:
        for row in df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO sensor_readings (station_id, timestamp, component, value, is_flagged)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (station_id, timestamp, component) DO UPDATE
                    SET value = EXCLUDED.value, is_flagged = EXCLUDED.is_flagged
                    WHERE sensor_readings.value IS NULL AND EXCLUDED.value IS NOT NULL
                """,
                (row.station_id, row.timestamp.to_pydatetime(), row.component,
                 None if pd.isna(row.value) else float(row.value), bool(row.is_flagged)),
            )
            written += cur.rowcount
    return written


def existing_timestamps(conn, since: datetime) -> set[datetime]:
    return {ts for (ts,) in conn.execute(
        "SELECT timestamp FROM sensor_readings WHERE station_id=%s AND component=%s AND timestamp >= %s",
        (STATION_ID, COMPONENT, since),
    ).fetchall()}


# --------------------------------------------------------------------------- main
def _fail(settings: Settings | None, error: str, **fields) -> int:
    """Log a structured failure and, when we have a database, record it for /health."""
    log_event(log, logging.ERROR, event="fetch_failed", source="Luchtmeetnet", station_id=STATION_ID,
              error=error, **fields)
    if settings is not None:
        try:
            with transaction(settings) as conn:
                record_failure(conn, SOURCE, error)
        except Exception as exc:  # noqa: BLE001 — the database itself may be the problem
            log_event(log, logging.ERROR, event="status_write_failed", source="Luchtmeetnet", error=str(exc))
    return 1


def run(settings: Settings | None, backfill_hours: int = 0, dry_run: bool = False) -> int:
    """One ingestion cycle. Returns a process exit code. ``settings`` may be None for a dry run."""
    started = datetime.now(timezone.utc)
    try:
        records = fetch_window(backfill_hours, started)
    except Exception as exc:  # noqa: BLE001
        return _fail(settings, str(exc))

    df = filter_no2_readings(to_frame(records))
    if df.empty:
        return _fail(settings, "API returned no NO2 records")

    if dry_run:
        print(flag_stale_or_null(df).sort_values("timestamp", ascending=False).head(10).to_string(index=False))
        return 0

    try:
        with transaction(settings) as conn:
            first = df["timestamp"].min().to_pydatetime()
            df = flag_stale_or_null(df, previous=previous_from_db(conn, before=first))
            # Report bad data only for hours we see for the first time; re-fetching the same
            # stale hour on the next cron run must not count it again.
            new_rows = df[~df["timestamp"].isin(existing_timestamps(conn, since=first))]
            for row in new_rows[new_rows["is_flagged"]].itertuples(index=False):
                record_bad_data(conn, log, source="Luchtmeetnet", location=STATION_ID, field=COMPONENT,
                                value=None if pd.isna(row.value) else float(row.value),
                                reason="null" if pd.isna(row.value) else "stale_or_null",
                                reading_ts=row.timestamp.to_pydatetime(), station_id=STATION_ID)
            written = upsert_readings(conn, df)
            record_success(conn, SOURCE, started)
    except Exception as exc:  # noqa: BLE001 — DB down, TLS, bad credentials …
        return _fail(settings, f"database write failed: {exc}")

    latest = df.sort_values("timestamp").iloc[-1]
    log_event(log, logging.INFO, event="fetch_success", source="Luchtmeetnet", station_id=STATION_ID,
              component=COMPONENT, value=None if pd.isna(latest.value) else float(latest.value),
              timestamp=latest.timestamp.isoformat(), rows_fetched=len(df), rows_written=written,
              rows_flagged=int(df["is_flagged"].sum()),
              duration_s=round((datetime.now(timezone.utc) - started).total_seconds(), 2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--backfill-hours", type=int, default=0,
                        help="Also fetch this many hours of history (first run only).")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print; do not touch the database.")
    args = parser.parse_args(argv)
    settings = None if args.dry_run else Settings.from_env()
    return run(settings, backfill_hours=args.backfill_hours, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
