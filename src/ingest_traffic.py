"""ingest_traffic.py — NDW traffic ingestion for the four A27/Breda measurement sites.

Streams the live NDW DATEX II v3 feed (``snelheden_en_intensiteiten_meetgegevens.xml.gz``,
~1 MB gzipped / ~70 MB raw, one per-minute snapshot of every loop detector in the Netherlands),
extracts the four sites at hectometre 63 of the A27, and:

  * appends the raw parsed reading to object storage as ``ndw/YYYY-MM-DD/HH-{site}.csv``
    (the date/hour come from the measurement period in the feed, not from the fetch time);
  * writes a clean row to ``traffic_readings`` in PostgreSQL — unless the site reported
    ``speed=-1``, the NDW sentinel for "no vehicles / detector fault", in which case the row is
    logged as a structured DATA_QUALITY_ERROR, counted, and *not* written to the database.

Why both the database and the bucket (required comment, Day 1 Lab 2):
    They answer different questions. The database answers "what is the latest / hourly
    intensity at site X?" in milliseconds with an index on (site_id, timestamp): that is what
    the dashboard and the training-set builder need. The bucket keeps every raw reading exactly
    as parsed, including the ``speed=-1`` rows the database refuses, forever, for ~€0.02/GB:
    that is the audit trail. Six months from now, when we retrain the model, we may want a
    different cleaning rule (keep zero-flow rows? treat -1 as 0 km/h?) or a different hourly
    aggregation. The database only has the rows that survived *today's* rule; the bucket lets
    us replay history under tomorrow's rule. Neither store can do the other's job: S3 cannot
    serve an indexed point query, and Postgres is the wrong (expensive, mutable) place to keep
    an immutable raw archive.

Polling interval (required comment, Day 2):
    This runs every 5 minutes. NDW publishes a new per-minute snapshot each minute, so a
    1-minute poll would be *possible*, but it would mean downloading and parsing ~70 MB of XML
    continuously on a 1 GB t3.micro (20–30 % CPU, permanently), 12× the S3 PUTs, and 12× the
    rows — all of them per-minute averages that the model aggregates back to hourly anyway.
    The course default of hourly polling is too coarse the other way: one minute-snapshot per
    hour is a very noisy estimate of that hour's traffic, and it would take weeks to build a
    usable training set. Twelve snapshots per hour averaged is a far better hourly estimate and
    fills the training table 12× faster during the five course days.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import logging
import sys
import time
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import boto3
import requests
from botocore.exceptions import ClientError

from db import record_bad_data, record_failure, record_success, transaction
from observability import get_logger, log_event
from settings import ConfigError, Settings
from sites import NDW_ID_TO_LABEL, SITES

SOURCE = "ndw"
FEED_URL = "https://opendata.ndw.nu/snelheden_en_intensiteiten_meetgegevens.xml.gz"
REQUEST_TIMEOUT_S = 60
MAX_ATTEMPTS = 3
BAD_SPEED_SENTINEL = -1.0

NS = {
    "roa": "http://datex2.eu/schema/3/roadTrafficData",
    "com": "http://datex2.eu/schema/3/common",
}
XSI_TYPE = "{http://www.w3.org/2001/XMLSchema-instance}type"
SITE_MEASUREMENTS_TAG = f"{{{NS['roa']}}}siteMeasurements"

CSV_COLUMNS = ["timestamp", "site_id", "ndw_site_id", "intensity_veh_per_hr", "speed_kmh",
               "lanes", "lane_flows", "lane_speeds", "bad_data"]

log = get_logger("ingest_traffic")


@dataclass
class SiteReading:
    site_id: str                 # hrl | hrr | vwd | vwa
    ndw_site_id: str
    timestamp: datetime          # measurement period (UTC) from the feed
    lane_flows: list[int] = field(default_factory=list)      # veh/h per lane
    lane_speeds: list[float] = field(default_factory=list)   # km/h per lane, -1 = sentinel

    @property
    def intensity_veh_per_hr(self) -> int:
        return int(sum(self.lane_flows))

    @property
    def speed_kmh(self) -> float | None:
        """Flow-weighted mean speed over lanes that carried traffic and reported a real speed."""
        pairs = [(f, s) for f, s in zip(self.lane_flows, self.lane_speeds) if f > 0 and s >= 0]
        if not pairs:
            return None
        return round(sum(f * s for f, s in pairs) / sum(f for f, _ in pairs), 1)

    @property
    def has_bad_speed(self) -> bool:
        return any(s == BAD_SPEED_SENTINEL for s in self.lane_speeds)

    @property
    def lanes(self) -> int:
        return len(self.lane_flows)

    def csv_row(self) -> dict[str, str]:
        return {
            "timestamp": self.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "site_id": self.site_id,
            "ndw_site_id": self.ndw_site_id,
            "intensity_veh_per_hr": str(self.intensity_veh_per_hr),
            "speed_kmh": "" if self.speed_kmh is None else str(self.speed_kmh),
            "lanes": str(self.lanes),
            "lane_flows": "|".join(str(f) for f in self.lane_flows),
            "lane_speeds": "|".join(str(s) for s in self.lane_speeds),
            "bad_data": "1" if self.has_bad_speed else "0",
        }


# --------------------------------------------------------------------------- download + parse
def fetch_feed(url: str = FEED_URL) -> requests.Response:
    """Open the gzipped feed as a streaming HTTP response (caller closes it). Retries transient
    network errors with backoff; raises after MAX_ATTEMPTS."""
    last_exc: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT_S)
            resp.raise_for_status()
            resp.raw.decode_content = False  # we gunzip ourselves
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            log_event(log, logging.WARNING, event="fetch_retry", source="NDW", attempt=attempt, error=str(exc))
            time.sleep(2 ** attempt)
    raise RuntimeError(f"NDW feed unreachable after {MAX_ATTEMPTS} attempts: {last_exc}")


def parse_site_measurements(elem: ET.Element) -> SiteReading | None:
    """Turn one <roa:siteMeasurements> element into a SiteReading, or None if it is not one of
    our four sites. Lane values arrive as alternating (TrafficFlow, TrafficSpeed) quantities."""
    ref = elem.find("roa:measurementSiteReference", NS)
    ndw_id = ref.get("id") if ref is not None else None
    if ndw_id not in NDW_ID_TO_LABEL:
        return None
    time_text = elem.findtext("roa:measurementTimeDefault/roa:timeValue", namespaces=NS)
    if not time_text:
        return None
    ts = datetime.fromisoformat(time_text.replace("Z", "+00:00")).astimezone(timezone.utc)

    reading = SiteReading(site_id=NDW_ID_TO_LABEL[ndw_id], ndw_site_id=ndw_id, timestamp=ts)
    for pq in elem.findall("roa:physicalQuantity", NS):
        basic = pq.find(".//roa:basicData", NS)
        if basic is None:
            continue
        kind = (basic.get(XSI_TYPE) or "").split(":")[-1]
        if kind == "TrafficFlow":
            txt = basic.findtext(".//com:vehicleFlowRate", namespaces=NS)
            if txt is not None:
                reading.lane_flows.append(int(float(txt)))
        elif kind == "TrafficSpeed":
            txt = basic.findtext(".//com:speed", namespaces=NS)
            if txt is not None:
                reading.lane_speeds.append(float(txt))
    return reading


def extract_readings(stream, wanted: set[str] | None = None) -> dict[str, SiteReading]:
    """Stream-parse the feed with iterparse and stop as soon as all wanted sites have been seen.
    Each <siteMeasurements> is cleared after inspection, so the parser never holds more than the
    (emptied) element skeleton; in practice the four sites appear early in the file and the run
    reads only a fraction of it (~2 s end to end on a t3.micro)."""
    wanted = set(wanted or SITES)
    found: dict[str, SiteReading] = {}
    for _event, elem in ET.iterparse(stream, events=("end",)):
        if elem.tag != SITE_MEASUREMENTS_TAG:
            continue
        reading = parse_site_measurements(elem)
        elem.clear()
        if reading is not None:
            found[reading.site_id] = reading
            if wanted <= found.keys():
                break
    return found


# --------------------------------------------------------------------------- object storage
def s3_key_for(reading: SiteReading) -> str:
    return f"ndw/{reading.timestamp:%Y-%m-%d}/{reading.timestamp:%H}-{reading.site_id}.csv"


def append_to_s3_csv(s3, bucket: str, reading: SiteReading) -> str:
    """Append the reading to its hourly CSV in the bucket (read-modify-write; the cron job is the
    only writer, enforced with flock). Idempotent on timestamp: re-running for the same minute
    does not duplicate the row. Returns the object key."""
    key = s3_key_for(reading)
    rows: list[dict[str, str]] = []
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        rows = list(csv.DictReader(io.StringIO(body)))
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("NoSuchKey", "404"):
            raise
    new_row = reading.csv_row()
    if any(r.get("timestamp") == new_row["timestamp"] for r in rows):
        return key
    rows.append(new_row)
    rows.sort(key=lambda r: r["timestamp"])
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
    return key


# --------------------------------------------------------------------------- database
def insert_traffic_reading(conn, reading: SiteReading) -> int:
    """Idempotent insert; returns 1 if the row was new, 0 if that (site, minute) already existed."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO traffic_readings (site_id, ndw_site_id, timestamp, intensity_veh_per_hr, speed_kmh, lanes)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (site_id, timestamp) DO NOTHING
            """,
            (reading.site_id, reading.ndw_site_id, reading.timestamp,
             reading.intensity_veh_per_hr, reading.speed_kmh, reading.lanes),
        )
        return cur.rowcount


def process_reading(conn, s3, bucket: str | None, reading: SiteReading) -> dict:
    """Apply the data-quality rule and persist. The raw row always goes to the bucket first; the
    database only gets rows without the speed=-1 sentinel. An object-storage failure is logged
    and does not stop the database write — the two stores fail independently."""
    key = None
    if s3 is not None and bucket:
        try:
            key = append_to_s3_csv(s3, bucket, reading)
        except Exception as exc:  # noqa: BLE001 — credentials, permissions, network
            log_event(log, logging.ERROR, event="s3_write_failed", source="NDW", site_id=reading.site_id,
                      key=s3_key_for(reading), error=str(exc))
    if reading.has_bad_speed:
        record_bad_data(conn, log, source="NDW", location=reading.ndw_site_id, field="speed",
                        value=BAD_SPEED_SENTINEL, reason="sentinel_speed_minus_one",
                        reading_ts=reading.timestamp, site_id=reading.site_id)
        return {"site_id": reading.site_id, "db_inserted": 0, "dropped": True, "s3_key": key}
    inserted = insert_traffic_reading(conn, reading)
    return {"site_id": reading.site_id, "db_inserted": inserted, "dropped": False, "s3_key": key}


# --------------------------------------------------------------------------- main
def _fail(settings: Settings | None, error: str, **fields) -> int:
    """Log a structured failure and, when we have a database, record it for /health."""
    log_event(log, logging.ERROR, event="fetch_failed", source="NDW", error=error, **fields)
    if settings is not None:
        try:
            with transaction(settings) as conn:
                record_failure(conn, SOURCE, error)
        except Exception as exc:  # noqa: BLE001 — the database itself may be the problem
            log_event(log, logging.ERROR, event="status_write_failed", source="NDW", error=str(exc))
    return 1


def run(settings: Settings | None, dry_run: bool = False, skip_s3: bool = False) -> int:
    """One ingestion cycle. Returns a process exit code. ``settings`` may be None for a dry run."""
    started = datetime.now(timezone.utc)
    if not dry_run and not skip_s3 and not settings.s3_bucket:
        raise ConfigError("S3_BUCKET is required (or pass --skip-s3 to write to the database only)")

    try:
        with closing(fetch_feed()) as resp, gzip.GzipFile(fileobj=resp.raw) as stream:
            readings = extract_readings(stream)
    except Exception as exc:  # noqa: BLE001
        return _fail(settings, str(exc))

    missing = sorted(set(SITES) - set(readings))
    for site in missing:
        log_event(log, logging.WARNING, event="site_missing", source="NDW", site_id=site, ndw_site_id=SITES[site])
    if not readings:
        return _fail(settings, "none of the four sites present in feed")

    if dry_run:
        for r in readings.values():
            print(r.csv_row())
        return 0

    bucket = None if skip_s3 else settings.s3_bucket
    s3 = None if skip_s3 else boto3.client("s3", region_name=settings.aws_region)
    try:
        with transaction(settings) as conn:
            for reading in readings.values():
                outcome = process_reading(conn, s3, bucket, reading)
                log_event(log, logging.INFO, event="fetch_success", source="NDW",
                          location=reading.ndw_site_id, timestamp=reading.timestamp.isoformat(),
                          intensity_veh_per_hr=reading.intensity_veh_per_hr, speed_kmh=reading.speed_kmh,
                          lanes=reading.lanes, **outcome)
            record_success(conn, SOURCE, started)
    except Exception as exc:  # noqa: BLE001 — DB down, TLS, bad credentials …
        return _fail(settings, f"database write failed: {exc}")

    log_event(log, logging.INFO, event="run_complete", source="NDW", sites=sorted(readings),
              missing=missing, duration_s=round((datetime.now(timezone.utc) - started).total_seconds(), 2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="Download, parse and print; touch neither S3 nor the database.")
    parser.add_argument("--skip-s3", action="store_true", help="Write to the database only (local development without AWS credentials).")
    args = parser.parse_args(argv)
    settings = None if args.dry_run else Settings.from_env()
    return run(settings, dry_run=args.dry_run, skip_s3=args.skip_s3)


if __name__ == "__main__":
    sys.exit(main())
