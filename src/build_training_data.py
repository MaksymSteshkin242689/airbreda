"""build_training_data.py — join accumulated NO₂ and traffic readings into one hourly table.

Reads
  * NO₂: every clean row ``ingest_air.py`` wrote to ``sensor_readings`` (station NL10240,
    ``is_flagged = FALSE``, non-null);
  * traffic: every hourly CSV ``ingest_traffic.py`` saved to the bucket under
    ``ndw/YYYY-MM-DD/HH-{site}.csv`` (the raw store — this is the moment the bucket pays for
    itself; ``--traffic-source db`` reads ``traffic_readings`` instead for a quick local run).

Alignment detail that matters: Luchtmeetnet's ``timestamp_measured`` is the *end* of the
measurement hour (the value stamped 18:00 covers 17:00–18:00). NDW snapshots are stamped with
their own minute. So the traffic for the hour starting 17:00 is joined to the NO₂ value stamped
18:00. Getting this off by one hour would shift the whole relationship by an hour and quietly
weaken the model.

Output: ``data/training_data.csv`` with one row per hour:
    hour_start_utc, no2_ug_m3, intensity_hrl, intensity_hrr, intensity_vwd, intensity_vwa,
    total_intensity_veh_per_hr, hour_of_day, snapshots, sites_present
"""
from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

import boto3
import pandas as pd

from common import db_conn, get_logger, log_event
from predict import hour_of_day_from

SITES = ["hrl", "hrr", "vwd", "vwa"]
OUT_PATH = Path("data/training_data.csv")
log = get_logger("build_training_data")


# --------------------------------------------------------------------------- sources
def load_no2(conn) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT timestamp, value FROM sensor_readings
        WHERE station_id = 'NL10240' AND component = 'NO2' AND value IS NOT NULL AND NOT is_flagged
        ORDER BY timestamp
        """
    ).fetchall()
    df = pd.DataFrame(rows, columns=["no2_period_end_utc", "no2_ug_m3"])
    df["no2_period_end_utc"] = pd.to_datetime(df["no2_period_end_utc"], utc=True)
    # the hour of traffic this reading describes starts one hour earlier
    df["hour_start_utc"] = df["no2_period_end_utc"] - pd.Timedelta(hours=1)
    return df[["hour_start_utc", "no2_ug_m3"]]


def load_traffic_from_s3(bucket: str, region: str) -> pd.DataFrame:
    """Download every ndw/*.csv in the bucket and concatenate the clean (bad_data = 0) rows."""
    s3 = boto3.client("s3", region_name=region)
    frames: list[pd.DataFrame] = []
    paginator = s3.get_paginator("list_objects_v2")
    n_files = 0
    for page in paginator.paginate(Bucket=bucket, Prefix="ndw/"):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".csv"):
                continue
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            frames.append(pd.read_csv(io.BytesIO(body)))
            n_files += 1
    log_event(log, 20, event="traffic_files_loaded", source="S3", bucket=bucket, files=n_files)
    if not frames:
        return pd.DataFrame(columns=["timestamp", "site_id", "intensity_veh_per_hr", "bad_data"])
    df = pd.concat(frames, ignore_index=True)
    df = df[df["bad_data"] == 0]
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df[["timestamp", "site_id", "intensity_veh_per_hr"]]


def load_traffic_from_db(conn) -> pd.DataFrame:
    rows = conn.execute("SELECT timestamp, site_id, intensity_veh_per_hr FROM traffic_readings").fetchall()
    df = pd.DataFrame(rows, columns=["timestamp", "site_id", "intensity_veh_per_hr"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


# --------------------------------------------------------------------------- transform
def hourly_traffic(minutes: pd.DataFrame) -> pd.DataFrame:
    """Per-minute site snapshots → one row per hour with mean intensity per site and the total.

    A site with no clean snapshot in an hour is filled with 0: the only way that happens with a
    5-minute poll is that every snapshot carried speed=-1, i.e. no vehicles (slip roads at
    night). Hours where either mainline direction is missing are dropped instead — a missing
    mainline is a detector or feed problem, not an empty motorway."""
    if minutes.empty:
        return pd.DataFrame(columns=["hour_start_utc", *[f"intensity_{s}" for s in SITES],
                                     "total_intensity_veh_per_hr", "snapshots", "sites_present"])
    m = minutes.copy()
    m["hour_start_utc"] = m["timestamp"].dt.floor("h")
    per_site = m.groupby(["hour_start_utc", "site_id"])["intensity_veh_per_hr"].mean().unstack("site_id")
    per_site = per_site.reindex(columns=SITES)
    snapshots = m.groupby("hour_start_utc")["timestamp"].nunique().rename("snapshots")
    sites_present = per_site.notna().sum(axis=1).rename("sites_present")

    ok = per_site["hrl"].notna() & per_site["hrr"].notna()
    per_site = per_site[ok].fillna(0.0)
    hourly = per_site.add_prefix("intensity_")
    hourly["total_intensity_veh_per_hr"] = per_site.sum(axis=1).round(0)
    hourly = hourly.join(snapshots).join(sites_present).reset_index()
    for s in SITES:
        hourly[f"intensity_{s}"] = hourly[f"intensity_{s}"].round(0)
    return hourly


def build(no2: pd.DataFrame, traffic_minutes: pd.DataFrame) -> pd.DataFrame:
    hourly = hourly_traffic(traffic_minutes)
    df = hourly.merge(no2, on="hour_start_utc", how="inner").sort_values("hour_start_utc")
    df["hour_of_day"] = df["hour_start_utc"].apply(hour_of_day_from)
    cols = ["hour_start_utc", "no2_ug_m3", *[f"intensity_{s}" for s in SITES],
            "total_intensity_veh_per_hr", "hour_of_day", "snapshots", "sites_present"]
    return df[cols].reset_index(drop=True)


# --------------------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--traffic-source", choices=["s3", "db"], default="s3")
    parser.add_argument("--out", type=Path, default=OUT_PATH)
    args = parser.parse_args(argv)

    with db_conn() as conn:
        no2 = load_no2(conn)
        if args.traffic_source == "db":
            traffic = load_traffic_from_db(conn)
        else:
            traffic = load_traffic_from_s3(os.environ["S3_BUCKET"], os.getenv("AWS_REGION", "eu-north-1"))

    df = build(no2, traffic)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    log_event(log, 20, event="training_data_built", rows=len(df), no2_rows=len(no2),
              traffic_minutes=len(traffic), out=str(args.out),
              first_hour=str(df["hour_start_utc"].min()) if len(df) else None,
              last_hour=str(df["hour_start_utc"].max()) if len(df) else None)
    print(df.to_string(index=False, max_rows=40))
    return 0


if __name__ == "__main__":
    sys.exit(main())
