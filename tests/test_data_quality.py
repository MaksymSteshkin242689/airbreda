"""Integration tests for the two data-quality handlers (Day 2).

They run against a real PostgreSQL (the schema is created on the fly) so that the assertions
are about what actually lands in the tables. Point them at a throwaway database with the
usual DB_* environment variables; locally that is the dockerised Postgres from .env.local.
They are skipped automatically when no database is configured, so the unit tests still run
anywhere.
"""
import os
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

pytestmark = pytest.mark.skipif(
    "DB_HOST" not in os.environ, reason="set DB_HOST/DB_USER/DB_PASSWORD to run database integration tests"
)


@pytest.fixture
def conn():
    from db import transaction
    from settings import Settings

    with transaction(Settings.from_env()) as c:
        yield c
        c.rollback()  # never persist test rows


def _unique_ts():
    # far-future, unique timestamps so tests never collide with real ingested data
    return datetime(2099, 1, 1, tzinfo=timezone.utc) + timedelta(hours=uuid.uuid4().int % 100_000)


def test_null_luchtmeetnet_reading_is_written_flagged_not_dropped(conn):
    from ingest_air import flag_stale_or_null, upsert_readings

    ts = _unique_ts()
    df = pd.DataFrame({
        "station_id": ["NL10240"], "timestamp": [pd.Timestamp(ts)], "component": ["NO2"], "value": [None],
    })
    df = flag_stale_or_null(df)
    assert upsert_readings(conn, df) == 1

    row = conn.execute(
        "SELECT value, is_flagged FROM sensor_readings WHERE station_id='NL10240' AND component='NO2' AND timestamp=%s",
        (ts,),
    ).fetchone()
    assert row is not None, "null reading must be written, not dropped"
    assert row[0] is None
    assert row[1] is True


def test_stale_luchtmeetnet_run_is_written_flagged(conn):
    from ingest_air import flag_stale_or_null, upsert_readings

    base = _unique_ts()
    ts = [pd.Timestamp(base + timedelta(hours=i)) for i in range(3)]
    df = pd.DataFrame({"station_id": "NL10240", "timestamp": ts, "component": "NO2", "value": [17.0, 17.0, 17.0]})
    df = flag_stale_or_null(df)
    upsert_readings(conn, df)

    flags = [r[0] for r in conn.execute(
        "SELECT is_flagged FROM sensor_readings WHERE station_id='NL10240' AND timestamp = ANY(%s) ORDER BY timestamp",
        ([t.to_pydatetime() for t in ts],),
    ).fetchall()]
    assert flags == [False, False, True]


def test_ndw_speed_minus_one_is_not_written_and_increments_counter(conn):
    from db import bad_data_last_hour
    from ingest_traffic import SiteReading, process_reading

    before = conn.execute("SELECT COALESCE(bad_data_count, 0) FROM ingestion_status WHERE source='ndw'").fetchone()
    before_count = before[0] if before else 0
    before_hour = bad_data_last_hour(conn, "NDW")

    ts = _unique_ts()
    bad = SiteReading(site_id="hrl", ndw_site_id="RWS01_MONIBAS_0271hrl0063ra", timestamp=ts,
                      lane_flows=[0, 420], lane_speeds=[-1.0, 113.0])
    assert bad.has_bad_speed

    outcome = process_reading(conn, s3=None, bucket=None, reading=bad)
    assert outcome["dropped"] is True and outcome["db_inserted"] == 0

    assert conn.execute(
        "SELECT count(*) FROM traffic_readings WHERE site_id='hrl' AND timestamp=%s", (ts,)
    ).fetchone()[0] == 0, "speed=-1 row must not reach traffic_readings"

    after = conn.execute("SELECT bad_data_count FROM ingestion_status WHERE source='ndw'").fetchone()[0]
    assert after == before_count + 1
    assert bad_data_last_hour(conn, "NDW") == before_hour + 1


def test_ndw_clean_reading_is_written(conn):
    from ingest_traffic import SiteReading, process_reading

    ts = _unique_ts()
    good = SiteReading(site_id="vwa", ndw_site_id="RWS01_MONIBAS_0270vwa0063ra", timestamp=ts,
                       lane_flows=[60, 180], lane_speeds=[45.0, 41.0])
    assert not good.has_bad_speed
    assert good.intensity_veh_per_hr == 240
    assert good.speed_kmh == 42.0  # flow-weighted: (60*45 + 180*41) / 240

    outcome = process_reading(conn, s3=None, bucket=None, reading=good)
    assert outcome == {"site_id": "vwa", "db_inserted": 1, "dropped": False, "s3_key": None}
