"""Unit tests for the NDW DATEX II parser and the S3 CSV append. No network, no database.
The fixture is a cut-down copy of the real feed structure (namespaces, xsi:type, lane pairing)."""
import gzip
import io
from datetime import datetime, timezone

import pytest

from ingest_traffic import (
    CSV_COLUMNS, SiteReading, append_to_s3_csv, extract_readings, parse_site_measurements, s3_key_for,
)

FEED = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<mc:messageContainer xmlns:mc="http://datex2.eu/schema/3/messageContainer" xmlns:roa="http://datex2.eu/schema/3/roadTrafficData" xmlns:com="http://datex2.eu/schema/3/common" modelBaseVersion="3">
<mc:payload xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="roa:MeasuredDataPublication" lang="nl" modelBaseVersion="3">
<com:publicationTime>2026-10-01T18:58:00Z</com:publicationTime>
<roa:siteMeasurements>
  <roa:measurementSiteReference targetClass="roa:MeasurementSite" id="GAD02_Amstd_31_0" version="19"/>
  <roa:physicalQuantity index="1"><roa:physicalQuantity xsi:type="roa:SinglePhysicalQuantity"><roa:basicData xsi:type="roa:TrafficFlow"><roa:vehicleFlow accuracy="0.0"><com:vehicleFlowRate>240</com:vehicleFlowRate></roa:vehicleFlow></roa:basicData></roa:physicalQuantity></roa:physicalQuantity>
  <roa:measurementTimeDefault><roa:timeValue>2026-10-01T18:55:00Z</roa:timeValue></roa:measurementTimeDefault>
</roa:siteMeasurements>
<roa:siteMeasurements>
  <roa:measurementSiteReference targetClass="roa:MeasurementSite" id="RWS01_MONIBAS_0271hrl0063ra" version="1738"/>
  <roa:physicalQuantity index="1"><roa:physicalQuantity xsi:type="roa:SinglePhysicalQuantity"><roa:basicData xsi:type="roa:TrafficFlow"><roa:vehicleFlow accuracy="0.0"><com:vehicleFlowRate>360</com:vehicleFlowRate></roa:vehicleFlow></roa:basicData></roa:physicalQuantity></roa:physicalQuantity>
  <roa:physicalQuantity index="2"><roa:physicalQuantity xsi:type="roa:SinglePhysicalQuantity"><roa:basicData xsi:type="roa:TrafficSpeed"><roa:averageVehicleSpeed accuracy="0.0"><com:speed>127.0</com:speed></roa:averageVehicleSpeed></roa:basicData></roa:physicalQuantity></roa:physicalQuantity>
  <roa:physicalQuantity index="3"><roa:physicalQuantity xsi:type="roa:SinglePhysicalQuantity"><roa:basicData xsi:type="roa:TrafficFlow"><roa:vehicleFlow accuracy="0.0"><com:vehicleFlowRate>600</com:vehicleFlowRate></roa:vehicleFlow></roa:basicData></roa:physicalQuantity></roa:physicalQuantity>
  <roa:physicalQuantity index="4"><roa:physicalQuantity xsi:type="roa:SinglePhysicalQuantity"><roa:basicData xsi:type="roa:TrafficSpeed"><roa:averageVehicleSpeed accuracy="0.0"><com:speed>107.0</com:speed></roa:averageVehicleSpeed></roa:basicData></roa:physicalQuantity></roa:physicalQuantity>
  <roa:measurementTimeDefault><roa:timeValue>2026-10-01T18:55:00Z</roa:timeValue></roa:measurementTimeDefault>
</roa:siteMeasurements>
<roa:siteMeasurements>
  <roa:measurementSiteReference targetClass="roa:MeasurementSite" id="RWS01_MONIBAS_0270vwd0063ra" version="1738"/>
  <roa:physicalQuantity index="1"><roa:physicalQuantity xsi:type="roa:SinglePhysicalQuantity"><roa:basicData xsi:type="roa:TrafficFlow"><roa:vehicleFlow accuracy="0.0"><com:vehicleFlowRate>0</com:vehicleFlowRate></roa:vehicleFlow></roa:basicData></roa:physicalQuantity></roa:physicalQuantity>
  <roa:physicalQuantity index="2"><roa:physicalQuantity xsi:type="roa:SinglePhysicalQuantity"><roa:basicData xsi:type="roa:TrafficSpeed"><roa:averageVehicleSpeed accuracy="0.0"><com:speed>-1.0</com:speed></roa:averageVehicleSpeed></roa:basicData></roa:physicalQuantity></roa:physicalQuantity>
  <roa:measurementTimeDefault><roa:timeValue>2026-10-01T18:55:00Z</roa:timeValue></roa:measurementTimeDefault>
</roa:siteMeasurements>
</mc:payload>
</mc:messageContainer>
"""


def _stream():
    return gzip.GzipFile(fileobj=io.BytesIO(gzip.compress(FEED)))


def test_extract_only_our_sites_and_pair_lanes():
    found = extract_readings(_stream(), wanted={"hrl", "vwd"})
    assert set(found) == {"hrl", "vwd"}  # the Amsterdam site is ignored
    hrl = found["hrl"]
    assert hrl.ndw_site_id == "RWS01_MONIBAS_0271hrl0063ra"
    assert hrl.timestamp == datetime(2026, 10, 1, 18, 55, tzinfo=timezone.utc)
    assert hrl.lane_flows == [360, 600] and hrl.lane_speeds == [127.0, 107.0]
    assert hrl.lanes == 2 and hrl.intensity_veh_per_hr == 960
    assert hrl.speed_kmh == pytest.approx((360 * 127 + 600 * 107) / 960, abs=0.05)
    assert not hrl.has_bad_speed


def test_sentinel_speed_is_detected_and_excluded_from_mean():
    found = extract_readings(_stream(), wanted={"vwd"})
    vwd = found["vwd"]
    assert vwd.has_bad_speed
    assert vwd.intensity_veh_per_hr == 0
    assert vwd.speed_kmh is None
    assert vwd.csv_row()["bad_data"] == "1" and vwd.csv_row()["lane_speeds"] == "-1.0"


def test_early_exit_when_all_wanted_sites_seen():
    # hrr/vwa are not in the fixture; asking for hrl only must still return without scanning for them
    found = extract_readings(_stream(), wanted={"hrl"})
    assert set(found) == {"hrl"}


def test_parse_returns_none_for_unknown_site():
    import xml.etree.ElementTree as ET
    root = ET.fromstring(FEED)
    first = root.find(".//{http://datex2.eu/schema/3/roadTrafficData}siteMeasurements")
    assert parse_site_measurements(first) is None


def test_s3_key_uses_measurement_time_not_fetch_time():
    r = SiteReading("hrl", "RWS01_MONIBAS_0271hrl0063ra", datetime(2026, 10, 1, 23, 59, tzinfo=timezone.utc), [1], [1.0])
    assert s3_key_for(r) == "ndw/2026-10-01/23-hrl.csv"


class FakeS3:
    """Minimal in-memory stand-in for the two boto3 calls append_to_s3_csv uses."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def get_object(self, Bucket, Key):
        from botocore.exceptions import ClientError
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "x"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body


def test_append_to_s3_csv_is_idempotent_and_sorted():
    s3 = FakeS3()
    t1 = SiteReading("hrl", "X", datetime(2026, 10, 1, 20, 10, tzinfo=timezone.utc), [100], [90.0])
    t0 = SiteReading("hrl", "X", datetime(2026, 10, 1, 20, 5, tzinfo=timezone.utc), [50], [80.0])
    key = append_to_s3_csv(s3, "b", t1)
    append_to_s3_csv(s3, "b", t0)
    append_to_s3_csv(s3, "b", t1)  # same minute again → no duplicate
    lines = s3.objects[key].decode().strip().splitlines()
    assert lines[0] == ",".join(CSV_COLUMNS)
    assert len(lines) == 3
    assert lines[1].startswith("2026-10-01T20:05:00Z") and lines[2].startswith("2026-10-01T20:10:00Z")
