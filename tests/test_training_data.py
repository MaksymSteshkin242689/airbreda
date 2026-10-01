"""Pure-function tests for the training-set builder: hourly aggregation rules and the
one-hour alignment between NDW minute snapshots and Luchtmeetnet's end-of-hour timestamps."""
import pandas as pd

from build_training_data import build, hourly_traffic


def _minutes(rows):
    df = pd.DataFrame(rows, columns=["timestamp", "site_id", "intensity_veh_per_hr"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def test_hourly_mean_per_site_and_total():
    m = _minutes([
        ("2026-10-01T17:05Z", "hrl", 1000), ("2026-10-01T17:35Z", "hrl", 1400),
        ("2026-10-01T17:05Z", "hrr", 800), ("2026-10-01T17:35Z", "hrr", 1200),
        ("2026-10-01T17:05Z", "vwd", 100), ("2026-10-01T17:35Z", "vwd", 300),
        ("2026-10-01T17:05Z", "vwa", 200), ("2026-10-01T17:35Z", "vwa", 200),
    ])
    h = hourly_traffic(m)
    assert len(h) == 1
    row = h.iloc[0]
    assert row["intensity_hrl"] == 1200 and row["intensity_hrr"] == 1000
    assert row["intensity_vwd"] == 200 and row["intensity_vwa"] == 200
    assert row["total_intensity_veh_per_hr"] == 2600
    assert row["snapshots"] == 2 and row["sites_present"] == 4


def test_missing_slip_road_counts_as_zero_but_missing_mainline_drops_hour():
    m = _minutes([
        # hour 17: both mainlines present, slip roads absent (all their minutes had speed=-1)
        ("2026-10-01T17:05Z", "hrl", 1000), ("2026-10-01T17:05Z", "hrr", 800),
        # hour 18: hrr missing → dropped
        ("2026-10-01T18:05Z", "hrl", 1000), ("2026-10-01T18:05Z", "vwd", 100),
    ])
    h = hourly_traffic(m)
    assert h["hour_start_utc"].dt.hour.tolist() == [17]
    assert h.iloc[0]["intensity_vwd"] == 0 and h.iloc[0]["intensity_vwa"] == 0
    assert h.iloc[0]["sites_present"] == 2


def test_build_joins_traffic_hour_to_no2_stamped_at_end_of_that_hour():
    traffic = _minutes([("2026-10-01T17:10Z", "hrl", 1000), ("2026-10-01T17:10Z", "hrr", 1000)])
    no2 = pd.DataFrame({
        # Luchtmeetnet value stamped 18:00 covers 17:00–18:00 → hour_start 17:00
        "hour_start_utc": pd.to_datetime(["2026-10-01T17:00Z", "2026-10-01T16:00Z"], utc=True),
        "no2_ug_m3": [25.0, 99.0],
    })
    df = build(no2, traffic)
    assert len(df) == 1
    assert df.iloc[0]["no2_ug_m3"] == 25.0
    assert df.iloc[0]["total_intensity_veh_per_hr"] == 2000
    assert df.iloc[0]["hour_of_day"] == 19  # 17:00 UTC = 19:00 Europe/Amsterdam (CEST)


def test_empty_inputs_give_empty_frame_with_schema():
    df = build(pd.DataFrame({"hour_start_utc": pd.to_datetime([], utc=True), "no2_ug_m3": []}), _minutes([]))
    assert df.empty
    assert "total_intensity_veh_per_hr" in df.columns and "hour_of_day" in df.columns
