"""Unit tests for the Luchtmeetnet ingestion transforms. No network, no database."""
import pandas as pd

from ingest_air import filter_no2_readings, flag_stale_or_null, to_frame


def test_filter_no2_readings_handles_null_value():
    df = pd.DataFrame({
        "component": ["NO2", "NO2", "PM10"],
        "value": [18.4, None, 22.1],
        "timestamp": ["2024-01-15T08:00:00Z", "2024-01-15T09:00:00Z", "2024-01-15T08:00:00Z"],
    })
    result = filter_no2_readings(df)
    assert len(result) == 2
    assert result["value"].isnull().sum() == 1  # null NO2 rows are kept, not silently dropped


def test_to_frame_maps_api_fields():
    records = [
        {"value": 12.25, "timestamp_measured": "2026-10-01T18:00:00+00:00", "formula": "NO2"},
        {"value": None, "timestamp_measured": "2026-10-01T17:00:00+00:00", "formula": "NO2"},
    ]
    df = to_frame(records)
    assert list(df.columns) == ["station_id", "timestamp", "component", "value"]
    assert (df["station_id"] == "NL10240").all()
    assert str(df["timestamp"].dt.tz) == "UTC"
    assert df["value"].isna().sum() == 1


def test_to_frame_empty():
    assert to_frame([]).empty


def _series(values):
    ts = pd.date_range("2024-01-15T00:00:00Z", periods=len(values), freq="h")
    return pd.DataFrame({"station_id": "NL10240", "timestamp": ts, "component": "NO2", "value": values})


def test_flag_null_values():
    out = flag_stale_or_null(_series([10.0, None, 12.0]))
    assert out["is_flagged"].tolist() == [False, True, False]


def test_flag_three_identical_consecutive_hours():
    out = flag_stale_or_null(_series([10.0, 15.0, 15.0, 15.0, 16.0]))
    # the third identical value completes a run of 3 → flagged; the first two are not
    assert out["is_flagged"].tolist() == [False, False, False, True, False]


def test_flag_run_continues_across_fetches_using_db_history():
    # the two previous hours in the database already read 15.0 → the first new row is the 3rd in a row
    prev = [(pd.Timestamp("2024-01-14T22:00:00Z").to_pydatetime(), 15.0),
            (pd.Timestamp("2024-01-14T23:00:00Z").to_pydatetime(), 15.0)]
    out = flag_stale_or_null(_series([15.0, 15.0, 20.0]), previous=prev)
    assert out["is_flagged"].tolist() == [True, True, False]


def test_identical_values_hours_apart_are_not_a_frozen_sensor():
    ts = pd.to_datetime(["2024-01-15T00:00Z", "2024-01-15T05:00Z", "2024-01-15T11:00Z"], utc=True)
    df = pd.DataFrame({"station_id": "NL10240", "timestamp": ts, "component": "NO2", "value": [15.0, 15.0, 15.0]})
    assert not flag_stale_or_null(df)["is_flagged"].any()


def test_flag_two_identical_values_is_not_stale():
    out = flag_stale_or_null(_series([15.0, 15.0, 16.0]))
    assert not out["is_flagged"].any()
