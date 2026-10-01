"""Route tests for the dashboard API. The database layer is replaced with in-memory fakes so the
tests exercise routing, response shape, the missing-mainline rule and graceful degradation when
the model is unavailable — without Postgres or a model file."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import dashboard
import predict

NOW = datetime.now(timezone.utc)


@pytest.fixture(autouse=True)
def env(monkeypatch):
    for k, v in {"DB_HOST": "db.invalid", "DB_USER": "u", "DB_PASSWORD": "p", "S3_BUCKET": "b"}.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def client(monkeypatch):
    @contextmanager
    def fake_transaction(_settings):
        yield object()
    monkeypatch.setattr(dashboard, "transaction", fake_transaction)
    with TestClient(dashboard.app) as c:
        yield c


def _fake_data(monkeypatch, *, no2=True, sites=("hrl", "hrr", "vwd", "vwa"), hourly_sites=None):
    latest = {s: {"ndw_site_id": dashboard.SITES[s], "timestamp": NOW - timedelta(minutes=3),
                  "intensity_veh_per_hr": 1000 + i * 100, "speed_kmh": 100.0} for i, s in enumerate(sites)}
    hourly = {s: {"mean_intensity": 900.0 + i * 100, "latest": NOW - timedelta(minutes=3), "snapshots": 12}
              for i, s in enumerate(hourly_sites if hourly_sites is not None else sites)}
    monkeypatch.setattr(dashboard, "latest_no2", lambda conn: (
        {"no2_ug_m3": 23.4, "no2_timestamp": NOW.isoformat(), "no2_is_flagged": False} if no2 else None))
    monkeypatch.setattr(dashboard, "latest_traffic_all_sites", lambda conn: latest)
    monkeypatch.setattr(dashboard, "trailing_hour_traffic", lambda conn: hourly)


def test_unknown_site_is_404(client):
    r = client.get("/site/bogus")
    assert r.status_code == 404
    assert "hrl" in r.json()["detail"]


def test_site_without_model_degrades_to_real_values_with_null_prediction(client, monkeypatch):
    _fake_data(monkeypatch)
    monkeypatch.setattr(predict, "MODEL_PATH", predict.Path("/nonexistent/model.pkl"))
    predict._models.clear()
    r = client.get("/site/hrl")
    assert r.status_code == 200
    body = r.json()
    for key in ("site_id", "no2_ug_m3", "intensity_veh_per_hr", "no2_exceedance_risk", "timestamp"):
        assert key in body
    assert body["site_id"] == "hrl" and body["no2_ug_m3"] == 23.4 and body["intensity_veh_per_hr"] == 1000
    assert body["no2_exceedance_risk"] is None and body["no2_ug_m3_predicted"] is None
    assert "model file not found" in body["prediction_error"]


def test_site_with_model_returns_prediction_from_hourly_total(client, monkeypatch):
    _fake_data(monkeypatch)
    seen = {}

    def fake_predict(total, hour, model_path=None):
        seen.update(total=total, hour=hour)
        return {"no2_ug_m3_predicted": 31.2, "no2_exceedance_risk": 0.17}
    monkeypatch.setattr(dashboard.model, "predict", fake_predict)

    r = client.get("/site/vwa")
    body = r.json()
    assert r.status_code == 200
    assert body["no2_exceedance_risk"] == 0.17 and body["no2_ug_m3_predicted"] == 31.2
    # hourly means 900+1000+1100+1200 = 4200, not the latest-minute values
    assert seen["total"] == 4200 and body["prediction_basis"]["total_intensity_veh_per_hr"] == 4200
    assert body["prediction_basis"]["sites_in_total"] == ["hrl", "hrr", "vwa", "vwd"]
    assert 0 <= seen["hour"] <= 23


def test_missing_mainline_in_last_hour_means_no_prediction(client, monkeypatch):
    _fake_data(monkeypatch, hourly_sites=("hrl", "vwd", "vwa"))  # hrr has no clean snapshot
    monkeypatch.setattr(dashboard.model, "predict", lambda *a, **k: pytest.fail("must not be called"))
    body = client.get("/site/hrl").json()
    assert body["no2_exceedance_risk"] is None
    assert "mainline" in body["prediction_error"]


def test_site_without_any_readings_is_503(client, monkeypatch):
    _fake_data(monkeypatch, no2=False)
    assert client.get("/site/hrl").status_code == 503


def test_health_ok_and_degraded(client, monkeypatch):
    fresh = (NOW - timedelta(minutes=2)).isoformat()
    stale = (NOW - timedelta(hours=5)).isoformat()
    monkeypatch.setattr(dashboard, "read_status", lambda conn: {
        "luchtmeetnet": {"last_successful_fetch": fresh, "bad_data_count": 1, "bad_data_last_24h": 1, "last_error": None},
        "ndw": {"last_successful_fetch": fresh, "bad_data_count": 7, "bad_data_last_24h": 3, "last_error": None},
    })
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["luchtmeetnet"]["bad_data_count"] == 1 and body["ndw"]["bad_data_count"] == 7
    assert body["ndw"]["last_successful_fetch"] == fresh

    monkeypatch.setattr(dashboard, "read_status", lambda conn: {
        "luchtmeetnet": {"last_successful_fetch": stale, "bad_data_count": 0, "bad_data_last_24h": 0, "last_error": None},
        "ndw": {"last_successful_fetch": fresh, "bad_data_count": 0, "bad_data_last_24h": 0, "last_error": None},
    })
    body = client.get("/health").json()
    assert body["status"] == "degraded" and body["luchtmeetnet"]["fresh"] is False


def test_health_is_503_when_database_unreachable(monkeypatch):
    @contextmanager
    def broken(_settings):
        raise ConnectionError("no route to host")
        yield  # pragma: no cover
    monkeypatch.setattr(dashboard, "transaction", broken)
    with TestClient(dashboard.app) as c:
        r = c.get("/health")
    assert r.status_code == 503 and r.json()["status"] == "error"


def test_index_page_renders_sites(client):
    r = client.get("/")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    for s in ("hrl", "hrr", "vwd", "vwa"):
        assert f'id="row-{s}"' in r.text
