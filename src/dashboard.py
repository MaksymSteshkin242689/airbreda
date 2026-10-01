"""dashboard.py — the AirBreda serving layer: JSON API + human-readable page, one FastAPI app.

Routes
    GET /site/{site_id}   latest real NO₂ + latest real traffic for one NDW site + model prediction
    GET /health           both ingestion sources side by side (last fetch, bad-data counters)
    GET /                 HTML dashboard; it is just another client of /site/{id}

Where each field of /site/{id} comes from (required comment, Day 4):
    site_id, ndw_site_id          — the SITES table in sites.py (hrl/hrr/vwd/vwa → NDW id)
    no2_ug_m3, no2_timestamp,
    no2_is_flagged                — sensor_readings: latest non-null NO₂ row for station NL10240,
                                    written by ingest_air.py. One station covers all four sites
                                    (same interchange), so this value is identical across sites.
    intensity_veh_per_hr,
    speed_kmh, timestamp          — traffic_readings: latest clean row for *this* site, written by
                                    ingest_traffic.py (rows with speed=-1 never reach this table).
    no2_ug_m3_predicted,
    no2_exceedance_risk           — predict.predict(total_intensity, hour_of_day) from predict.py,
                                    model.pkl baked into this image. The model was trained on the
                                    *total* interchange intensity, so the prediction is computed
                                    from the sum of the four sites' latest readings and is the same
                                    for all four — a per-site prediction would need per-site
                                    training data we do not have (see ADR-006).
    prediction_basis              — exactly what went into predict(), for transparency.

What happens if predict() raises:
    The request does NOT fail. The real measurements are the valuable part of the response and
    they do not depend on the model; a broken model file or an sklearn version mismatch should
    not take the air-quality numbers off the dashboard. The route returns the real NO₂ and
    intensity with ``no2_ug_m3_predicted`` / ``no2_exceedance_risk`` set to null and a
    ``prediction_error`` string, and logs a structured ``prediction_failed`` event so the failure is
    visible in the logs rather than hidden behind a 500. Graceful degradation over all-or-nothing.
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

import predict as model
from db import read_status, transaction
from observability import get_logger, log_event
from settings import Settings
from sites import MAINLINE_SITES, SITES, STATION_ID

# Freshness windows for /health. Luchtmeetnet publishes hourly and the cron runs at :05, so
# anything older than ~3 h means missed runs; NDW is polled every 5 min, so 20 min = 4 misses.
AIR_STALE_AFTER = timedelta(hours=3)
NDW_STALE_AFTER = timedelta(minutes=20)
# A site's latest reading older than this is not "current traffic" and is left out of the total.
TRAFFIC_CURRENT_WINDOW = timedelta(minutes=15)

log = get_logger("dashboard")
templates = Jinja2Templates(directory=str(Path(__file__).with_name("templates")))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.settings = Settings.from_env()
    # Load the model once at startup so the first request is not the one that discovers a
    # broken model file; a failure here is logged but the API still serves real measurements.
    try:
        model.load_model()
        app.state.model_loaded = True
    except Exception as exc:  # noqa: BLE001 — degrade, do not crash the service
        app.state.model_loaded = False
        log_event(log, logging.ERROR, event="model_load_failed", path=str(model.MODEL_PATH), error=str(exc))
    log_event(log, logging.INFO, event="startup", model_loaded=app.state.model_loaded, sites=list(SITES))
    yield


app = FastAPI(title="AirBreda", version="1.0", lifespan=lifespan)


@app.middleware("http")
async def access_log(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    log_event(log, logging.INFO, event="http_request", method=request.method, path=request.url.path,
              status=response.status_code, duration_ms=round((time.perf_counter() - started) * 1000, 1))
    return response


# --------------------------------------------------------------------------- queries
def latest_no2(conn) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT timestamp, value, is_flagged FROM sensor_readings
        WHERE station_id = %s AND component = 'NO2' AND value IS NOT NULL
        ORDER BY timestamp DESC LIMIT 1
        """,
        (STATION_ID,),
    ).fetchone()
    if row is None:
        return None
    ts, value, flagged = row
    return {"no2_ug_m3": round(float(value), 2), "no2_timestamp": ts.isoformat(), "no2_is_flagged": bool(flagged)}


def latest_traffic_all_sites(conn) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT DISTINCT ON (site_id) site_id, ndw_site_id, timestamp, intensity_veh_per_hr, speed_kmh
        FROM traffic_readings ORDER BY site_id, timestamp DESC
        """
    ).fetchall()
    return {
        site: {"ndw_site_id": ndw_id, "timestamp": ts, "intensity_veh_per_hr": int(intensity),
               "speed_kmh": None if speed is None else float(speed)}
        for site, ndw_id, ts, intensity, speed in rows
    }


# --------------------------------------------------------------------------- prediction
def prediction_for(traffic: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Build the model input from the latest reading of every site and run predict().

    Mirrors build_training_data.hourly_traffic: the feature is the *total* intensity over the
    four sites; a slip road with no current reading counts as 0 (no vehicles), but if either
    mainline direction has no current reading the feed is considered down and no prediction is
    made. hour_of_day comes from the measurement time, not the wall clock, through the same
    function used at training time."""
    now = datetime.now(timezone.utc)
    current = {s: r for s, r in traffic.items() if now - r["timestamp"] <= TRAFFIC_CURRENT_WINDOW}
    basis: dict[str, Any] = {"sites_in_total": sorted(current)}
    if not set(MAINLINE_SITES) <= current.keys():
        return {"no2_ug_m3_predicted": None, "no2_exceedance_risk": None,
                "prediction_basis": basis, "prediction_error": "no current mainline traffic reading"}
    total = sum(r["intensity_veh_per_hr"] for r in current.values())
    latest_ts = max(r["timestamp"] for r in current.values())
    hour = model.hour_of_day_from(latest_ts)
    basis.update({"total_intensity_veh_per_hr": total, "hour_of_day": hour, "as_of": latest_ts.isoformat()})
    try:
        out = model.predict(total, hour)
    except Exception as exc:  # noqa: BLE001 — see module docstring
        log_event(log, logging.ERROR, event="prediction_failed", total_intensity_veh_per_hr=total,
                  hour_of_day=hour, error=str(exc))
        return {"no2_ug_m3_predicted": None, "no2_exceedance_risk": None,
                "prediction_basis": basis, "prediction_error": str(exc)}
    return {**out, "prediction_basis": basis}


# --------------------------------------------------------------------------- routes
@app.get("/site/{site_id}")
def site(site_id: str, request: Request) -> dict[str, Any]:
    if site_id not in SITES:
        raise HTTPException(status_code=404, detail=f"unknown site '{site_id}'; expected one of {sorted(SITES)}")
    with transaction(request.app.state.settings) as conn:
        no2 = latest_no2(conn)
        traffic = latest_traffic_all_sites(conn)
    mine = traffic.get(site_id)
    if no2 is None or mine is None:
        raise HTTPException(status_code=503, detail="no readings ingested yet for this site")

    body: dict[str, Any] = {
        "site_id": site_id,
        "ndw_site_id": SITES[site_id],
        **no2,
        "intensity_veh_per_hr": mine["intensity_veh_per_hr"],
        "speed_kmh": mine["speed_kmh"],
        **prediction_for(traffic),
        "timestamp": mine["timestamp"].isoformat(),
    }
    return body


@app.get("/health")
def health(request: Request) -> JSONResponse:
    settings = request.app.state.settings
    now = datetime.now(timezone.utc)
    try:
        with transaction(settings) as conn:
            status = read_status(conn)
    except Exception as exc:  # noqa: BLE001 — the database *is* the health of this service
        log_event(log, logging.ERROR, event="health_db_unreachable", error=str(exc))
        return JSONResponse(status_code=503, content={"status": "error", "error": "database unreachable",
                                                      "checked_at": now.isoformat()})

    def view(key: str, stale_after: timedelta) -> dict[str, Any]:
        s = status.get(key, {})
        last = s.get("last_successful_fetch")
        fresh = last is not None and now - datetime.fromisoformat(last) <= stale_after
        return {
            "last_successful_fetch": last,
            "bad_data_count": s.get("bad_data_count", 0),
            "bad_data_last_24h": s.get("bad_data_last_24h", 0),
            "last_error": s.get("last_error"),
            "fresh": fresh,
        }

    air, ndw = view("luchtmeetnet", AIR_STALE_AFTER), view("ndw", NDW_STALE_AFTER)
    overall = "ok" if (air["fresh"] and ndw["fresh"]) else "degraded"
    return JSONResponse({
        "status": overall,
        "luchtmeetnet": air,
        "ndw": ndw,
        "model": {"loaded": bool(getattr(request.app.state, "model_loaded", False)),
                  "path": str(model.MODEL_PATH), "threshold_ug_m3": model.THRESHOLD_UG_M3},
        "checked_at": now.isoformat(),
    })


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html", {
        "sites": SITES, "station_id": STATION_ID, "threshold": model.THRESHOLD_UG_M3,
    })
