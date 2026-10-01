"""predict.py — NO₂ prediction and exceedance risk for one AirBreda site-hour.

This module is the *single* place where model features are computed. ``build_training_data.py``
calls ``make_features`` / ``hour_of_day_from`` when building the training set, and
``dashboard.py`` calls ``predict`` at serving time. Sharing one implementation is the cheapest
defence against training-serving skew: the classic bug is computing ``hour_of_day`` in UTC in
one place and in local time in the other, with the model silently fed a different feature
distribution in production than it saw in training (see ADR-006).

The model file is baked into the dashboard image (Day 4), so there is no network dependency
at prediction time and the dashboard version and model version always ship together.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

import joblib
import numpy as np
import pandas as pd

FEATURES = ["total_intensity_veh_per_hr", "hour_of_day"]
TARGET = "no2_ug_m3"

# Exceedance threshold (µg/m³). 40 is the EU annual limit value for NO₂ (Directive 2008/50/EC)
# and the level the municipality already reports against. It is an *annual* mean, and we apply
# it to *hourly* predictions, so "risk" here means "this hour is running above the level the
# city is trying to stay under on average", not a legal hourly exceedance (that limit is 200).
# At this station hourly values sit in the 10-50 range, so 40 produces a usable 0-1 spread;
# 200 would give a risk of ~0 for every hour of the year and carry no information. See ADR-006.
THRESHOLD_UG_M3 = float(os.getenv("NO2_THRESHOLD_UG_M3", "40"))
STEEPNESS = float(os.getenv("NO2_RISK_STEEPNESS", "0.2"))

LOCAL_TZ = ZoneInfo("Europe/Amsterdam")
MODEL_PATH = Path(os.getenv("MODEL_PATH", "model/model.pkl"))


def hour_of_day_from(ts: datetime) -> int:
    """Hour of day in *local* (Europe/Amsterdam) time, 0-23. Traffic follows local clocks, not
    UTC, and the one-hour DST shift would otherwise smear the rush-hour signal twice a year.
    Naive datetimes are treated as UTC."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(LOCAL_TZ).hour


def make_features(total_intensity_veh_per_hr: float, hour_of_day: int) -> pd.DataFrame:
    """One-row feature frame in the exact column order the model was trained on."""
    return pd.DataFrame([{"total_intensity_veh_per_hr": float(total_intensity_veh_per_hr),
                          "hour_of_day": int(hour_of_day)}], columns=FEATURES)


def exceedance_risk(predicted_no2: float, threshold: float = THRESHOLD_UG_M3, steepness: float = STEEPNESS) -> float:
    """Map a predicted concentration to a 0-1 risk with a logistic curve centred on the
    threshold: 0.5 exactly at the threshold, ~0.88 at +10 µg/m³, ~0.12 at -10 µg/m³."""
    return float(1.0 / (1.0 + np.exp(-steepness * (predicted_no2 - threshold))))


@lru_cache(maxsize=1)
def load_model(path: Path = MODEL_PATH):
    if not path.exists():
        raise FileNotFoundError(f"model file not found at {path}; run train.py first")
    return joblib.load(path)


def predict(total_intensity_veh_per_hr: float, hour_of_day: int) -> dict[str, float]:
    """Return ``{"no2_ug_m3_predicted": float, "no2_exceedance_risk": float}``.

    Predictions are clipped at 0: a linear model can extrapolate below zero for very low traffic
    at night, and a negative concentration is not a thing."""
    model = load_model()
    raw = float(model.predict(make_features(total_intensity_veh_per_hr, hour_of_day))[0])
    predicted = max(0.0, raw)
    return {
        "no2_ug_m3_predicted": round(predicted, 2),
        "no2_exceedance_risk": round(exceedance_risk(predicted), 3),
    }
