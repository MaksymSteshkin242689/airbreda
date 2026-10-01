"""Model tests (Day 4): the pickle loads, predict() returns plausible values, the risk mapping
behaves, and train.py produces a model + metrics from a CSV. Training here uses a small
synthetic dataset with a known positive traffic→NO₂ relationship so the test is deterministic
and does not depend on how much real data has been collected."""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import predict
from train import train


@pytest.fixture(scope="module")
def trained(tmp_path_factory) -> Path:
    rng = np.random.default_rng(0)
    hours = np.arange(72)
    traffic = 2000 + 1500 * np.sin((hours % 24 - 8) / 24 * 2 * np.pi) + rng.normal(0, 150, 72)
    no2 = 8 + 0.006 * traffic + rng.normal(0, 2, 72)
    df = pd.DataFrame({
        "hour_start_utc": pd.date_range("2026-09-01", periods=72, freq="h", tz="UTC"),
        "no2_ug_m3": no2, "total_intensity_veh_per_hr": traffic, "hour_of_day": hours % 24,
    })
    out = tmp_path_factory.mktemp("model")
    model, metrics = train(df)
    import joblib
    joblib.dump(model, out / "model.pkl")
    (out / "metrics.json").write_text(json.dumps(metrics))
    return out / "model.pkl"


def test_model_pickle_loads(trained):
    model = predict.load_model(trained)
    assert hasattr(model, "predict") and hasattr(model, "coef_")


def test_predict_returns_plausible_no2_and_risk_in_unit_interval(trained):
    out = predict.predict(3200, 8, model_path=trained)
    assert set(out) == {"no2_ug_m3_predicted", "no2_exceedance_risk"}
    assert 0.0 <= out["no2_ug_m3_predicted"] <= 200.0
    assert 0.0 <= out["no2_exceedance_risk"] <= 1.0


def test_more_traffic_means_higher_prediction(trained):
    low = predict.predict(500, 8, model_path=trained)
    high = predict.predict(4000, 8, model_path=trained)
    assert high["no2_ug_m3_predicted"] > low["no2_ug_m3_predicted"]
    assert high["no2_exceedance_risk"] > low["no2_exceedance_risk"]


def test_prediction_never_negative(trained):
    assert predict.predict(0, 3, model_path=trained)["no2_ug_m3_predicted"] >= 0.0


def test_exceedance_risk_is_half_at_threshold_and_monotonic():
    t = predict.THRESHOLD_UG_M3
    assert predict.exceedance_risk(t) == pytest.approx(0.5)
    assert predict.exceedance_risk(t + 10) > 0.8
    assert predict.exceedance_risk(t - 10) < 0.2
    assert predict.exceedance_risk(t - 100) >= 0.0 and predict.exceedance_risk(t + 100) <= 1.0


def test_hour_of_day_uses_local_time():
    from datetime import datetime, timezone
    # 22:30 UTC on 1 Oct 2026 is 00:30 local (CEST, UTC+2)
    assert predict.hour_of_day_from(datetime(2026, 10, 1, 22, 30, tzinfo=timezone.utc)) == 0
    # naive datetimes are treated as UTC
    assert predict.hour_of_day_from(datetime(2026, 10, 1, 22, 30)) == 0


def test_train_reports_metrics_and_positive_traffic_coefficient(trained):
    metrics = json.loads((trained.parent / "metrics.json").read_text())
    assert metrics["rows_total"] == 72
    assert metrics["evaluation"].startswith("held-out")
    assert metrics["coefficients"]["total_intensity_veh_per_hr"] > 0
    assert metrics["r2"] > 0.5
    assert metrics["mae_ug_m3"] < 5


def test_train_refuses_too_few_rows():
    df = pd.DataFrame({"no2_ug_m3": [10.0, 12.0], "total_intensity_veh_per_hr": [100, 200], "hour_of_day": [1, 2]})
    with pytest.raises(SystemExit):
        train(df)


def test_shipped_model_loads_and_predicts():
    """Guards the artefact that is actually baked into the dashboard image."""
    shipped = Path("model/model.pkl")
    if not shipped.exists():
        pytest.skip("model/model.pkl not trained yet")
    out = predict.predict(2500, 8, model_path=shipped)
    assert 0.0 <= out["no2_ug_m3_predicted"] <= 200.0
    assert 0.0 <= out["no2_exceedance_risk"] <= 1.0
    metrics = json.loads(Path("model/metrics.json").read_text())
    assert metrics["features"] == predict.FEATURES
