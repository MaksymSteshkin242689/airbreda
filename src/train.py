"""train.py — fit the AirBreda NO₂ regression and write model/model.pkl + model/metrics.json.

A plain linear regression on two features (total traffic intensity, local hour of day). With a
few days of hourly data this is the right amount of model: anything more flexible would fit the
noise of a few dozen points and be impossible to debug (Google's "Rules of ML", rule #1; see
ADR-006). The metrics file is what ADR-006 quotes, so the numbers in the document are always the
numbers of the model that is actually deployed.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

from predict import FEATURES, TARGET, THRESHOLD_UG_M3

DATA_PATH = Path("data/training_data.csv")
MODEL_DIR = Path("model")
MIN_ROWS_FOR_HOLDOUT = 30  # below this a held-out test set is too small to mean anything


MIN_ROWS = len(FEATURES) + 1  # the mathematical minimum for an exact fit; anything less is a dry run


def train(df: pd.DataFrame, seed: int = 42, min_rows: int = MIN_ROWS) -> tuple[LinearRegression, dict]:
    df = df.dropna(subset=[*FEATURES, TARGET])
    X, y = df[FEATURES], df[TARGET]
    n = len(df)
    if n < min_rows:
        raise SystemExit(f"only {n} rows — need at least {min_rows} to fit {len(FEATURES)} coefficients")

    holdout = n >= MIN_ROWS_FOR_HOLDOUT
    if holdout:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=seed)
    else:
        X_train, y_train = X, y
        X_test, y_test = X, y  # evaluated in-sample; reported as such

    model = LinearRegression().fit(X_train, y_train)
    pred_test = model.predict(X_test)
    pred_all = model.predict(X)

    metrics = {
        "trained_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "algorithm": "sklearn.linear_model.LinearRegression",
        "features": FEATURES,
        "target": TARGET,
        "rows_total": int(n),
        "rows_train": int(len(X_train)),
        "rows_test": int(len(X_test)) if holdout else 0,
        "evaluation": "held-out 25 % test split" if holdout else "in-sample (too few rows for a meaningful hold-out)",
        "r2": round(float(r2_score(y_test, pred_test)), 4),
        "mae_ug_m3": round(float(mean_absolute_error(y_test, pred_test)), 3),
        "r2_in_sample": round(float(r2_score(y, pred_all)), 4),
        "mae_in_sample_ug_m3": round(float(mean_absolute_error(y, pred_all)), 3),
        "coefficients": {f: round(float(c), 6) for f, c in zip(FEATURES, model.coef_)},
        "intercept": round(float(model.intercept_), 4),
        "target_stats": {"mean": round(float(y.mean()), 2), "std": round(float(y.std()), 2),
                         "min": round(float(y.min()), 2), "max": round(float(y.max()), 2)},
        "feature_stats": {f: {"mean": round(float(X[f].mean()), 2), "min": round(float(X[f].min()), 2),
                              "max": round(float(X[f].max()), 2)} for f in FEATURES},
        "hours_above_threshold": int((y > THRESHOLD_UG_M3).sum()),
        "threshold_ug_m3": THRESHOLD_UG_M3,
        "data_first_hour_utc": str(df["hour_start_utc"].min()) if "hour_start_utc" in df else None,
        "data_last_hour_utc": str(df["hour_start_utc"].max()) if "hour_start_utc" in df else None,
    }
    return model, metrics


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data", type=Path, default=DATA_PATH)
    parser.add_argument("--out-dir", type=Path, default=MODEL_DIR)
    parser.add_argument("--min-rows", type=int, default=MIN_ROWS,
                        help=f"refuse to train on fewer rows (default {MIN_ROWS}); lower only for a pipeline dry run")
    args = parser.parse_args(argv)

    df = pd.read_csv(args.data)
    model, metrics = train(df, min_rows=args.min_rows)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, args.out_dir / "model.pkl")
    (args.out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")

    print(json.dumps(metrics, indent=2))
    sign = "+" if metrics["coefficients"]["total_intensity_veh_per_hr"] >= 0 else "-"
    print(f"\nmodel.pkl written to {args.out_dir}/ — traffic coefficient sign: {sign} "
          f"({'as expected' if sign == '+' else 'UNEXPECTED: more traffic → lower NO2; discuss in ADR-006'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
