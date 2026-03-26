#!/usr/bin/env python3
"""ML-based damage prediction as alternative to FAST deterministic DDFs."""

import argparse

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

try:
    import lightgbm as lgb

    _USE_LGB = True
except ImportError:
    import xgboost as xgb

    _USE_LGB = False


def build_training_data(fast_output_csv, raster_path=None):
    """Build features/target from FAST output CSV, optionally sampling flood depth from raster."""
    df = pd.read_csv(fast_output_csv)

    # Compute damage ratio from FAST output columns
    if "BldgLossUSD" in df.columns and "Cost" in df.columns:
        df["damage_ratio"] = (df["BldgLossUSD"] / df["Cost"]).clip(0, 1)
    elif "BldgDmgPct" in df.columns:
        df["damage_ratio"] = df["BldgDmgPct"].clip(0, 1)
    else:
        raise ValueError("Cannot compute damage ratio: need BldgLossUSD+Cost or BldgDmgPct")

    # Sample surge depth from raster if provided
    if raster_path:
        import rasterio

        with rasterio.open(raster_path) as src:
            coords = list(zip(df["Longitude"], df["Latitude"]))
            df["surge_depth"] = [max(0, v[0]) for v in src.sample(coords)]
    elif "SurgeDepth" in df.columns:
        df["surge_depth"] = df["SurgeDepth"].fillna(0)
    else:
        df["surge_depth"] = 0

    occ_dummies = pd.get_dummies(df["Occ"].astype(str).str[:3], prefix="occ")
    features = pd.concat(
        [
            df[["FoundationType", "NumStories", "FirstFloorHt", "Cost", "surge_depth"]].rename(
                columns={"Cost": "building_value"}
            ),
            occ_dummies,
        ],
        axis=1,
    ).fillna(0)

    return features, df["damage_ratio"]


def train_damage_model(features, target):
    """Train gradient boosted model, return (model, metrics_dict)."""
    X_train, X_test, y_train, y_test = train_test_split(
        features, target, test_size=0.2, random_state=42
    )

    if _USE_LGB:
        model = lgb.LGBMRegressor(
            n_estimators=200, learning_rate=0.05, max_depth=6, random_state=42, verbose=-1
        )
    else:
        model = xgb.XGBRegressor(
            n_estimators=200, learning_rate=0.05, max_depth=6, random_state=42, verbosity=0
        )

    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    metrics = {
        "rmse": float(np.sqrt(mean_squared_error(y_test, preds))),
        "mae": float(mean_absolute_error(y_test, preds)),
        "r2": float(r2_score(y_test, preds)),
        "train_size": len(X_train),
        "test_size": len(X_test),
    }
    return model, metrics, X_test, y_test


def compare_with_fast(model, test_features, test_actual, fast_predictions):
    """Compare ML vs FAST deterministic predictions."""
    ml_preds = model.predict(test_features)
    fast_preds = np.array(fast_predictions)
    return {
        "ml_rmse": float(np.sqrt(mean_squared_error(test_actual, ml_preds))),
        "fast_rmse": float(np.sqrt(mean_squared_error(test_actual, fast_preds))),
        "ml_mae": float(mean_absolute_error(test_actual, ml_preds)),
        "fast_mae": float(mean_absolute_error(test_actual, fast_preds)),
        "ml_r2": float(r2_score(test_actual, ml_preds)),
        "fast_r2": float(r2_score(test_actual, fast_preds)),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train ML damage prediction model")
    parser.add_argument("--fast-output", required=True, help="FAST output CSV path")
    parser.add_argument("--raster", default=None, help="Flood depth raster path")
    args = parser.parse_args()

    print("Building training data...")
    features, target = build_training_data(args.fast_output, args.raster)
    print(f"  Features shape: {features.shape}, target size: {len(target)}")

    print("Training model...")
    model, metrics, X_test, y_test = train_damage_model(features, target)
    print(f"  Engine: {'LightGBM' if _USE_LGB else 'XGBoost'}")
    for k, v in metrics.items():
        print(f"  {k}: {v:.4f}" if isinstance(v, float) else f"  {k}: {v}")
