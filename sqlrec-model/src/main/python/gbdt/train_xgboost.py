"""XGBoost training entry point.

Reads the JSON pipeline.config, loads parquet training data, trains an XGBoost
model and persists the native model file (``model.ubj``) to ``{model_dir}/``.
XGBoost only accepts float/double features; categorical/integer columns are
rejected at validation time (use CatBoost for those).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile

import numpy as np
import pandas as pd
import xgboost as xgb

from common import filesystem as common

logger = logging.getLogger("gbdt.train_xgboost")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _build_xgb_params(p: dict) -> dict:
    """Translate the JSON params block into an XGBoost params dict."""
    objective = p.get("objective", "binary")
    metric = p.get("metric", "auc")

    # Map generic objective names to XGBoost-native names.
    xgb_objective = {
        "binary": "binary:logistic",
        "regression": "reg:squarederror",
        "multiclass": "multi:softprob",
    }.get(objective, "binary:logistic")

    # Map generic metric names to XGBoost-native names.
    if objective == "multiclass" and metric in ("auc", "logloss"):
        xgb_eval_metric = "mlogloss"
    elif objective == "regression" and metric in ("auc", "logloss"):
        xgb_eval_metric = "rmse"
    elif objective == "binary" and metric == "logloss":
        xgb_eval_metric = "logloss"
    else:
        xgb_eval_metric = {
            "auc": "auc",
            "logloss": "logloss",
            "rmse": "rmse",
        }.get(metric, metric)

    return {
        "objective": xgb_objective,
        "eval_metric": xgb_eval_metric,
        "max_depth": int(p.get("max_depth", 6)),
        "learning_rate": float(p.get("learning_rate", 0.1)),
        "subsample": float(p.get("bagging_fraction", 0.9)),
        "colsample_bytree": float(p.get("feature_fraction", 0.9)),
        "min_child_weight": int(p.get("min_child_weight", 1)),
        "reg_lambda": float(p.get("l2_regularization", 1.0)),
        "verbosity": 0,
    }


def train(config: dict) -> None:
    train_input_path = config["train_input_path"]
    model_dir = config["model_dir"]
    base_model_dir = config.get("base_model_dir", "")
    label_col = config["label_columns"]
    feature_cols = list(config.get("feature_columns", []))
    params_block = config.get("params", {})

    logger.info("Loading training data from %s", train_input_path)
    table = common.read_parquet_table(train_input_path)
    df: pd.DataFrame = common.to_pandas(table)
    logger.info("Loaded %d rows, columns=%s", len(df), list(df.columns))

    if label_col not in df.columns:
        raise ValueError(f"Label column '{label_col}' not found in training data")
    if not feature_cols:
        feature_cols = [c for c in df.columns if c != label_col]
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Feature columns missing in data: {missing}")

    X = df[feature_cols]
    y = df[label_col]

    # Validate feature types: XGBoost only accepts float/double columns.
    non_float_cols = [
        c for c in feature_cols
        if not pd.api.types.is_float_dtype(X[c])
    ]
    if non_float_cols:
        raise ValueError(
            f"XGBoost only supports float/double features, but columns {non_float_cols} "
            f"have non-float dtypes: {', '.join(f'{c}={X[c].dtype}' for c in non_float_cols)}. "
            f"Consider using CatBoost for categorical/integer features."
        )

    # Validate that no feature column contains array/list data.
    for c in feature_cols:
        if X[c].dtype == object or X[c].dtype.name == "category":
            sample = X[c].dropna().iloc[0] if not X[c].dropna().empty else None
            if isinstance(sample, (list, tuple, np.ndarray)):
                raise ValueError(
                    f"XGBoost does not support array-type features, but column '{c}' "
                    f"contains {type(sample).__name__} values"
                )

    params = _build_xgb_params(params_block)
    num_iterations = int(params_block.get("num_iterations", 100))

    # Load base model for incremental training (FROM 'checkpoint' syntax).
    init_model = None
    if base_model_dir:
        base_path = f"{base_model_dir}/model.ubj"
        logger.info("Loading base model from %s for incremental training", base_path)
        fd, tmp_base = tempfile.mkstemp(suffix=".ubj")
        os.close(fd)
        try:
            common.copy_file(base_path, tmp_base)
            init_model = xgb.Booster()
            init_model.load_model(tmp_base)
        finally:
            os.remove(tmp_base)

    if params.get("objective") == "multi:softprob":
        num_class = int(y.nunique())
        params["num_class"] = num_class
        logger.info("Multiclass: %d classes detected from label data", num_class)

    logger.info("Training XGBoost with params=%s, num_iterations=%d", params, num_iterations)

    # onnxmltools's XGBoost converter requires feature names in 'f0, f1, ...'
    # format. Rename the DataFrame columns before creating DMatrix; the original
    # column names are preserved in schema.json for the serving side.
    xgb_feature_names = [f"f{i}" for i in range(len(feature_cols))]
    X = X.copy()
    X.columns = xgb_feature_names

    dtrain = xgb.DMatrix(X, label=y)
    booster = xgb.train(params, dtrain, num_boost_round=num_iterations, xgb_model=init_model)

    # Save model in UBJ (Universal Binary JSON) format — XGBoost's recommended
    # cross-platform binary format, directly loadable by onnxmltools for export.
    native_model_path = f"{model_dir}/model.ubj"
    logger.info("Saving XGBoost model to %s", native_model_path)
    fd, tmp_path = tempfile.mkstemp(suffix=".ubj")
    os.close(fd)
    try:
        booster.save_model(tmp_path)
        common.copy_file(tmp_path, native_model_path)
    finally:
        os.remove(tmp_path)

    schema = {
        "feature_columns": feature_cols,
        "label_columns": label_col,
        "objective": params_block.get("objective", "binary"),
    }
    common.write_text(f"{model_dir}/schema.json", json.dumps(schema, indent=2))
    logger.info("Training complete")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_config_path", required=True,
                        help="path to the JSON pipeline.config (hdfs:// or local)")
    args = parser.parse_args()
    config = common.load_pipeline_config(args.pipeline_config_path)
    train(config)


if __name__ == "__main__":
    main()
