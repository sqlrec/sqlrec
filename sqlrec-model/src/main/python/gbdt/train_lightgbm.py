"""LightGBM training entry point.

Reads the JSON pipeline.config, loads parquet training data, trains a LightGBM
model and persists the native model file to ``{model_dir}/model.txt``. The
trained model is later picked up by ``export_lightgbm`` to produce an ONNX
artifact for serving.
"""
from __future__ import annotations

import argparse
import json
import logging

import lightgbm as lgb
import pandas as pd

from common import filesystem as common

logger = logging.getLogger("gbdt.train_lightgbm")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _build_lgb_params(p: dict) -> dict:
    """Translate the JSON params block into a LightGBM params dict."""
    objective = p.get("objective", "binary")
    metric = p.get("metric", "auc")
    # LightGBM uses different metric names than our generic config:
    #   "logloss" → "binary_logloss" (binary) / "multi_logloss" (multiclass)
    #   "auc" is binary-only; regression uses "rmse"
    if objective == "multiclass" and metric in ("auc", "binary_logloss", "logloss"):
        metric = "multi_logloss"
    elif objective == "regression" and metric in ("auc", "binary_logloss", "logloss"):
        metric = "rmse"
    elif objective == "binary" and metric == "logloss":
        metric = "binary_logloss"
    params = {
        "objective": objective,
        "metric": metric,
        "num_leaves": int(p.get("num_leaves", 63)),
        "max_depth": int(p.get("max_depth", 6)),
        "learning_rate": float(p.get("learning_rate", 0.1)),
        "feature_fraction": float(p.get("feature_fraction", 0.9)),
        "bagging_fraction": float(p.get("bagging_fraction", 0.9)),
        "bagging_freq": int(p.get("bagging_freq", 5)),
        "min_data_in_leaf": int(p.get("min_data_in_leaf", 20)),
        "lambda_l2": float(p.get("l2_regularization", 1.0)),
        "verbose": -1,
    }
    return params


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

    # Validate feature types: LightGBM only accepts float/double columns.
    non_float_cols = [
        c for c in feature_cols
        if not pd.api.types.is_float_dtype(X[c])
    ]
    if non_float_cols:
        raise ValueError(
            f"LightGBM only supports float/double features, but columns {non_float_cols} "
            f"have non-float dtypes: {', '.join(f'{c}={X[c].dtype}' for c in non_float_cols)}. "
            f"Consider using CatBoost for categorical/integer features."
        )

    # Load base model for incremental training (FROM 'checkpoint' syntax).
    init_model = None
    if base_model_dir:
        base_path = f"{base_model_dir}/model.txt"
        logger.info("Loading base model from %s for incremental training", base_path)
        base_text = common.read_text(base_path)
        init_model = lgb.Booster(model_str=base_text)

    params = _build_lgb_params(params_block)
    if params.get("objective") == "multiclass":
        num_class = int(y.nunique())
        params["num_class"] = num_class
        logger.info("Multiclass: %d classes detected from label data", num_class)
    num_iterations = int(params_block.get("num_iterations", 100))
    logger.info("Training LightGBM with params=%s, num_iterations=%d", params, num_iterations)

    train_set = lgb.Dataset(X, label=y)
    booster = lgb.train(params, train_set, num_boost_round=num_iterations, init_model=init_model)

    native_model_path = f"{model_dir}/model.txt"
    logger.info("Saving LightGBM model to %s", native_model_path)
    model_text = booster.model_to_string()
    common.write_text(native_model_path, model_text)

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
