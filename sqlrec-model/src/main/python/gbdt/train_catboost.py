"""CatBoost training entry point.

Reads the JSON pipeline.config, loads parquet training data, trains a CatBoost
model and persists the native model file (``model.cbm``) to ``{model_dir}/``.
CatBoost handles categorical features natively (no need to pre-encode); the
categorical column names are passed via ``cat_features``.
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import tempfile

import catboost as cb
import numpy as np
import pandas as pd

from common import filesystem as common

logger = logging.getLogger("gbdt.train_catboost")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _build_cb_params(p: dict) -> dict:
    objective = p.get("objective", "binary")
    metric = p.get("metric", "auc")
    loss_function = {
        "binary": "Logloss",
        "regression": "RMSE",
        "multiclass": "MultiClass",
    }.get(objective, "Logloss")
    eval_metric = {
        "auc": "AUC",
        "logloss": "Logloss",
        "rmse": "RMSE",
    }.get(metric.lower(), metric)
    # AUC/Logloss are binary-only; use MultiClass for multiclass, RMSE for regression.
    if objective == "multiclass" and eval_metric in ("AUC", "Logloss"):
        eval_metric = "MultiClass"
    elif objective == "regression" and eval_metric in ("AUC", "Logloss"):
        eval_metric = "RMSE"

    return {
        "loss_function": loss_function,
        "eval_metric": eval_metric,
        "iterations": int(p.get("iterations", 1000)),
        "depth": int(p.get("depth", 6)),
        "learning_rate": float(p.get("learning_rate", 0.1)),
        "l2_leaf_reg": float(p.get("l2_leaf_reg", 3.0)),
        "verbose": False,
        "allow_writing_files": False,
    }


def train(config: dict) -> None:
    train_input_path = config["train_input_path"]
    model_dir = config["model_dir"]
    base_model_dir = config.get("base_model_dir", "")
    label_col = config["label_columns"]
    feature_cols = list(config.get("feature_columns", []))
    categorical_features = list(config.get("categorical_features", []))
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

    X = df[feature_cols].copy()
    y = df[label_col]

    # Validate that no feature column contains array/list data (CatBoost does not support it).
    for c in feature_cols:
        if X[c].dtype == object or X[c].dtype.name == "category":
            sample = X[c].dropna().iloc[0] if not X[c].dropna().empty else None
            if isinstance(sample, (list, tuple, np.ndarray)):
                raise ValueError(
                    f"CatBoost does not support array-type features, but column '{c}' "
                    f"contains {type(sample).__name__} values"
                )

    # Categorical feature indices for CatBoost (int/bigint/string auto-detected
    # by checkModel on the Java side; data types are guaranteed by the input).
    cat_feature_idx: list[int] = [
        i for i, c in enumerate(feature_cols) if c in categorical_features
    ]

    # CatBoost cannot handle None in string categorical columns; replace with "".
    # int64 categorical columns have no None (types guaranteed externally).
    for i in cat_feature_idx:
        if X.iloc[:, i].dtype == object:
            X.iloc[:, i] = X.iloc[:, i].fillna("")

    params = _build_cb_params(params_block)
    train_pool = cb.Pool(X, label=y, cat_features=cat_feature_idx)

    # Load base model for incremental training (FROM 'checkpoint' syntax).
    init_model = None
    if base_model_dir:
        base_path = f"{base_model_dir}/model.cbm"
        logger.info("Loading base model from %s for incremental training", base_path)
        base_bytes = common.read_binary(base_path)
        init_model = cb.CatBoost()
        init_model.load_model(io.BytesIO(base_bytes))

    model = cb.CatBoost(params)

    logger.info("Training CatBoost with params=%s, cat_features=%s", params, cat_feature_idx)
    model.fit(train_pool, init_model=init_model)

    native_model_path = f"{model_dir}/model.cbm"
    logger.info("Saving CatBoost model to %s", native_model_path)
    # save_model only accepts a file path, not a file-like object.
    fd, tmp_path = tempfile.mkstemp(suffix=".cbm")
    os.close(fd)
    try:
        model.save_model(tmp_path)
        common.copy_file(tmp_path, native_model_path)
    finally:
        os.remove(tmp_path)

    schema = {
        "feature_columns": feature_cols,
        "categorical_features": categorical_features,
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
