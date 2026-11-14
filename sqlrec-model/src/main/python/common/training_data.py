"""Shared feature selection and validation for model training."""
from __future__ import annotations


def resolve_feature_columns(columns, label_col: str, feature_cols: list[str]) -> list[str]:
    if label_col not in columns:
        raise ValueError(f"Label column '{label_col}' not found in training data")
    if not feature_cols:
        feature_cols = [c for c in columns if c != label_col]
    missing = [c for c in feature_cols if c not in columns]
    if missing:
        raise ValueError(f"Feature columns missing in data: {missing}")

    if label_col in feature_cols:
        raise ValueError("Label column must not be included in feature_columns")
    return feature_cols


def validate_float_features(features, feature_cols: list[str], model_name: str) -> None:
    """Check the float-only input shared by LightGBM and XGBoost."""
    import pandas as pd

    non_float_cols = [c for c in feature_cols if not pd.api.types.is_float_dtype(features[c])]
    if non_float_cols:
        raise ValueError(
            f"{model_name} only supports float/double features, but columns {non_float_cols} "
            f"have non-float dtypes: {', '.join(f'{c}={features[c].dtype}' for c in non_float_cols)}. "
            f"Consider using CatBoost for categorical/integer features."
        )
