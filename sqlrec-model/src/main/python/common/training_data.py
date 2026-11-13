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
