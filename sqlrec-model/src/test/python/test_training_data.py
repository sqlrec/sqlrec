"""Float-only trainers retain their input validation contract."""

from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from common import filesystem
from common.training_data import validate_float_features


class FloatFeatureValidationTest(unittest.TestCase):
    def test_float_features_and_missing_values_are_accepted_without_conversion(self):
        features = pd.DataFrame({
            "single": pd.Series([1.0, float("nan")], dtype="float32"),
            "double": pd.Series([1.0, float("nan")], dtype="float64"),
            "nullable": pd.Series([1.0, None], dtype="Float32"),
        })
        original = features.copy()
        for name in ("LightGBM", "XGBoost"):
            validate_float_features(features, list(features.columns), name)
        pd.testing.assert_frame_equal(features, original)

    def test_both_trainers_reject_non_float_features_before_training(self):
        features = pd.DataFrame({"id": [1, 2], "price": [1.0, 2.0],
                                 "flag": [True, False], "label": [0, 1]})
        config = {"train_input_path": "/data", "model_dir": "/model",
                  "label_columns": "label", "feature_columns": ["flag", "price", "id"]}
        for framework, name, constructor in (("lightgbm", "LightGBM", "Dataset"),
                                              ("xgboost", "XGBoost", "DMatrix")):
            with self.subTest(framework=framework):
                library = types.ModuleType(framework)
                build_inputs = Mock()
                setattr(library, constructor, build_inputs)
                path = Path(__file__).resolve().parents[2] / f"main/python/gbdt/train_{framework}.py"
                with patch.dict(sys.modules, {framework: library}), patch("logging.basicConfig"):
                    module = runpy.run_path(str(path))
                with (patch.object(filesystem, "read_parquet_table"),
                      patch.object(filesystem, "to_pandas", return_value=features)):
                    with self.assertRaises(ValueError) as raised:
                        module["train"](config)
                self.assertEqual(str(raised.exception),
                                 f"{name} only supports float/double features, but columns ['flag', 'id'] "
                                 "have non-float dtypes: flag=bool, id=int64. "
                                 "Consider using CatBoost for categorical/integer features.")
                build_inputs.assert_not_called()


if __name__ == "__main__":
    unittest.main()
