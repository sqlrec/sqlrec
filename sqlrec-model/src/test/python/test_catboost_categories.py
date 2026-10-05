"""Exercise real Arrow/pandas conversion before handing categories to CatBoost."""

from pathlib import Path
import runpy
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq


class PoolCaptured(Exception):
    pass


class CatBoostCategoriesTest(unittest.TestCase):
    def setUp(self):
        self.backend = types.ModuleType("catboost")
        with patch.dict(sys.modules, {"catboost": self.backend}), patch("logging.basicConfig"):
            self.entry = runpy.run_path(str(Path(__file__).resolve().parents[2]
                                          / "main/python/gbdt/train_catboost.py"))

    def pool_input(self, table, categoricals):
        captured = {}

        def pool(frame, label, cat_features):
            captured.update(frame=frame, label=label, indices=cat_features)
            raise PoolCaptured

        self.backend.Pool = pool
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "train.parquet")
            pq.write_table(table, path)
            with self.assertRaises(PoolCaptured):
                self.entry["train"]({
                    "train_input_path": path, "model_dir": directory + "/model",
                    "label_columns": "label",
                    "feature_columns": [name for name in table.column_names if name != "label"],
                    "categorical_features": categoricals,
                })
        return captured

    def test_nullable_integer_and_string_categories_match_serving(self):
        table = pa.table({
            "id": pa.array([2**63 - 1, None, -(2**63)], type=pa.int64()),
            "text": ["a", None, ""],
            "numeric": pa.array([1.5, None, 0.0], type=pa.float64()),
            "label": [0, 1, 0],
        })
        captured = self.pool_input(table, ["id", "text"])
        self.assertEqual(captured["indices"], [0, 1])
        self.assertEqual(captured["frame"]["id"].tolist(), [str(2**63 - 1), "", str(-(2**63))])
        self.assertEqual(captured["frame"]["text"].tolist(), ["a", "", ""])
        self.assertTrue(captured["frame"]["numeric"].isna().iloc[1])
        self.assertEqual(captured["label"].tolist(), [0, 1, 0])

    def test_all_null_and_unsigned_categories(self):
        captured = self.pool_input(pa.table({
            "empty": pa.array([None, None], type=pa.string()),
            "unsigned": pa.array([2**64 - 1, None], type=pa.uint64()),
            "label": [0, 1],
        }), ["empty", "unsigned"])
        self.assertEqual(captured["frame"]["empty"].tolist(), ["", ""])
        self.assertEqual(captured["frame"]["unsigned"].tolist(), [str(2**64 - 1), ""])

    def test_float_categories_fail_before_pool_construction(self):
        with self.assertRaisesRegex(ValueError, "Categorical column 'category'"):
            self.pool_input(pa.table({"category": [1.0, None], "label": [0, 1]}), ["category"])


if __name__ == "__main__":
    unittest.main()
