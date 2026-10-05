"""Training must see the same float32 feature values as ONNX serving."""

from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
from common import filesystem


class LightGBMPrecisionTest(unittest.TestCase):
    def train(self, values):
        library = types.ModuleType("lightgbm")
        library.Dataset = Mock()
        library.train = Mock(return_value=Mock(model_to_string=lambda: "model"))
        with patch.dict(sys.modules, {"lightgbm": library}), patch("logging.basicConfig"):
            module = runpy.run_path(str(Path(__file__).resolve().parents[2] / "main/python/gbdt/train_lightgbm.py"))
        frame = pd.DataFrame({"feature": values, "label": [0] * len(values)})
        with (patch.object(filesystem, "read_parquet_table"),
              patch.object(filesystem, "to_pandas", return_value=frame),
              patch.object(filesystem, "write_text")):
            module["train"]({"train_input_path": "/data", "model_dir": "/model",
                             "label_columns": "label", "feature_columns": ["feature"]})
        return library.Dataset.call_args.args[0]

    def test_double_values_are_narrowed_before_the_training_dataset_is_constructed(self):
        features = self.train([1., 1.00000004, float("nan")])
        self.assertEqual(features["feature"].dtype, np.dtype("float32"))
        self.assertEqual(features.iloc[0, 0], features.iloc[1, 0])
        self.assertTrue(np.isnan(features.iloc[2, 0]))

    def test_float32_overflow_and_infinities_fail_before_training(self):
        for value in (1e40, float("inf"), -float("inf")):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "finite float32"):
                self.train([value])


if __name__ == "__main__":
    unittest.main()
