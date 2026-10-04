"""Handwritten training configs cannot use labels as GBDT inputs."""

from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import patch

from common import filesystem


class LabelLeakageTest(unittest.TestCase):
    def test_all_frameworks_reject_label_before_constructing_training_inputs(self):
        class Frame:
            columns = ["feature", "label"]

            def __len__(self):
                return 2

            def __getitem__(self, key):
                raise AssertionError("Training input must not be constructed from leaked features")

        config = {"train_input_path": "/data", "model_dir": "/model",
                  "label_columns": "label", "feature_columns": ["feature", "label"]}
        for framework in ("lightgbm", "xgboost", "catboost"):
            with self.subTest(framework=framework):
                stubs = {name: types.ModuleType(name) for name in (framework, "pandas", "numpy")}
                with patch.dict(sys.modules, stubs), patch("logging.basicConfig"):
                    module = runpy.run_path(str(Path(__file__).resolve().parents[2]
                                              / f"main/python/gbdt/train_{framework}.py"))
                with (patch.object(filesystem, "read_parquet_table", return_value=object()),
                      patch.object(filesystem, "to_pandas", return_value=Frame())):
                    with self.assertRaisesRegex(ValueError, "Label column must not"):
                        module["train"](config)


if __name__ == "__main__":
    unittest.main()
