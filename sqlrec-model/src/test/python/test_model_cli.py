"""Model commands retain their CLI and pass the loaded config to the right operation."""

from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import runpy
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

from common import filesystem
from common.cli import load_pipeline_config_from_args


class ModelCliTest(unittest.TestCase):
    def test_all_gbdt_entrypoints_load_config_and_call_their_operation(self):
        config = {"model_dir": "/models/测试", "feature_columns": ["price", "类别"], "params": {"depth": 6}}
        stubs = {name: types.ModuleType(name) for name in
                 ("lightgbm", "xgboost", "catboost", "numpy", "pandas", "onnx", "onnxmltools")}
        stubs["onnx"].TensorProto = types.SimpleNamespace(FLOAT=1)
        stubs["onnxmltools.convert.common.data_types"] = types.SimpleNamespace(FloatTensorType=object)
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "pipeline config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            for framework in ("lightgbm", "xgboost", "catboost"):
                for operation in ("train", "export"):
                    with self.subTest(framework=framework, operation=operation):
                        script = f"{operation}_{framework}.py"
                        path = Path(__file__).resolve().parents[2] / f"main/python/gbdt/{script}"
                        with patch.dict(sys.modules, stubs), patch("logging.basicConfig"):
                            module = runpy.run_path(str(path))
                        action = Mock()
                        with (patch.dict(module["main"].__globals__, {operation: action}),
                              patch.object(sys, "argv", [script, "--pipeline_config_path", str(config_path)])):
                            module["main"]()
                        action.assert_called_once_with(config)

    def test_help_and_invalid_arguments_exit_before_reading_the_config(self):
        cases = (
            (["--help"], 0, "usage: train_xgboost.py"),
            ([], 2, "the following arguments are required: --pipeline_config_path"),
            (["--pipeline_config_path", "/config", "--unknown"], 2, "unrecognized arguments: --unknown"),
        )
        for args, code, message in cases:
            with self.subTest(args=args):
                stdout, stderr = io.StringIO(), io.StringIO()
                with (patch.object(sys, "argv", ["train_xgboost.py", *args]),
                      patch.object(filesystem, "load_pipeline_config") as load,
                      redirect_stdout(stdout), redirect_stderr(stderr)):
                    with self.assertRaises(SystemExit) as raised:
                        load_pipeline_config_from_args()
                self.assertEqual(raised.exception.code, code)
                self.assertIn(message, stdout.getvalue() + stderr.getvalue())
                load.assert_not_called()


if __name__ == "__main__":
    unittest.main()
