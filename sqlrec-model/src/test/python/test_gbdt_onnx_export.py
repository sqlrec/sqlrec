"""GBDT exporters preserve the ONNX artifact and serving metadata contract."""
import json
from pathlib import Path
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from common import filesystem
from common.onnx_export import write_onnx_export


class GbdtOnnxExportTest(unittest.TestCase):
    def test_both_exporters_write_matching_metadata_and_keep_conversion_options(self):
        for framework in ("lightgbm", "xgboost"):
            for explicit in (True, False):
                with self.subTest(framework=framework, explicit=explicit), tempfile.TemporaryDirectory() as directory:
                    base = Path(directory) / "checkpoint"
                    base.mkdir()
                    schema = {"feature_columns": ["price", "类别", "price"]}
                    if explicit:
                        schema.update(label_columns="target", objective="regression")
                    (base / "schema.json").write_text(json.dumps(schema), encoding="utf-8")
                    (base / "model.txt").write_text("native model", encoding="utf-8")
                    (base / "model.ubj").write_bytes(b"native model")
                    native = Mock()
                    factory = Mock(return_value=native)
                    model = SimpleNamespace(producer_name="", doc_string="",
                                            graph=SimpleNamespace(node=[], output=[]))
                    model.SerializeToString = Mock(side_effect=lambda:
                            (model.producer_name + "\n" + model.doc_string).encode("utf-8"))
                    convert = Mock(return_value=model)
                    stubs = {
                        framework: SimpleNamespace(Booster=factory),
                        "onnxmltools": SimpleNamespace(convert_lightgbm=convert, convert_xgboost=convert),
                        "onnxmltools.convert.common.data_types": SimpleNamespace(FloatTensorType=lambda shape: shape),
                        "onnx": SimpleNamespace(TensorProto=SimpleNamespace(FLOAT=1)),
                    }
                    path = Path(__file__).resolve().parents[2] / f"main/python/gbdt/export_{framework}.py"
                    with patch.dict(sys.modules, stubs), patch("logging.basicConfig"):
                        module = runpy.run_path(str(path))
                    config = {"base_model_dir": str(base)}
                    if explicit:
                        config["export_dir"] = str(Path(directory) / "output")
                    module["export"](config)

                    expected = {
                        "feature_columns": ["price", "类别", "price"],
                        "label_columns": "target" if explicit else None,
                        "objective": "regression" if explicit else "binary",
                        "framework": framework,
                    }
                    output = Path(config.get("export_dir", str(base) + "_export"))
                    self.assertEqual((output / "schema.json").read_text(encoding="utf-8"),
                                     json.dumps(expected, indent=2))
                    self.assertEqual((output / "model.onnx").read_bytes(),
                                     ("sqlrec-gbdt-export\n" + json.dumps(expected)).encode("utf-8"))
                    options = {"initial_types": [("input", [None, 3])], "target_opset": 15}
                    if framework == "lightgbm":
                        options["zipmap"] = False
                        factory.assert_called_once_with(model_str="native model")
                    else:
                        factory.assert_called_once_with()
                        self.assertFalse(Path(native.load_model.call_args.args[0]).exists())
                    convert.assert_called_once_with(native, **options)

    def test_binary_write_failure_does_not_write_the_schema(self):
        model = SimpleNamespace(SerializeToString=lambda: b"onnx")
        with patch.object(filesystem, "write_binary", side_effect=OSError("storage unavailable")), \
                patch.object(filesystem, "write_text") as write_schema:
            with self.assertRaisesRegex(OSError, "storage unavailable"):
                write_onnx_export(model, "/output", {"feature_columns": ["x"]}, "lightgbm")
            write_schema.assert_not_called()


if __name__ == "__main__":
    unittest.main()
