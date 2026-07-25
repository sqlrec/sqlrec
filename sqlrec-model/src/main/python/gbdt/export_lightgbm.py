"""LightGBM → ONNX export entry point.

Loads the native LightGBM model from ``{base_model_dir}/model.txt``, converts
it to ONNX via ``onnxmltools`` and writes ``{export_dir}/model.onnx``. The
schema.json is copied alongside so the serving runtime can reconstruct the
input feature order without the original pipeline.config.
"""
from __future__ import annotations

import argparse
import json
import logging

import lightgbm as lgb
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType

from common import filesystem as common

logger = logging.getLogger("gbdt.export_lightgbm")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def export(config: dict) -> None:
    base_model_dir = config["base_model_dir"]
    export_dir = config.get("export_dir") or f"{base_model_dir}_export"

    schema_text = common.read_text(f"{base_model_dir}/schema.json")
    schema = json.loads(schema_text)
    feature_cols: list[str] = schema["feature_columns"]

    logger.info("Loading LightGBM model from %s/model.txt", base_model_dir)
    model_text = common.read_text(f"{base_model_dir}/model.txt")
    booster = lgb.Booster(model_str=model_text)

    n_features = len(feature_cols)
    initial_types = [("input", FloatTensorType([None, n_features]))]

    logger.info("Converting to ONNX (n_features=%d)", n_features)
    # zipmap=False emits a float probability tensor of shape (batch, n_classes)
    # instead of a seq<map>. The C++ server can only read tensor outputs, so
    # this is required — otherwise it would read output 0 (the int64 label)
    # and return 0.0/1.0 as "probabilities".
    onnx_model = onnxmltools.convert_lightgbm(
        booster, initial_types=initial_types, target_opset=15, zipmap=False,
    )

    # Annotate the ONNX model with feature metadata.
    meta = {
        "feature_columns": feature_cols,
        "label_columns": schema.get("label_columns"),
        "objective": schema.get("objective", "binary"),
        "framework": "lightgbm",
    }
    onnx_model.producer_name = "sqlrec-gbdt-export"
    onnx_model.doc_string = json.dumps(meta)

    onnx_bytes = onnx_model.SerializeToString()
    onnx_path = f"{export_dir}/model.onnx"
    logger.info("Writing ONNX model to %s (%d bytes)", onnx_path, len(onnx_bytes))
    common.write_binary(onnx_path, onnx_bytes)

    common.write_text(f"{export_dir}/schema.json", json.dumps(meta, indent=2))
    logger.info("Export complete")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_config_path", required=True,
                        help="path to the JSON pipeline.config (hdfs:// or local)")
    args = parser.parse_args()
    config = common.load_pipeline_config(args.pipeline_config_path)
    export(config)


if __name__ == "__main__":
    main()
