"""XGBoost → ONNX export entry point.

Loads the native XGBoost model from ``{base_model_dir}/model.ubj``, converts
it to ONNX via ``onnxmltools`` and writes ``{export_dir}/model.onnx``. The
schema.json is copied alongside so the serving runtime can reconstruct the
input feature order without the original pipeline.config.

Unlike ``convert_lightgbm``, ``convert_xgboost`` does not support a ``zipmap``
parameter, so the ZipMap node is stripped from the ONNX graph in a post-
processing step (``_remove_zipmap``). This ensures the C++ server receives a
float probability tensor instead of a ``seq<map>`` output.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile

import onnx
import onnxmltools
import xgboost as xgb
from onnx import TensorProto
from onnxmltools.convert.common.data_types import FloatTensorType

from common import filesystem as common

logger = logging.getLogger("gbdt.export_xgboost")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _remove_zipmap(onnx_model) -> None:
    """Strip ZipMap nodes so the probability output is a float tensor.

    ``convert_xgboost`` always wraps classifier probabilities in a ZipMap
    operator, producing a ``seq<map>`` output. This removes the ZipMap node
    and replaces the graph output with the raw float probability tensor
    (shape ``(batch, n_classes)``) that was the ZipMap's input.

    No-op when the model has no ZipMap (e.g. regression models).
    """
    graph = onnx_model.graph

    # Find ZipMap nodes: map their seq<map> output name → float-tensor input.
    zipmap_io: dict[str, str] = {}
    for node in graph.node:
        if node.op_type == "ZipMap":
            zipmap_io[node.output[0]] = node.input[0]

    if not zipmap_io:
        return

    # Delete ZipMap nodes (backwards to keep indices valid).
    for i in range(len(graph.node) - 1, -1, -1):
        if graph.node[i].op_type == "ZipMap":
            del graph.node[i]

    # Replace seq<map> graph outputs with float-tensor outputs using onnx.helper.
    new_outputs = []
    for out in graph.output:
        if out.name not in zipmap_io:
            new_outputs.append(out)
            continue
        # Create a float tensor output with dynamic (batch, n_classes) shape.
        new_outputs.append(onnx.helper.make_tensor_value_info(
            zipmap_io[out.name], TensorProto.FLOAT, ["batch", "classes"],
        ))
    del graph.output[:]
    graph.output.extend(new_outputs)


def export(config: dict) -> None:
    base_model_dir = config["base_model_dir"]
    export_dir = config.get("export_dir") or f"{base_model_dir}_export"

    schema_text = common.read_text(f"{base_model_dir}/schema.json")
    schema = json.loads(schema_text)
    feature_cols: list[str] = schema["feature_columns"]

    # Load the XGBoost model. model.ubj is stored on remote storage; copy to
    # a local temp file first because xgb.Booster.load_model() uses C-level I/O
    # that bypasses Python's open().
    logger.info("Loading XGBoost model from %s/model.ubj", base_model_dir)
    fd, tmp_model = tempfile.mkstemp(suffix=".ubj")
    os.close(fd)
    try:
        common.copy_file(f"{base_model_dir}/model.ubj", tmp_model)
        booster = xgb.Booster()
        booster.load_model(tmp_model)
    finally:
        os.remove(tmp_model)

    n_features = len(feature_cols)
    initial_types = [("input", FloatTensorType([None, n_features]))]

    logger.info("Converting to ONNX (n_features=%d)", n_features)
    # convert_xgboost does not support zipmap=False (unlike convert_lightgbm).
    # The default conversion wraps classifier probabilities in a ZipMap node,
    # producing a seq<map> output. We strip it in a post-processing step so
    # the C++ server gets a float probability tensor instead.
    onnx_model = onnxmltools.convert_xgboost(
        booster, initial_types=initial_types, target_opset=15,
    )
    _remove_zipmap(onnx_model)

    # Annotate the ONNX model with feature metadata.
    meta = {
        "feature_columns": feature_cols,
        "label_columns": schema.get("label_columns"),
        "objective": schema.get("objective", "binary"),
        "framework": "xgboost",
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
