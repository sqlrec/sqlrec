"""Write the ONNX artifact and serving metadata shared by GBDT exporters."""
from __future__ import annotations

import json
import logging

from common import filesystem as common


def write_onnx_export(onnx_model, export_dir: str, schema: dict, framework: str) -> None:
    meta = {
        "feature_columns": schema["feature_columns"],
        "label_columns": schema.get("label_columns"),
        "objective": schema.get("objective", "binary"),
        "framework": framework,
    }
    onnx_model.producer_name = "sqlrec-gbdt-export"
    onnx_model.doc_string = json.dumps(meta)

    onnx_bytes = onnx_model.SerializeToString()
    onnx_path = f"{export_dir}/model.onnx"
    logger = logging.getLogger(f"gbdt.export_{framework}")
    logger.info("Writing ONNX model to %s (%d bytes)", onnx_path, len(onnx_bytes))
    common.write_binary(onnx_path, onnx_bytes)

    common.write_text(f"{export_dir}/schema.json", json.dumps(meta, indent=2))
    logger.info("Export complete")
