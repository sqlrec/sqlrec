"""CatBoost model export entry point.

Copies the native CatBoost model (``model.cbm``) and schema.json from the
training directory to the export directory. CatBoost ONNX export does not
support categorical features, so we serve via the native ``.cbm`` format
using the CatBoost C API directly.
"""
from __future__ import annotations

import argparse
import json
import logging

from common import filesystem as common

logger = logging.getLogger("gbdt.export_catboost")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def export(config: dict) -> None:
    base_model_dir = config["base_model_dir"]
    export_dir = config.get("export_dir") or f"{base_model_dir}_export"

    schema_text = common.read_text(f"{base_model_dir}/schema.json")
    schema = json.loads(schema_text)
    feature_cols: list[str] = schema["feature_columns"]
    categorical_features: list[str] = schema.get("categorical_features", [])

    cbm_path = f"{base_model_dir}/model.cbm"
    export_cbm_path = f"{export_dir}/model.cbm"
    logger.info("Copying CatBoost model from %s to %s", cbm_path, export_cbm_path)
    common.copy_file(cbm_path, export_cbm_path)

    meta = {
        "feature_columns": feature_cols,
        "categorical_features": categorical_features,
        "label_columns": schema.get("label_columns"),
        "objective": schema.get("objective", "binary"),
        "framework": "catboost",
    }
    common.write_text(f"{export_dir}/schema.json", json.dumps(meta, indent=2))
    logger.info("Export complete (native .cbm format, %d categorical features)",
                len(categorical_features))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_config_path", required=True,
                        help="path to the JSON pipeline.config (hdfs:// or local)")
    args = parser.parse_args()
    config = common.load_pipeline_config(args.pipeline_config_path)
    export(config)


if __name__ == "__main__":
    main()
