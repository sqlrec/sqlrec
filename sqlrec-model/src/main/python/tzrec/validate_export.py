"""Validate complete exports while retaining compatibility with legacy model directories."""

import hashlib
import argparse
import json
import os
from pathlib import Path
import sys


REQUIRED_FILES = ("scripted_model.pt", "fg.json", "pipeline.config")


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def validate_export(directory: str, pipeline_labels=None, pipeline_contract=None) -> None:
    root = Path(directory)
    for name in REQUIRED_FILES:
        if not (root / name).is_file():
            raise ValueError(f"Export is missing {name}: {root}")
    features = json.loads((root / "fg.json").read_text())["features"]
    if pipeline_labels is not None:
        labels = set(pipeline_labels)
        if any(feature["feature_name"] in labels or feature["expression"].split(":", 1)[-1] in labels for feature in features):
            raise ValueError("Model contains label features; retrain with labels excluded")
    if any("hash_bucket_size" in feature for feature in features) and os.getenv("USE_FARM_HASH_TO_BUCKETIZE", "true").lower() != "true":
        raise ValueError("Hash features require USE_FARM_HASH_TO_BUCKETIZE=true")
    manifest = root / "model_meta.json"
    if not manifest.exists():
        if pipeline_contract is not None:
            raise ValueError("Model export requires an output/task manifest")
        return
    if not (root / "_SUCCESS").is_file():
        raise ValueError("Model export is incomplete: missing _SUCCESS")
    metadata = json.loads(manifest.read_text())
    if metadata.get("format_version") != 1 or set(metadata.get("sha256", {})) != set(REQUIRED_FILES):
        raise ValueError("Invalid model export manifest")
    if pipeline_labels is not None and list(pipeline_labels) != metadata.get("labels"):
        raise ValueError("Model export label_fields do not match pipeline.config")
    if pipeline_contract is not None:
        architecture = pipeline_contract.get("architecture", "mmoe")
        if any(metadata.get(key) != value for key, value in pipeline_contract.items()) or metadata.get("architecture") != architecture:
            raise ValueError("Model export output/task contract does not match pipeline.config")
    for name, expected in metadata["sha256"].items():
        if digest(root / name) != expected:
            raise ValueError(f"Model export checksum mismatch: {name}")
    labels = set(metadata.get("labels", []))
    if any(feature["feature_name"] in labels or feature["expression"].split(":", 1)[-1] in labels for feature in features):
        raise ValueError("Model contains label features; retrain with labels excluded")


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("directory")
        parser.add_argument("--check-pipeline", action="store_true")
        args = parser.parse_args()
        labels = None
        contract = None
        if args.check_pipeline:
            from tzrec.utils import config_util
            pipeline = config_util.load_pipeline_config(str(Path(args.directory) / "pipeline.config"), allow_unknown_field=True)
            labels = list(pipeline.data_config.label_fields)
            from prediction_contract import export_contract
            contract = export_contract(pipeline)
        validate_export(args.directory, labels, contract)
    except Exception as error:
        sys.exit(f"TZRec export validation failed: {error}")
