"""Train/export with checkpoint-owned structure and complete export publication."""

import argparse
import hashlib
import importlib.metadata
import json
import logging
import os
import tempfile
import uuid

from google.protobuf import text_format
import fsspec
import torch

import common
from tzrec.protos import pipeline_pb2
from tzrec.utils import config_util
from validate_export import REQUIRED_FILES


def prepare_config(requested, saved=None):
    config = pipeline_pb2.EasyRecConfig()
    config.CopyFrom(requested)
    if saved is not None:
        if list(config.data_config.label_fields) != list(saved.data_config.label_fields):
            raise ValueError("Checkpoint label_fields cannot be changed during export or fine-tuning")
        config.model_config.CopyFrom(saved.model_config)
        del config.feature_configs[:]
        config.feature_configs.extend(saved.feature_configs)
        batch_size, workers = config.data_config.batch_size, config.data_config.num_workers
        config.data_config.CopyFrom(saved.data_config)
        config.data_config.batch_size, config.data_config.num_workers = batch_size, workers
    labels = set(config.data_config.label_fields)
    if len(labels) != 1:
        raise ValueError("TZRec requires exactly one label")
    for feature in config.feature_configs:
        name = feature.WhichOneof("feature")
        spec = getattr(feature, name)
        if spec.feature_name in labels or spec.expression.split(":", 1)[-1] in labels:
            raise ValueError("Checkpoint contains label features; retrain with labels excluded")
        if name not in ("id_feature", "raw_feature"):
            raise ValueError(f"Unsupported SQLRec serving feature: {name}")
        if name == "id_feature" and spec.HasField("hash_bucket_size") and os.getenv("USE_FARM_HASH_TO_BUCKETIZE", "true").lower() != "true":
            raise ValueError("Hash features require USE_FARM_HASH_TO_BUCKETIZE=true")
    return config


def publish_export(staging: str, target: str, config) -> None:
    filesystem, stage_path = fsspec.core.url_to_fs(staging)
    _, target_path = fsspec.core.url_to_fs(target)
    directories = [staging] if filesystem.exists(stage_path + "/scripted_model.pt") else [staging + "/user", staging + "/item"]
    for directory in directories:
        hashes = {}
        for name in REQUIRED_FILES:
            digest = hashlib.sha256()
            source_fs, source_path = fsspec.core.url_to_fs(directory + "/" + name)
            with source_fs.open(source_path, "rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            hashes[name] = digest.hexdigest()
        metadata = {"format_version": 1, "sha256": hashes, "labels": list(config.data_config.label_fields),
                    "architecture": config.model_config.WhichOneof("model"),
                    "torch": torch.__version__, "tzrec": importlib.metadata.version("tzrec")}
        common.write_text(directory + "/model_meta.json", json.dumps(metadata, indent=2))
        common.write_text(directory + "/_SUCCESS", "")
    if filesystem.exists(target_path):
        raise FileExistsError(f"Export already exists: {target}")
    filesystem.mv(stage_path, target_path, recursive=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("train", "export"), required=True)
    parser.add_argument("--pipeline_config_path", required=True)
    parser.add_argument("--export_dir")
    args = parser.parse_args()
    os.environ.setdefault("USE_FARM_HASH_TO_BUCKETIZE", "true")
    if args.mode == "export" and (int(os.getenv("WORLD_SIZE", "1")) != 1 or int(os.getenv("RANK", "0")) != 0):
        raise ValueError("TZRec export requires a single process")
    requested = config_util.load_pipeline_config(args.pipeline_config_path)
    source = requested.model_dir if args.mode == "export" else requested.train_config.fine_tune_checkpoint
    saved = None
    if source:
        saved = text_format.Parse(common.read_text(source.rstrip("/") + "/pipeline.config"), pipeline_pb2.EasyRecConfig())
    config = prepare_config(requested, saved)
    with tempfile.TemporaryDirectory(prefix="sqlrec-tzrec-") as temporary:
        path = os.path.join(temporary, "pipeline.config")
        config_util.save_message(config, path)
        if args.mode == "train":
            from tzrec.main import train_and_evaluate
            changed_optimizer = saved is not None and (
                config.train_config.sparse_optimizer != saved.train_config.sparse_optimizer
                or config.train_config.dense_optimizer != saved.train_config.dense_optimizer
            )
            train_and_evaluate(path, ignore_restore_optimizer=changed_optimizer)
        else:
            if not args.export_dir:
                raise ValueError("--export_dir is required")
            filesystem, target = fsspec.core.url_to_fs(args.export_dir)
            if filesystem.exists(target):
                raise FileExistsError(f"Export already exists: {args.export_dir}")
            staging = args.export_dir + ".staging-" + uuid.uuid4().hex
            from tzrec.main import export
            try:
                export(path, export_dir=staging)
                publish_export(staging, args.export_dir, config)
            except Exception:
                _, stage_path = fsspec.core.url_to_fs(staging)
                try:
                    if filesystem.exists(stage_path):
                        filesystem.rm(stage_path, recursive=True)
                except Exception:
                    logging.exception("Failed to clean export staging directory %s", staging)
                raise


if __name__ == "__main__":
    main()
