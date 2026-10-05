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
from multi_task import task_contract, validate_scripted_export
from pipeline_config import prepare_config


def publish_export(staging: str, target: str, config) -> None:
    filesystem, stage_path = fsspec.core.url_to_fs(staging)
    _, target_path = fsspec.core.url_to_fs(target)
    directories = [staging] if filesystem.exists(stage_path + "/scripted_model.pt") else [staging + "/user", staging + "/item"]
    contract = task_contract(config)
    if contract is not None:
        # TorchScript needs a local directory; metadata is published only after this succeeds.
        with tempfile.TemporaryDirectory(prefix="sqlrec-mmoe-export-") as temporary:
            common.download_dir(staging, temporary)
            validate_scripted_export(temporary, contract)
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
        if contract is not None:
            metadata.update(contract)
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
    parser.add_argument("--check_checkpoint_structure", action="store_true")
    parser.add_argument("--checkpoint_structure_options", default="{}")
    args = parser.parse_args()
    os.environ.setdefault("USE_FARM_HASH_TO_BUCKETIZE", "true")
    if args.mode == "export" and (int(os.getenv("WORLD_SIZE", "1")) != 1 or int(os.getenv("RANK", "0")) != 0):
        raise ValueError("TZRec export requires a single process")
    requested = config_util.load_pipeline_config(args.pipeline_config_path)
    source = requested.model_dir if args.mode == "export" else requested.train_config.fine_tune_checkpoint
    saved = None
    if source:
        saved = text_format.Parse(common.read_text(source.rstrip("/") + "/pipeline.config"), pipeline_pb2.EasyRecConfig())
    structure = json.loads(args.checkpoint_structure_options)
    config = prepare_config(requested, saved, check_checkpoint_structure=args.check_checkpoint_structure,
                            structure_options=structure.get("options"), structure_masks=structure.get("masked_features"))
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
