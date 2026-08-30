"""Download a pinned Hugging Face snapshot and create the SQLRec manifest."""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from huggingface_hub import HfApi, snapshot_download
from transformers import AutoConfig, AutoImageProcessor, AutoProcessor, AutoTokenizer


def _version(package: str) -> str:
    try:
        return importlib.metadata.version(package)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _validate_snapshot(model_dir: str, config: dict) -> None:
    trust_remote_code = bool(config.get("trust_remote_code", False))
    common = {"local_files_only": True, "trust_remote_code": trust_remote_code}
    AutoConfig.from_pretrained(model_dir, **common)
    if config["task"] == "image-embedding":
        try:
            AutoImageProcessor.from_pretrained(model_dir, **common)
        except (ValueError, OSError):
            AutoProcessor.from_pretrained(model_dir, **common)
    else:
        AutoTokenizer.from_pretrained(model_dir, **common)


def download(config: dict, output_dir: str) -> dict:
    repo_id = config["repo_id"]
    requested_revision = config.get("revision") or "main"
    token = os.environ.get("HF_TOKEN")
    api = HfApi(token=token)
    resolved_revision = api.model_info(repo_id, revision=requested_revision).sha

    output = Path(output_dir)
    shutil.rmtree(output, ignore_errors=True)
    output.mkdir(parents=True, exist_ok=True)
    snapshot_download(
        repo_id=repo_id,
        revision=resolved_revision,
        local_dir=str(output),
        allow_patterns=config.get("allow_patterns") or None,
        ignore_patterns=config.get("ignore_patterns") or None,
        force_download=bool(config.get("force_download", False)),
        token=token,
    )
    # local_dir metadata is useful only to huggingface_hub's local cache and is not a model artifact.
    shutil.rmtree(output / ".cache", ignore_errors=True)
    _validate_snapshot(str(output), config)

    manifest = {
        "framework": "huggingface.transformers",
        "task": config["task"],
        "repo_id": repo_id,
        "requested_revision": requested_revision,
        "resolved_revision": resolved_revision,
        "model_name": config.get("model_name"),
        "checkpoint_name": config.get("checkpoint_name"),
        "trust_remote_code": bool(config.get("trust_remote_code", False)),
        "input_fields": config.get("input_fields", []),
        "model_params": config.get("model_params", {}),
        "versions": {
            "transformers": _version("transformers"),
            "huggingface_hub": _version("huggingface-hub"),
            "torch": _version("torch"),
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (output / "sqlrec_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_config_path", required=True)
    parser.add_argument("--output_dir", required=True)
    args = parser.parse_args()
    with open(args.pipeline_config_path, encoding="utf-8") as stream:
        config = json.load(stream)
    download(config, args.output_dir)


if __name__ == "__main__":
    main()
