"""Model execution shared by SQLRec and OpenAI HTTP endpoints."""
from __future__ import annotations

from contextlib import contextmanager
import logging
import threading
from typing import Any

logger = logging.getLogger("sqlrec.huggingface.runtime")
SERVICE_PARAMS = ("served_model_name", "openai_api_key_secret", "openai_api_key_secret_key",
                  "openai_max_request_bytes", "openai_max_batch_inputs")


class ModelBusy(Exception):
    pass


class ModelRuntime:
    def __init__(self, adapter_types):
        self.adapter_types = adapter_types
        self.adapter = None
        self.batch_size = 8
        self._slot = threading.Lock()

    def configure(self, manifest, service_config):
        if service_config.get("task", manifest["task"]) != manifest["task"]:
            raise RuntimeError("checkpoint task does not match the declared service task; create a new model instead")
        config = dict(manifest.get("model_params", {}))
        for key in SERVICE_PARAMS:
            config.pop(key, None)
        config.update(service_config)
        config["task"] = manifest["task"]
        config["trust_remote_code"] = manifest.get("trust_remote_code", False)
        adapter_type = self.adapter_types.get(manifest["task"])
        if adapter_type is None:
            raise RuntimeError(f"unsupported task in checkpoint: {manifest['task']}")
        self.batch_size = max(1, int(config.get("inference_batch_size", 8)))
        self.config = config
        self.manifest = manifest

    def load(self, model_dir):
        manifest = self.manifest
        logger.info("Loading task=%s repo=%s revision=%s", manifest["task"],
                    manifest.get("repo_id"), manifest.get("resolved_revision"))
        self.adapter = self.adapter_types[manifest["task"]](model_dir, self.config)

    @contextmanager
    def inference(self):
        if not self._slot.acquire(blocking=False):
            raise ModelBusy("model is busy")
        try:
            yield
        finally:
            self._slot.release()

    def predict(self, rows):
        merged: dict[str, list[Any]] = {}
        with self.inference():
            for start in range(0, len(rows), self.batch_size):
                result = self.adapter.predict(rows[start:start + self.batch_size])
                for name, values in result.items():
                    merged.setdefault(name, []).extend(values)
        return merged
