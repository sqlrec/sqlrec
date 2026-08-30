from __future__ import annotations

from typing import Any

import torch


def bool_value(config: dict[str, Any], key: str, default: bool) -> bool:
    value = config.get(key, default)
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("true", "1", "yes", "on")


def int_value(config: dict[str, Any], key: str, default: int) -> int:
    return int(config.get(key, default))


def float_value(config: dict[str, Any], key: str, default: float) -> float:
    return float(config.get(key, default))


def resolve_device(config: dict[str, Any]) -> torch.device:
    requested = config.get("device", "auto")
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("device=cuda was requested but CUDA is unavailable")
    if requested == "auto":
        requested = "cuda" if torch.cuda.is_available() else "cpu"
    return torch.device(requested)


def resolve_dtype(config: dict[str, Any], device: torch.device):
    name = config.get("dtype", "auto")
    if name == "auto":
        if device.type == "cpu":
            return torch.float32
        return torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16
    return {
        "float32": torch.float32,
        "float16": torch.float16,
        "bfloat16": torch.bfloat16,
    }[name]


class TaskAdapter:
    def __init__(self, model_dir: str, config: dict[str, Any]):
        self.model_dir = model_dir
        self.config = config
        self.device = resolve_device(config)
        self.dtype = resolve_dtype(config, self.device)
        self.trust_remote_code = bool_value(config, "trust_remote_code", False)

    def predict(self, rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
        raise NotImplementedError

    def _model_kwargs(self) -> dict[str, Any]:
        return {
            "local_files_only": True,
            "trust_remote_code": self.trust_remote_code,
            "torch_dtype": self.dtype,
        }
