from __future__ import annotations

from typing import Any

import torch
import torch.nn.functional as functional
from transformers import AutoImageProcessor, AutoModel, AutoProcessor

from huggingface.image_loader import ImageUrlLoader
from .base import TaskAdapter, bool_value, int_value


class ImageEmbeddingAdapter(TaskAdapter):
    def __init__(self, model_dir: str, config: dict[str, Any]):
        super().__init__(model_dir, config)
        self.image_column = config["image_column"]
        self.pooling = config.get("pooling", "cls")
        self.normalize = bool_value(config, "normalize", True)
        common = {"local_files_only": True, "trust_remote_code": self.trust_remote_code}
        try:
            self.processor = AutoImageProcessor.from_pretrained(model_dir, **common)
        except (ValueError, OSError):
            self.processor = AutoProcessor.from_pretrained(model_dir, **common)
        self.model = AutoModel.from_pretrained(model_dir, **self._model_kwargs()).to(self.device).eval()
        self.loader = ImageUrlLoader(
            str(config.get("image_url_allowed_hosts", "")),
            int_value(config, "image_download_timeout_ms", 5000),
            int_value(config, "image_max_bytes", 10 * 1024 * 1024),
            int_value(config, "image_max_pixels", 20_000_000),
        )

    def predict(self, rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
        images = [self.loader.load(str(row[self.image_column])) for row in rows]
        inputs = self.processor(images=images, return_tensors="pt")
        inputs = {name: value.to(self.device) if hasattr(value, "to") else value
                  for name, value in inputs.items()}
        with torch.inference_mode():
            if hasattr(self.model, "get_image_features"):
                embeddings = self.model.get_image_features(**inputs)
            else:
                outputs = self.model(**inputs)
                if self.pooling == "pooler":
                    embeddings = getattr(outputs, "pooler_output", None)
                    if embeddings is None:
                        raise ValueError("pooling=pooler requested but the model has no pooler_output")
                elif self.pooling == "mean":
                    embeddings = outputs.last_hidden_state.mean(dim=1)
                else:
                    embeddings = outputs.last_hidden_state[:, 0]
        if self.normalize:
            embeddings = functional.normalize(embeddings.float(), p=2, dim=1)
        return {"embedding": embeddings.float().cpu().tolist()}
