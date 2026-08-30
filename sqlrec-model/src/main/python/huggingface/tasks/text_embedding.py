from __future__ import annotations

from typing import Any

import torch
import torch.nn.functional as functional
from transformers import AutoModel, AutoTokenizer

from .base import TaskAdapter, bool_value, int_value


class TextEmbeddingAdapter(TaskAdapter):
    def __init__(self, model_dir: str, config: dict[str, Any]):
        super().__init__(model_dir, config)
        self.text_column = config["text_column"]
        self.pooling = config.get("pooling", "mean")
        self.normalize = bool_value(config, "normalize", True)
        self.max_length = int_value(config, "max_length", 512)
        common = {"local_files_only": True, "trust_remote_code": self.trust_remote_code}
        self.tokenizer = AutoTokenizer.from_pretrained(model_dir, **common)
        self.model = AutoModel.from_pretrained(model_dir, **self._model_kwargs()).to(self.device).eval()

    def predict(self, rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
        texts = [str(row[self.text_column]) for row in rows]
        inputs = self.tokenizer(
            texts, padding=True, truncation=True, max_length=self.max_length,
            return_tensors="pt",
        ).to(self.device)
        with torch.inference_mode():
            outputs = self.model(**inputs)
        hidden = outputs.last_hidden_state
        mask = inputs["attention_mask"]
        if self.pooling == "cls":
            embeddings = hidden[:, 0]
        elif self.pooling == "last_token":
            indices = mask.sum(dim=1) - 1
            embeddings = hidden[torch.arange(hidden.shape[0], device=hidden.device), indices]
        elif self.pooling == "pooler":
            if getattr(outputs, "pooler_output", None) is None:
                raise ValueError("pooling=pooler requested but the model has no pooler_output")
            embeddings = outputs.pooler_output
        else:
            expanded = mask.unsqueeze(-1).to(hidden.dtype)
            embeddings = (hidden * expanded).sum(dim=1) / expanded.sum(dim=1).clamp(min=1e-9)
        if self.normalize:
            embeddings = functional.normalize(embeddings.float(), p=2, dim=1)
        return {"embedding": embeddings.float().cpu().tolist()}
