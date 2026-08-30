from __future__ import annotations

from typing import Any

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from .base import TaskAdapter, int_value


class TextClassificationAdapter(TaskAdapter):
    def __init__(self, model_dir: str, config: dict[str, Any]):
        super().__init__(model_dir, config)
        self.text_column = config["text_column"]
        self.text_pair_column = config.get("text_pair_column") or None
        self.max_length = int_value(config, "max_length", 512)
        common = {"local_files_only": True, "trust_remote_code": self.trust_remote_code}
        self.tokenizer = AutoTokenizer.from_pretrained(model_dir, **common)
        self.model = AutoModelForSequenceClassification.from_pretrained(
            model_dir, **self._model_kwargs()
        ).to(self.device).eval()

    def predict(self, rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
        texts = [str(row[self.text_column]) for row in rows]
        pairs = [str(row[self.text_pair_column]) for row in rows] if self.text_pair_column else None
        inputs = self.tokenizer(
            texts, pairs, padding=True, truncation=True, max_length=self.max_length,
            return_tensors="pt",
        ).to(self.device)
        with torch.inference_mode():
            logits = self.model(**inputs).logits.float()
        if logits.shape[-1] == 1:
            scores = torch.sigmoid(logits[:, 0])
            indices = torch.zeros_like(scores, dtype=torch.long)
        else:
            probabilities = torch.softmax(logits, dim=-1)
            scores, indices = probabilities.max(dim=-1)
        id2label = self.model.config.id2label or {}
        labels = [id2label.get(int(index), f"LABEL_{int(index)}") for index in indices.cpu()]
        return {"label": labels, "score": scores.cpu().tolist()}
