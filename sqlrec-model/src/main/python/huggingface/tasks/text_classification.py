from __future__ import annotations

from typing import Any

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from .base import TaskAdapter, int_value, text_values


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
        problem_type = self.model.config.problem_type
        if problem_type == "regression":
            raise ValueError("text-classification requires a classification checkpoint; regression outputs cannot be served as label/score")

    def predict(self, rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
        texts = text_values(rows, self.text_column)
        pairs = text_values(rows, self.text_pair_column) if self.text_pair_column else None
        inputs = self.tokenizer(
            texts, pairs, padding=True, truncation=True, max_length=self.max_length,
            return_tensors="pt",
        ).to(self.device)
        with torch.inference_mode():
            logits = self.model(**inputs).logits.float()
        probabilities = (torch.sigmoid(logits)
                         if (self.model.config.problem_type == "multi_label_classification"
                             or self.model.config.num_labels == 1)
                         else torch.softmax(logits, dim=-1))
        scores, indices = probabilities.max(dim=-1)
        id2label = self.model.config.id2label or {}
        labels = [id2label.get(int(index), f"LABEL_{int(index)}") for index in indices.cpu()]
        return {"label": labels, "score": scores.cpu().tolist()}
