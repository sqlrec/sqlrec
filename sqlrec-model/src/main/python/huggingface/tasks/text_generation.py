from __future__ import annotations

from typing import Any

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from .base import TaskAdapter, bool_value, float_value, int_value


class TextGenerationAdapter(TaskAdapter):
    def __init__(self, model_dir: str, config: dict[str, Any]):
        super().__init__(model_dir, config)
        self.prompt_column = config["prompt_column"]
        self.max_length = int_value(config, "max_length", 512)
        common = {"local_files_only": True, "trust_remote_code": self.trust_remote_code}
        self.tokenizer = AutoTokenizer.from_pretrained(model_dir, **common)
        if self.tokenizer.pad_token_id is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        self.tokenizer.padding_side = "left"
        self.model = AutoModelForCausalLM.from_pretrained(
            model_dir, **self._model_kwargs()
        ).to(self.device).eval()

    def predict(self, rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
        prompts = [str(row[self.prompt_column]) for row in rows]
        inputs = self.tokenizer(
            prompts, padding=True, truncation=True, max_length=self.max_length,
            return_tensors="pt",
        ).to(self.device)
        do_sample = bool_value(self.config, "do_sample", False)
        generation = {
            "max_new_tokens": int_value(self.config, "max_new_tokens", 128),
            "do_sample": do_sample,
            "pad_token_id": self.tokenizer.pad_token_id,
        }
        if do_sample:
            generation["temperature"] = float_value(self.config, "temperature", 1.0)
            generation["top_p"] = float_value(self.config, "top_p", 1.0)
        with torch.inference_mode():
            output_ids = self.model.generate(**inputs, **generation)
        completion_ids = output_ids[:, inputs["input_ids"].shape[1]:]
        generated = self.tokenizer.batch_decode(completion_ids, skip_special_tokens=True)
        return {"generated_text": generated}
