from __future__ import annotations

from typing import Any

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from huggingface.openai_api import APIError

from .base import TaskAdapter, bool_value, float_value, int_value, text_values


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
        prompts = text_values(rows, self.prompt_column)
        inputs = self.tokenizer(
            prompts, padding=True, truncation=True, max_length=self.max_length,
            return_tensors="pt",
        ).to(self.device)
        generation = self.generation_options({})
        with torch.inference_mode():
            output_ids = self.model.generate(**inputs, **generation)
        completion_ids = output_ids[:, inputs["input_ids"].shape[1]:]
        generated = self.tokenizer.batch_decode(completion_ids, skip_special_tokens=True)
        return {"generated_text": generated}

    def generation_options(self, options):
        do_sample = bool_value(self.config, "do_sample", False)
        temperature = float_value(self.config, "temperature", 1.0)
        top_p = options.get("top_p", float_value(self.config, "top_p", 1.0))
        if "temperature" in options:
            temperature = options["temperature"]
            do_sample = temperature > 0
        elif top_p < 1 and "top_p" in options:
            do_sample = True
        maximum = int_value(self.config, "max_new_tokens", 128)
        requested = options.get("max_completion_tokens", maximum)
        if maximum <= 0 or requested > maximum:
            raise APIError(400, "max_completion_tokens exceeds the service limit", param="max_completion_tokens")
        generation = {
            "max_new_tokens": requested,
            "do_sample": do_sample,
            "pad_token_id": self.tokenizer.pad_token_id,
            "num_return_sequences": 1,
            "num_beams": 1,
            "return_dict_in_generate": False,
        }
        if do_sample:
            if not 0 < temperature <= 2 or not 0 < top_p <= 1:
                raise APIError(400, "invalid service sampling parameters")
            generation["temperature"] = temperature
            generation["top_p"] = top_p
        return generation

    def chat(self, messages, options):
        if not self.tokenizer.chat_template:
            raise APIError(400, "model has no chat template", "chat_template_missing")
        generation = self.generation_options(options)
        try:
            inputs = self.tokenizer.apply_chat_template(
                messages, tokenize=True, add_generation_prompt=True, return_dict=True,
                return_tensors="pt",
            ).to(self.device)
        except (ValueError, TypeError):
            raise APIError(400, "model chat template does not support this request", "unsupported_messages") from None
        prompt_tokens = inputs["input_ids"].shape[1]
        limits = [getattr(self.model.config, key, None) for key in ("max_position_embeddings", "n_positions", "n_ctx")]
        limits = [limit for limit in limits if type(limit) is int and limit > 0]
        if not limits:
            raise APIError(400, "model context limit is unknown", "unsupported_model")
        if prompt_tokens > self.max_length or prompt_tokens + generation["max_new_tokens"] > min(limits):
            raise APIError(400, "prompt and output budget exceed the context limit", "context_length_exceeded")
        with torch.inference_mode():
            output_ids = self.model.generate(**inputs, **generation)
        ids = output_ids[0, prompt_tokens:].tolist()
        eos = self.model.generation_config.eos_token_id
        eos = eos if isinstance(eos, (list, tuple)) else [eos]
        finish_reason = "length"
        for index, token in enumerate(ids):
            if token in eos:
                ids = ids[:index + 1]
                finish_reason = "stop"
                break
        count = len(ids)
        return {"content": self.tokenizer.decode(ids, skip_special_tokens=True), "finish_reason": finish_reason,
                "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": count,
                          "total_tokens": prompt_tokens + count}}
