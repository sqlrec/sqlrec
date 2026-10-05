"""All text tasks validate SQL inputs before invoking a tokenizer."""

import importlib.util
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import Mock, patch


class TextInputTest(unittest.TestCase):
    def load(self, name):
        directory = Path(__file__).resolve().parents[2] / "main/python/huggingface/tasks"
        torch, nn, functional = ModuleType("torch"), ModuleType("torch.nn"), ModuleType("torch.nn.functional")
        torch.nn, nn.functional = nn, functional
        transformers = ModuleType("transformers")
        transformers.AutoModel = transformers.AutoModelForCausalLM = transformers.AutoTokenizer = Mock()
        modules = {"torch": torch, "torch.nn": nn, "torch.nn.functional": functional, "transformers": transformers}
        for source in ("base", name):
            spec = importlib.util.spec_from_file_location("sqlrec_text_input_test." + source, directory / (source + ".py"))
            module = importlib.util.module_from_spec(spec)
            with patch.dict(sys.modules, modules):
                spec.loader.exec_module(module)
            modules[spec.name] = module
        return module

    def test_embedding_and_generation_reject_null_missing_and_nonstring_text(self):
        for name, class_name, column_attr in (("text_embedding", "TextEmbeddingAdapter", "text_column"),
                                               ("text_generation", "TextGenerationAdapter", "prompt_column")):
            adapter_type = getattr(self.load(name), class_name)
            adapter = adapter_type.__new__(adapter_type)
            setattr(adapter, column_attr, "body")
            adapter.tokenizer = Mock()
            for rows in ([{"body": None}], [{}], [{"body": 1}], [{"body": "valid"}, {"body": None}]):
                with self.subTest(task=name, rows=rows), self.assertRaisesRegex(ValueError, "non-null STRING"):
                    adapter.predict(rows)
                adapter.tokenizer.assert_not_called()
            adapter.max_length = 512
            adapter.tokenizer.side_effect = LookupError("tokenizer reached")
            with self.assertRaisesRegex(LookupError, "tokenizer reached"):
                adapter.predict([{"body": "None"}, {"body": ""}])
            self.assertEqual(adapter.tokenizer.call_args.args[0], ["None", ""])


if __name__ == "__main__":
    unittest.main()
