"""Exercise adapter output semantics with NumPy tensors, without downloading models."""

from contextlib import nullcontext
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import numpy as np


class Tensor:
    def __init__(self, values):
        self.values = np.asarray(values)

    def float(self):
        return self

    def cpu(self):
        return self

    def __iter__(self):
        return iter(self.values)

    def tolist(self):
        return self.values.tolist()

    def max(self, dim):
        return Tensor(self.values.max(axis=dim)), Tensor(self.values.argmax(axis=dim))


class ClassificationTest(unittest.TestCase):
    def adapter(self, problem_type, logits):
        torch = ModuleType("torch")
        torch.inference_mode = nullcontext
        torch.sigmoid = lambda tensor: Tensor(1 / (1 + np.exp(-tensor.values)))
        def softmax(tensor, dim):
            values = np.exp(tensor.values - tensor.values.max(axis=dim, keepdims=True))
            return Tensor(values / values.sum(axis=dim, keepdims=True))
        torch.softmax = softmax
        directory = Path(__file__).resolve().parents[2] / "main/python/huggingface/tasks"
        base_spec = importlib.util.spec_from_file_location("sqlrec_classification_test.base", directory / "base.py")
        base = importlib.util.module_from_spec(base_spec)
        with patch.dict(sys.modules, {"torch": torch}):
            base_spec.loader.exec_module(base)
        class TaskAdapter:
            def __init__(self, *args):
                self.device = "cpu"
                self.trust_remote_code = False
            def _model_kwargs(self):
                return {}
        base.TaskAdapter, base.int_value = TaskAdapter, lambda conf, key, default: conf.get(key, default)
        transformers = ModuleType("transformers")
        model = Mock()
        model.to.return_value = model.eval.return_value = model
        model.config = SimpleNamespace(problem_type=problem_type, num_labels=len(logits[0]),
                                       id2label={0: "first", 1: "second"})
        model.return_value = SimpleNamespace(logits=Tensor(logits))
        transformers.AutoModelForSequenceClassification = Mock(from_pretrained=Mock(return_value=model))
        inputs = {}
        class Inputs(dict):
            def to(self, device):
                return self
        tokenizer = Mock(return_value=Inputs(inputs))
        transformers.AutoTokenizer = Mock(from_pretrained=Mock(return_value=tokenizer))
        path = Path(__file__).resolve().parents[2] / "main/python/huggingface/tasks/text_classification.py"
        spec = importlib.util.spec_from_file_location("sqlrec_classification_test.text_classification", path)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"torch": torch, "transformers": transformers, base.__name__: base}):
            spec.loader.exec_module(module)
        return module.TextClassificationAdapter("/model", {"text_column": "body"})

    def test_multilabel_scores_use_independent_sigmoids(self):
        adapter = self.adapter("multi_label_classification", [[2., 3.], [2., 2.]])
        output = adapter.predict([{"body": "a"}, {"body": "b"}])
        self.assertEqual(output["label"], ["second", "first"])
        np.testing.assert_allclose(output["score"], [1 / (1 + np.exp(-3)), 1 / (1 + np.exp(-2))])

    def test_singlelabel_and_unspecified_multiclass_scores_use_softmax(self):
        for problem_type in (None, "single_label_classification"):
            output = self.adapter(problem_type, [[2., 3.]]).predict([{"body": "a"}])
            self.assertEqual(output["label"], ["second"])
            self.assertAlmostEqual(output["score"][0], 1 / (1 + np.exp(-1)))

    def test_single_logit_multilabel_is_supported_but_regression_is_rejected(self):
        output = self.adapter("multi_label_classification", [[2.]]).predict([{"body": "a"}])
        self.assertAlmostEqual(output["score"][0], 1 / (1 + np.exp(-2)))
        for problem_type in (None, "single_label_classification"):
            output = self.adapter(problem_type, [[2.]]).predict([{"body": "a"}])
            self.assertAlmostEqual(output["score"][0], 1 / (1 + np.exp(-2)))
        for logits in ([[2.]], [[2., 3.]]):
            with self.assertRaisesRegex(ValueError, "regression"):
                self.adapter("regression", logits)

    def test_null_missing_and_nonstring_text_never_reach_the_tokenizer(self):
        for rows in ([{"body": None}], [{}], [{"body": 1}], [{"body": "valid"}, {"body": None}]):
            adapter = self.adapter(None, [[2., 3.]])
            with self.subTest(rows=rows), self.assertRaisesRegex(ValueError, "non-null STRING"):
                adapter.predict(rows)
            adapter.tokenizer.assert_not_called()
        adapter = self.adapter(None, [[2., 3.]])
        adapter.text_pair_column = "pair"
        with self.assertRaisesRegex(ValueError, "pair"):
            adapter.predict([{"body": "valid", "pair": None}])
        adapter.tokenizer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
