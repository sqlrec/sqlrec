"""Test real adapter methods with controlled tokenizer/model tensor boundaries."""
from contextlib import nullcontext
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import numpy as np

from huggingface.openai_api import APIError
import test_huggingface_text_inputs as loader


class Inputs(dict):
    def to(self, device):
        return self


class GenerationTest(unittest.TestCase):
    def setUp(self):
        module = loader.TextInputTest().load('text_generation')
        module.torch.inference_mode = nullcontext
        self.adapter = module.TextGenerationAdapter.__new__(module.TextGenerationAdapter)
        self.adapter.config = {'max_new_tokens': 8, 'do_sample': False}
        self.adapter.max_length, self.adapter.device, self.adapter.prompt_column = 10, 'cpu', 'prompt'
        self.adapter.tokenizer = Mock(chat_template='template', pad_token_id=0)
        self.adapter.tokenizer.apply_chat_template.return_value = Inputs(
            input_ids=np.array([[11, 12, 13]]), attention_mask=np.array([[1, 1, 1]]))
        self.adapter.tokenizer.decode.side_effect = lambda ids, **kwargs: ','.join(str(x) for x in ids)
        self.adapter.model = Mock(config=SimpleNamespace(max_position_embeddings=32),
                                  generation_config=SimpleNamespace(eos_token_id=[9, 10]))
        self.adapter.model.generate.return_value = np.array([[11, 12, 13, 4, 9, 0]])
        self.messages = [{'role': 'user', 'content': 'hello'}]

    def test_template_tokens_and_eos_are_counted_without_retokenizing_text(self):
        result = self.adapter.chat(self.messages, {'temperature': 0, 'max_completion_tokens': 3})
        self.assertEqual(result['content'], '4,9')
        self.assertEqual(result['finish_reason'], 'stop')
        self.assertEqual(result['usage'], {'prompt_tokens': 3, 'completion_tokens': 2, 'total_tokens': 5})
        self.adapter.tokenizer.assert_not_called()
        self.assertEqual(self.adapter.tokenizer.apply_chat_template.call_args.kwargs,
                         {'tokenize': True, 'add_generation_prompt': True, 'return_dict': True, 'return_tensors': 'pt'})
        self.assertFalse(self.adapter.model.generate.call_args.kwargs['do_sample'])
        self.adapter.model.generate.return_value = np.array([[11, 12, 13, 4, 5, 6]])
        self.assertEqual(self.adapter.chat(self.messages, {})['finish_reason'], 'length')
        self.adapter.model.generate.return_value = np.array([[11, 12, 13, 10]])
        self.assertEqual(self.adapter.chat(self.messages, {})['usage']['completion_tokens'], 1)

    def test_per_request_sampling_and_budget_do_not_mutate_defaults(self):
        options = self.adapter.generation_options({'top_p': .5, 'max_completion_tokens': 2})
        self.assertTrue(options['do_sample'])
        self.assertEqual(options['top_p'], .5)
        self.assertEqual(self.adapter.config, {'max_new_tokens': 8, 'do_sample': False})
        self.assertFalse(self.adapter.generation_options({})['do_sample'])
        self.assertEqual(self.adapter.generation_options({})['max_new_tokens'], 8)
        self.assertTrue(self.adapter.generation_options({'temperature': .7})['do_sample'])
        with self.assertRaises(APIError):
            self.adapter.chat(self.messages, {'max_completion_tokens': 9})

    def test_missing_template_and_excessive_context_never_generate(self):
        for change in ('template', 'input', 'output', 'unknown'):
            with self.subTest(change=change):
                self.setUp()
                if change == 'template':
                    self.adapter.tokenizer.chat_template = None
                elif change == 'input':
                    self.adapter.max_length = 2
                elif change == 'output':
                    self.adapter.model.config.max_position_embeddings = 10
                else:
                    self.adapter.model.config = SimpleNamespace()
                with self.assertRaises(APIError):
                    self.adapter.chat(self.messages, {})
                self.adapter.model.generate.assert_not_called()

    def test_native_predict_still_uses_plain_prompts_and_legacy_truncation(self):
        self.adapter.tokenizer.return_value = Inputs(input_ids=np.array([[11, 12, 13]]))
        self.adapter.tokenizer.batch_decode.return_value = ['native']
        self.assertEqual(self.adapter.predict([{'prompt': 'raw'}]), {'generated_text': ['native']})
        self.adapter.tokenizer.apply_chat_template.assert_not_called()
        self.assertTrue(self.adapter.tokenizer.call_args.kwargs['truncation'])


class Tensor:
    def __init__(self, values):
        self.values = np.asarray(values)
        self.shape, self.dtype = self.values.shape, self.values.dtype
    def to(self, *args):
        return self
    def float(self):
        return self
    def cpu(self):
        return self
    def tolist(self):
        return self.values.tolist()
    def item(self):
        return self.values.item()
    def unsqueeze(self, dim):
        return Tensor(np.expand_dims(self.values, dim))
    def sum(self, dim=None):
        return Tensor(self.values.sum(axis=dim))
    def clamp(self, min):
        return Tensor(self.values.clip(min=min))
    def __mul__(self, other):
        return Tensor(self.values * other.values)
    def __truediv__(self, other):
        return Tensor(self.values / other.values)


class EmbeddingTest(unittest.TestCase):
    def test_native_and_openai_share_pooling_normalization_and_real_token_counts(self):
        module = loader.TextInputTest().load('text_embedding')
        module.torch.inference_mode = nullcontext
        module.functional.normalize = lambda tensor, p, dim: Tensor(
            tensor.values / np.linalg.norm(tensor.values, axis=dim, keepdims=True))
        adapter = module.TextEmbeddingAdapter.__new__(module.TextEmbeddingAdapter)
        adapter.max_length, adapter.device, adapter.text_column = 3, 'cpu', 'body'
        adapter.pooling, adapter.normalize = 'mean', True
        adapter.tokenizer = Mock(return_value=Inputs(input_ids=Tensor([[1, 2, 0], [1, 2, 3]]),
                                                     attention_mask=Tensor([[1, 1, 0], [1, 1, 1]])))
        adapter.model = Mock(return_value=SimpleNamespace(last_hidden_state=Tensor([
            [[1., 0], [0, 1.], [999, 999]], [[1., 0], [1, 0], [1, 0]]])))
        result = adapter.embed(['a', 'b'])
        self.assertEqual(result['prompt_tokens'], 5)
        np.testing.assert_allclose(result['embedding'], [[2 ** -.5, 2 ** -.5], [1, 0]])
        self.assertEqual(adapter.predict([{'body': 'a'}, {'body': 'b'}])['embedding'], result['embedding'])
        self.assertTrue(adapter.tokenizer.call_args.kwargs['truncation'])
        adapter.max_length = 2
        adapter.model.reset_mock()
        with self.assertRaises(APIError):
            adapter.embed(['a', 'b'])
        adapter.model.assert_not_called()
        self.assertFalse(adapter.tokenizer.call_args.kwargs['truncation'])


if __name__ == '__main__':
    unittest.main()
