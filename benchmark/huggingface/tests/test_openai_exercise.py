"""Run the benchmark's checks against the real ASGI protocol without sockets."""
from contextlib import contextmanager, nullcontext
import math
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from benchmark.huggingface.config import CASES
from benchmark.huggingface import openai_checks as checks

# Load the lightweight production protocol package without requiring PYTHONPATH
# or importing the model adapters (and their torch/transformers dependencies).
source = Path(__file__).resolve().parents[3] / 'sqlrec-model/src/main/python'
with patch.object(sys, 'path', [str(source), *sys.path]):
    from huggingface import openai_api as protocol
    from huggingface.runtime import ModelRuntime


class Adapter:
    embedding_dimension, batch_size = 32, 2

    def __init__(self, case):
        self.case = case

    def chat(self, messages, options):
        if not self.case.chat_template:
            raise protocol.APIError(400, 'no template', 'chat_template_missing')
        text = messages[0]['content']
        maximum = options.get('max_completion_tokens', 8)
        if maximum > 8 or len(text.split()) > 64:
            raise protocol.APIError(400, 'token limit')
        tokens = len(text.split()) + 2
        return {'content': 'reply:' + text, 'finish_reason': 'stop',
                'usage': {'prompt_tokens': tokens, 'completion_tokens': 1, 'total_tokens': tokens + 1}}

    def embed(self, texts, strict=True):
        if strict and any(len(text.split()) > 64 for text in texts):
            raise protocol.APIError(400, 'token limit')
        vectors = []
        for text in texts:
            norm = math.sqrt(len(text) ** 2 + 1)
            vectors.append([len(text) / norm, 1 / norm] + [0.] * 30)
        return {'embedding': vectors, 'prompt_tokens': sum(len(text.split()) + 2 for text in texts)}


class Session:
    def __init__(self, client):
        self.client = client
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def request(self, method, url, **kwargs):
        kwargs.pop('timeout', None)  # TestClient is in-process; production requests keep their timeout.
        return self.client.request(method, url, **kwargs)


class OpenAIExerciseTest(unittest.TestCase):
    def run_checks(self, case, bad_udf=False):
        adapter = Adapter(case)
        model_runtime = ModelRuntime({})
        model_runtime.adapter = adapter
        with patch.dict('os.environ', {'SQLREC_OPENAI_API_KEY': ''}):
            runtime = protocol.OpenAIRuntime(model_runtime)
            runtime.configure({'task': case.task, 'model_name': 'hf_run'}, {})
        app = FastAPI()
        protocol.register_openai_routes(app, runtime)

        @app.post('/predict')
        def predict(body: list[dict]):
            with model_runtime.inference():
                return {'embedding': adapter.embed([row['body'] for row in body])['embedding']}

        def sql_call(function, body, retry):
            rows = body['data']['observations']
            result = []
            for row in rows:
                value = (adapter.chat([{'role': 'user', 'content': row['prompt']}], {})['content']
                         if case.chat_template else adapter.embed([row['body']])['embedding'][0])
                if bad_udf:
                    value = 'wrong' if case.chat_template else [0., 1.] + [0.] * 30
                result.append({'row_id': row['row_id'], case.output_columns[0]: value})
            return result

        @contextmanager
        def forward(*args):
            yield 'http://testserver'

        with tempfile.TemporaryDirectory() as directory, TestClient(app) as client:
            resources = SimpleNamespace(name='hf_run', root=Path(directory), execute=Mock(),
                                        workloads=SimpleNamespace(namespace='test'), stage=lambda name: nullcontext())
            api = SimpleNamespace(call=Mock(side_effect=sql_call))
            with patch.object(checks, 'forwarded_service', forward), \
                    patch.object(checks.requests, 'Session', return_value=Session(client)):
                result = checks.exercise_openai(case, resources, api, 'hf-service')
            if case.chat_template or case.task == 'embedding':
                self.assertTrue((resources.root / 'openai-responses.json').exists())
            return result

    def test_complete_benchmark_protocol_flow_including_rejections_and_udf_comparison(self):
        for case in CASES:
            if case.task in ('text-generation', 'embedding'):
                with self.subTest(case=case.name):
                    result = self.run_checks(case)
                    if case.task == 'text-generation' and not case.chat_template:
                        self.assertTrue(result['chat_template_missing_verified'])
                    else:
                        self.assertTrue(result['protocol_and_udf_equivalent'])
                        self.assertEqual(result['udf_rows'], 6)

    def test_valid_looking_but_different_udf_results_cannot_pass(self):
        for case in CASES:
            if case.chat_template or case.task == 'embedding':
                with self.subTest(case=case.name), self.assertRaisesRegex(ValueError, 'changed'):
                    self.run_checks(case, bad_udf=True)


if __name__ == '__main__':
    unittest.main()
