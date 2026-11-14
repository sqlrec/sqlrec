"""HTTP protocol and shared runtime contracts, without downloading weights."""
import asyncio
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient
from huggingface.openai_api import APIError, OpenAIMiddleware, OpenAIRuntime
from huggingface.runtime import ModelRuntime


class OpenAIHttpTest(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).resolve().parents[2] / 'main/python/huggingface/server.py'
        spec = importlib.util.spec_from_file_location('hf_openai_http_test_server', path)
        self.server = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'huggingface.tasks': SimpleNamespace(TASK_ADAPTERS={})}):
            spec.loader.exec_module(self.server)
        self.runtime = self.server._openai
        self.adapter = SimpleNamespace(chat=Mock(return_value={
            'content': '摘要', 'finish_reason': 'stop',
            'usage': {'prompt_tokens': 5, 'completion_tokens': 2, 'total_tokens': 7}}),
            embedding_dimension=2, batch_size=2)
        self.adapter.embed = Mock(side_effect=lambda texts, strict: {
            'embedding': [[float(len(text)), 1.] for text in texts], 'prompt_tokens': len(texts) * 3})
        self.adapter.predict = Mock(return_value={'generated_text': ['native']})
        self.server._runtime.adapter = self.adapter
        self.configure()
        self.client = TestClient(self.server.app)
        self.addCleanup(self.client.close)
        self.body = {'model': 'test_model', 'messages': [{'role': 'user', 'content': 'text'}],
                     'stream': False, 'n': 1, 'store': False}

    def configure(self, task='text-generation', **config):
        with patch.dict('os.environ', {}, clear=True):
            self.runtime.configure({'task': task, 'model_name': 'test_model'}, config)

    def post(self, body=None, status=200, path='/v1/chat/completions'):
        response = self.client.post(path, json=self.body if body is None else body)
        self.assertEqual(response.status_code, status, response.text)
        self.assertIn('x-request-id', response.headers)
        result = response.json()
        if status != 200:
            self.assertEqual(set(result), {'error'})
            self.assertTrue(result['error']['code'])
        return result

    def test_udf_request_works_by_default_and_models_uses_the_alias(self):
        result = self.post()
        self.assertEqual(result['choices'][0]['message'], {'role': 'assistant', 'content': '摘要'})
        self.assertEqual(result['object'], 'chat.completion')
        self.adapter.chat.assert_called_once_with(self.body['messages'], {})
        self.assertEqual(self.client.get('/v1/models').json()['data'][0]['id'], 'test_model')
        self.configure(served_model_name='alias')
        self.post(status=404)
        self.post({**self.body, 'model': 'alias'})
        self.post({**self.body, 'model': ''}, status=400)
        self.server._runtime.adapter = None
        self.post({**self.body, 'model': 'alias'}, status=503)

    def test_strict_chat_fields_and_types(self):
        for extra in ({'n': True}, {'n': 1.0}, {'n': 2}, {'stream': 0}, {'stream': True}, {'store': True},
                      {'temperature': True}, {'temperature': -1}, {'temperature': 2.1},
                      {'temperature': 10 ** 400}, {'top_p': 0},
                      {'max_completion_tokens': True}, {'max_completion_tokens': 1.5}, {'max_completion_tokens': 0},
                      {'reasoning_effort': 'low'}, {'tools': []}, {'user': 1},
                      {'temperature': 0, 'top_p': .5}, {'messages': []}, {'messages': None},
                      {'messages': [{'role': 'system', 'content': 'text'}]},
                      {'messages': [{'role': 'user', 'content': ['text']}]},
                      {'messages': [{'role': 'user', 'content': 'text', 'name': 'x'}]}):
            with self.subTest(extra=extra):
                self.post({**self.body, **extra}, 400)
        for value in ([], '', 1, True):
            self.post(value, 400)
        self.adapter.chat.assert_not_called()
        self.post({**self.body, 'messages': [{'role': 'user', 'content': ''}], 'temperature': 0})
        self.post({**self.body, 'top_p': .5, 'max_completion_tokens': 3, 'user': 'caller'})
        self.assertEqual(self.adapter.chat.call_args.args[1], {'top_p': .5, 'max_completion_tokens': 3})

    def test_embedding_batching_native_dimensions_and_token_counts(self):
        self.configure('embedding')
        body = {'model': 'test_model', 'input': ['a', 'bb', 'ccc'], 'encoding_format': 'float', 'dimensions': 2}
        result = self.post(body, path='/v1/embeddings')
        self.assertEqual([row['index'] for row in result['data']], [0, 1, 2])
        self.assertEqual([row['embedding'] for row in result['data']], [[1., 1.], [2., 1.], [3., 1.]])
        self.assertEqual(result['usage'], {'prompt_tokens': 9, 'total_tokens': 9})
        self.assertEqual([call.args[0] for call in self.adapter.embed.call_args_list], [['a', 'bb'], ['ccc']])
        result = self.post({**body, 'input': 'one'}, path='/v1/embeddings')
        self.assertEqual(len(result['data']), 1)
        for extra in ({'dimensions': 1}, {'dimensions': True}, {'dimensions': None}, {'encoding_format': 'base64'},
                      {'input': ''}, {'input': '  '}, {'input': []}, {'input': [1]}, {'input': [None]},
                      {'input': ['a'] * 33}):
            self.post({**body, **extra}, 400, '/v1/embeddings')
        self.adapter.embed.side_effect = lambda texts, strict: {'embedding': [[float('nan'), 1]], 'prompt_tokens': 3}
        with self.assertLogs('sqlrec.huggingface.openai', level='ERROR'):
            self.post({**body, 'input': 'one'}, 500, '/v1/embeddings')

    def test_auth_covers_v1_but_preserves_native_predict_and_health(self):
        self.runtime.api_key = 'secret'
        for path in ('/v1/chat/completions', '/v1/models'):
            response = self.client.get(path) if path.endswith('models') else self.client.post(path, json=self.body)
            self.assertEqual(response.status_code, 401)
        response = self.client.post('/v1/chat/completions', json=self.body, headers={'Authorization': 'Bearer wrong'})
        self.assertEqual(response.status_code, 401)
        response = self.client.post('/v1/chat/completions', json=self.body, headers={'Authorization': 'Bearer secret'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.post('/predict', json=[{'prompt': 'x'}]).status_code, 200)
        self.assertEqual(self.client.get('/ready').status_code, 200)
        with patch.dict('os.environ', {}, clear=True), self.assertRaisesRegex(ValueError, 'Secret is empty'):
            self.runtime.configure({'task': 'embedding', 'model_name': 'm'}, {'openai_api_key_secret': 'key'})

    def test_busy_requests_share_a_permit_and_release_it_after_failure(self):
        entered, release = threading.Event(), threading.Event()
        def chat(*args):
            entered.set()
            if not release.wait(5):
                raise RuntimeError('test timed out')
            raise RuntimeError('SECRET INPUT MUST NOT BE RETURNED')
        self.adapter.chat.side_effect = chat
        result = []
        thread = threading.Thread(target=lambda: result.append(self.client.post('/v1/chat/completions', json=self.body)))
        thread.start()
        try:
            self.assertTrue(entered.wait(5))
            self.post(status=429)
            self.assertEqual(self.client.post('/predict', json=[{'prompt': 'x'}]).status_code, 429)
            self.assertEqual(self.client.get('/health').status_code, 200)
            self.assertEqual(self.client.get('/ready').status_code, 200)
        finally:
            with self.assertLogs('sqlrec.huggingface.openai', level='ERROR') as logs:
                release.set()
                thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result[0].status_code, 500)
        self.assertNotIn('SECRET', result[0].text)
        self.assertNotIn('SECRET', '\n'.join(logs.output))
        self.adapter.chat.side_effect = None
        self.post()
        with self.runtime.inference():
            self.post(status=429)

    def test_limits_malformed_json_and_endpoint_errors_use_the_error_envelope(self):
        self.runtime.max_request_bytes = 10
        self.post(status=413)
        self.runtime.max_request_bytes = 1048576
        for body in ('{', '{"model":NaN}', '{"model":"test_model","temperature":Infinity}'):
            response = self.client.post('/v1/chat/completions', content=body)
            self.assertEqual(response.status_code, 400)
            self.assertIn('error', response.json())
        self.assertIn('error', self.client.get('/v1/unknown').json())
        self.assertIn('error', self.client.get('/v1/chat/completions').json())
        self.configure('text-classification')
        self.post(status=400)
        self.assertEqual(self.client.get('/v1/models').json()['data'], [])

    def test_runtime_settings_are_not_inherited_from_training_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '_SUCCESS').touch()
            (root / 'sqlrec_manifest.json').write_text(json.dumps({
                'task': 'embedding', 'model_name': 'original', 'model_params': {
                    'text_column': 'body', 'served_model_name': 'stale', 'openai_api_key_secret': 'stale',
                    'openai_max_request_bytes': 1}}))
            config = root / 'service.config'
            config.write_text('{}')
            with patch.dict('os.environ', {}, clear=True), patch.dict(self.server.TASK_ADAPTERS, {
                    'embedding': lambda directory, settings: SimpleNamespace(config=settings)}):
                self.server.load_runtime(directory, str(config))
        self.assertEqual(self.runtime.model_name, 'original')
        self.assertEqual(self.runtime.max_request_bytes, 1048576)
        self.assertNotIn('openai_api_key_secret', self.server._runtime.adapter.config)


class StreamLimitTest(unittest.TestCase):
    def test_chunked_body_is_bounded_without_content_length(self):
        async def run():
            runtime = OpenAIRuntime(ModelRuntime({}))
            runtime.max_request_bytes = 4
            app = Mock()
            middleware = OpenAIMiddleware(app, runtime)
            chunks = iter([{'type': 'http.request', 'body': b'123', 'more_body': True},
                           {'type': 'http.request', 'body': b'45', 'more_body': False}])
            async def receive():
                return next(chunks)
            responses = []
            async def send(message):
                responses.append(message)
            await middleware({'type': 'http', 'path': '/v1/embeddings', 'headers': []}, receive, send)
            self.assertEqual(responses[0]['status'], 413)
            app.assert_not_called()
        asyncio.run(run())


if __name__ == '__main__':
    unittest.main()
