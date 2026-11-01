"""Regression checks for OpenAI benchmark false passes and forwarder cleanup."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import copy
import tempfile
import unittest

from benchmark.huggingface.config import CASES
from benchmark.huggingface import openai_checks as checks
from benchmark.huggingface import __main__ as runner


class ResponseContractTest(unittest.TestCase):
    def setUp(self):
        self.chat = {'id': 'chatcmpl-test', 'object': 'chat.completion', 'model': 'model', 'created': 1,
                     'choices': [{'index': 0, 'message': {'role': 'assistant', 'content': ''}, 'finish_reason': 'stop'}],
                     'usage': {'prompt_tokens': 3, 'completion_tokens': 1, 'total_tokens': 4}}
        self.embedding = {'object': 'list', 'model': 'model',
                          'data': [{'object': 'embedding', 'index': 0, 'embedding': [1., 0.]}],
                          'usage': {'prompt_tokens': 3, 'total_tokens': 3}}

    def test_completion_allows_empty_eos_but_rejects_malformed_envelopes_and_budget_overruns(self):
        self.assertEqual(checks.completion(self.chat, 'model', 8), '')
        for change in ({'choices': []}, {'choices': self.chat['choices'] * 2}, {'model': 'wrong'},
                       {'object': 'completion'}, {'created': True}, {'id': None},
                       {'usage': {'prompt_tokens': 3, 'completion_tokens': 1, 'total_tokens': 3}},
                       {'usage': {'prompt_tokens': True, 'completion_tokens': 1, 'total_tokens': 2}}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                checks.completion({**self.chat, **change}, 'model', 8)
        for key, value in (('index', False), ('finish_reason', 'tool_calls'), ('message', {'role': 'user', 'content': 'x'})):
            result = copy.deepcopy(self.chat)
            result['choices'][0][key] = value
            with self.assertRaises(ValueError):
                checks.completion(result, 'model', 8)
        with self.assertRaises(ValueError):
            checks.completion(self.chat, 'model', 0)

    def test_embedding_checks_indices_dimensions_and_usage(self):
        self.assertEqual(checks.vectors(self.embedding, 'model', 1, 2), [[1., 0.]])
        for extra in ({'index': 1}, {'index': False}, {'embedding': [1.]}, {'embedding': [True, 0.]},
                      {'embedding': [float('nan'), 0.]}, {'object': 'wrong'}):
            result = copy.deepcopy(self.embedding)
            result['data'][0].update(extra)
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                checks.vectors(result, 'model', 1, 2)
        with self.assertRaises(ValueError):
            checks.vectors(self.embedding, 'model', 2, 2)


class IntegrationWiringTest(unittest.TestCase):
    def test_generation_has_a_pinned_chat_model_separate_from_the_plain_prompt_case(self):
        generation = [case for case in CASES if case.task == 'text-generation']
        self.assertEqual({case.chat_template for case in generation}, {False, True})
        self.assertTrue(all(len(case.revision) == 40 for case in generation))

    def test_scalar_api_uses_the_real_udfs_full_endpoint_and_native_column_names(self):
        for task, udf in (('text-generation', 'llm_complete'), ('embedding', 'llm_embed_text')):
            case = next(case for case in CASES if case.task == task)
            resources = SimpleNamespace(name='hf_run', execute=Mock())
            checks.register_udf_api(resources, case, 'http://service:80/v1/test')
            sql = '\n'.join(call.args[0] for call in resources.execute.call_args_list)
            self.assertIn(udf + "('http://service:80/v1/test', '', 'hf_run'", sql)
            self.assertIn('AS `' + case.output_columns[0] + '`', sql)
            self.assertNotIn('call_service', sql)
            self.assertNotIn('enable_openai_api', sql)

    def test_forwarder_is_terminated_even_if_a_protocol_check_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            resources = SimpleNamespace(root=Path(directory), env=SimpleNamespace(kubectl='kubectl'),
                                        workloads=SimpleNamespace(namespace='test'))
            process = Mock()
            process.poll.return_value = None
            def start(command, stdout, stderr):
                stdout.write('Forwarding from 127.0.0.1:12345 -> 80\n')
                stdout.flush()
                return process
            with patch.object(checks.subprocess, 'Popen', side_effect=start) as popen:
                with self.assertRaisesRegex(ValueError, 'bad protocol'):
                    with checks.forwarded_service(resources, 'hf-service') as endpoint:
                        self.assertEqual(endpoint, 'http://127.0.0.1:12345')
                        raise ValueError('bad protocol')
            self.assertIn('service/hf-service', popen.call_args.args[0])
            process.terminate.assert_called_once()
            process.wait.assert_called_once()

    def test_openai_failure_marks_the_run_failed_and_still_releases_service(self):
        case = next(case for case in CASES if case.chat_template)
        with tempfile.TemporaryDirectory() as directory:
            client, api = Mock(), Mock()
            resources = Mock(root=Path(directory), client=client, times={})
            resources.deploy.return_value = {'scalar': 'hf-service'}
            env = SimpleNamespace(endpoints={'sqlrec': {}, 'api': {}})
            result = {}
            with patch.object(runner, 'HS2Client') as hs2, \
                    patch.object(runner, 'APIClient', return_value=api), \
                    patch.object(runner, 'HuggingFaceResources', return_value=resources), \
                    patch.object(runner, 'register_api'), patch.object(runner, 'exercise', return_value={}), \
                    patch.object(runner, 'exercise_openai', side_effect=ValueError('bad protocol')):
                hs2.return_value.connect.return_value = client
                with self.assertRaisesRegex(ValueError, 'bad protocol'):
                    runner.run_case(case, env, Path(directory), result, 1)
            resources.cleanup.assert_called_once()
            self.assertEqual(result['status'], 'FAILED')


if __name__ == '__main__':
    unittest.main()
