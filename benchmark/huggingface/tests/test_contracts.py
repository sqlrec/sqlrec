"""Local regression checks for false passes in the integration benchmark."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import copy
import tempfile
import unittest

from benchmark.huggingface.__main__ import HuggingFaceResources
from benchmark.huggingface import __main__ as runner
from benchmark.huggingface.checks import check_equivalent, check_predictions
from benchmark.huggingface.config import CASES, input_rows
from benchmark.model_quality.client import Indeterminate


class PredictionContractTest(unittest.TestCase):
    def setUp(self):
        self.embedding = next(case for case in CASES if case.task == 'embedding')
        self.inputs = input_rows(self.embedding)
        self.predictions = [{'row_id': row['row_id'], 'embedding': [1.] + [0.] * 31}
                            for row in self.inputs]

    def test_reordered_predictions_align_by_identity(self):
        expected = check_predictions(self.embedding, self.inputs, self.predictions)
        actual = check_predictions(self.embedding, self.inputs, self.predictions[::-1])
        check_equivalent(self.embedding, expected, actual)

    def test_missing_duplicate_and_extra_rows_cannot_pass(self):
        for rows in (self.predictions[:-1], self.predictions + self.predictions[:1],
                     [self.predictions[0], self.predictions[0], self.predictions[2]],
                     [{**row, 'row_id': 'wrong'} for row in self.predictions]):
            with self.subTest(rows=rows), self.assertRaisesRegex(ValueError, 'row IDs'):
                check_predictions(self.embedding, self.inputs, rows)

    def test_invalid_vectors_cannot_pass(self):
        for vector in ([1.] * 31, [0.] * 32, [1.] * 32, [float('nan')] * 32,
                       [float('inf')] * 32, [True] + [0.] * 31, 'not an array'):
            rows = [{**row, 'embedding': vector} for row in self.predictions]
            with self.subTest(vector=vector), self.assertRaises(ValueError):
                check_predictions(self.embedding, self.inputs, rows)

    def test_valid_but_changed_vectors_fail_batch_consistency(self):
        expected = check_predictions(self.embedding, self.inputs, self.predictions)
        rows = copy.deepcopy(self.predictions)
        rows[1]['embedding'] = [0., 1.] + [0.] * 30
        actual = check_predictions(self.embedding, self.inputs, rows)
        with self.assertRaisesRegex(ValueError, 'changed'):
            check_equivalent(self.embedding, expected, actual)

    def test_classification_requires_known_labels_and_finite_probabilities(self):
        case = CASES[0]
        inputs = input_rows(case)
        rows = [{'row_id': row['row_id'], 'label': 'LABEL_0', 'score': .5} for row in inputs]
        check_predictions(case, inputs, rows)
        for value in ({'label': 'unknown'}, {'score': -1}, {'score': 1.1},
                      {'score': float('nan')}, {'score': True}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                check_predictions(case, inputs, [{**row, **value} for row in rows])

    def test_generation_requires_string_output_and_reproducible_completion(self):
        case = next(case for case in CASES if case.task == 'text-generation')
        inputs = input_rows(case)
        rows = [{'row_id': row['row_id'], 'generated_text': ''} for row in inputs]
        reference = check_predictions(case, inputs, rows)
        with self.assertRaisesRegex(ValueError, 'STRING'):
            check_predictions(case, inputs, [{**row, 'generated_text': None} for row in rows])
        changed = check_predictions(case, inputs, [{**row, 'generated_text': 'changed'} for row in rows])
        with self.assertRaisesRegex(ValueError, 'changed'):
            check_equivalent(case, reference, changed)


class DownloadContractTest(unittest.TestCase):
    def resource(self, directory, checkpoint_type):
        env = SimpleNamespace(kubectl='kubectl', train_resources={'NAMESPACE': 'test', 'pod_cpu_cores': 1})
        resource = HuggingFaceResources('hf_test', env, Path(directory), Mock())
        resource.workloads = Mock()
        resource.workloads.get.return_value = None
        resource.execute = Mock()
        resource.checkpoint = Mock(return_value={'Status:': 'succeeded', 'Checkpoint Type:': checkpoint_type})
        return resource

    def test_hf_download_serves_origin_without_a_training_table_or_export(self):
        with tempfile.TemporaryDirectory() as directory:
            resource = self.resource(directory, 'origin')
            resource.download()
            sql = resource.execute.call_args.args[0]
            self.assertIn('TRAIN MODEL', sql)
            self.assertNotIn(' ON ', sql.replace('\n', ' '))
            self.assertNotIn('EXPORT MODEL', sql)
            self.assertEqual(resource.jobs[0]['status'], 'completed')

    def test_export_checkpoint_is_not_accepted_as_hf_download_success(self):
        with tempfile.TemporaryDirectory() as directory:
            resource = self.resource(directory, 'export')
            with self.assertRaisesRegex(Indeterminate, 'origin'):
                resource.download()
            self.assertEqual(resource.jobs[0]['status'], 'submitted')


class CleanupContractTest(unittest.TestCase):
    def test_failed_predictions_still_release_the_service(self):
        with tempfile.TemporaryDirectory() as directory:
            client, api = Mock(), Mock()
            resources = Mock(root=Path(directory) / CASES[0].name, client=client, times={})
            resources.deploy.return_value = {'scalar': 'hf-test-service'}
            env = SimpleNamespace(endpoints={'sqlrec': {}, 'api': {}})
            result = {}
            with patch.object(runner, 'HS2Client') as hs2, \
                    patch.object(runner, 'APIClient', return_value=api), \
                    patch.object(runner, 'HuggingFaceResources', return_value=resources), \
                    patch.object(runner, 'register_api', return_value='hf_test_predict'), \
                    patch.object(runner, 'exercise', side_effect=ValueError('bad predictions')):
                hs2.return_value.connect.return_value = client
                with self.assertRaisesRegex(ValueError, 'bad predictions'):
                    runner.run_case(CASES[0], env, Path(directory), result, 1)
            resources.cleanup.assert_called_once()
            client.close.assert_called_once()
            api.close.assert_called_once()
            self.assertEqual(result['status'], 'FAILED')


if __name__ == '__main__':
    unittest.main()
