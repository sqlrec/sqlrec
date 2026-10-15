"""Identity, public SQL paths, lifecycle failures and compact report contracts."""
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import copy
import json
import tempfile
import unittest

import numpy as np
import pandas as pd

from benchmark.model_quality import config
from benchmark.model_quality import __main__ as runner
from benchmark.model_quality.client import HS2Client, Indeterminate, SQLResult
from benchmark.model_quality.common import digest, read_json, write_json
from benchmark.model_quality.data import (check_prepared, load_dataset, sample, training_identity,
                                         training_table, upload_training_tables)
from benchmark.model_quality.metrics import ranking_metrics, retrieval_metrics
from benchmark.model_quality.prepare import preparation_version, prepare_one
from benchmark.model_quality.prepare_kuairand import prepare
from benchmark.model_quality.ranking import output_columns, run_ranking
from benchmark.model_quality.report import write_report
from benchmark.model_quality.resources import RunResources, Workloads
from benchmark.model_quality.retrieval import query_inputs, run_retrieval
from benchmark.model_quality.tests.test_quality import rand_fixture


def environment():
    return config.Environment({'sqlrec': {}, 'milvus': {'url': 'http://unused', 'token_env': 'UNUSED'}},
                              'jfs://test/quality', {'NAMESPACE': 'test'}, {'NAMESPACE': 'test'},
                              {'tzrec': 'test-cpu', 'gbdt': 'test-cpu'}, 'hadoop', 'kubectl')


class PreparedTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        raw = self.root / 'raw/kuairand'
        raw.mkdir(parents=True)
        rand_fixture(raw)
        definition = config.dataset_definition('kuairand')
        write_json(raw / 'download.json', {'spec': definition['source'],
                   'files': {name: digest(raw / name) for name in definition['source']['files'].values()}})
        self.prepared = prepare_one('kuairand', self.root)
        self.dataset = load_dataset('kuairand', self.root)

    def test_prepared_reuse_and_corruption(self):
        with patch('benchmark.model_quality.prepare_kuairand.prepare', side_effect=AssertionError('must reuse')):
            self.assertEqual(prepare_one('kuairand', self.root), self.prepared)
        path = self.prepared / 'train.parquet'
        path.write_bytes(path.read_bytes() + b'broken')
        with self.assertRaisesRegex(ValueError, 'checksum'):
            check_prepared(self.prepared, 'kuairand')

    def test_sampling_is_reproducible_and_label_independent(self):
        frame = pd.DataFrame({'row_id': [str(i) for i in range(100)], 'label': np.arange(100) % 2})
        chosen = sample(frame, 'row_id', 20).row_id.tolist()
        changed = frame.sample(frac=1, random_state=9).assign(label=1)
        self.assertEqual(chosen, sample(changed, 'row_id', 20).row_id.tolist())
        self.assertEqual(len(sample(frame, 'row_id', None)), 100)

    def test_query_sampling_does_not_shrink_catalog_or_targets(self):
        original = query_inputs(self.dataset)
        with patch('benchmark.model_quality.data.EVAL_ROWS', 1):
            self.assertEqual(query_inputs(self.dataset), original)
            self.assertEqual(len(self.dataset.ranking_sample()), 1)
        self.assertEqual(set(self.dataset.items.video_id), {'a', 'b'})
        self.assertEqual(len(original[0]['targets']), 1)

    def training_warehouse(self, wrong_location=False):
        warehouse = Mock()
        names = {training_identity(self.dataset.manifest, view, environment())[0]: view
                 for view in self.dataset.manifest['training']}
        def execute(sql):
            if sql == 'SHOW TABLES':
                return SQLResult(['database', 'tableName'], [['default', name] for name in names])
            name = sql.split('`')[1]
            view = names[name]
            if sql.startswith('DESCRIBE'):
                spec = self.dataset.definition['views'][view]
                columns = [*spec['features'], *(self.dataset.definition['tasks'][task]['column'] for task in spec['tasks'])]
                rows = [[key, self.dataset.definition['columns'][key]['type'].lower(), None] for key in columns]
                _, uri = training_identity(self.dataset.manifest, view, environment())
                return SQLResult([], rows + [['split', 'string', None], ['# Partition Information', '', ''],
                                 ['Location', 'wrong' if wrong_location else uri, ''], ['Type', 'EXTERNAL', '']])
            return SQLResult(['count'], [[self.dataset.manifest['training'][view]['rows']]])
        warehouse.execute.side_effect = execute
        return warehouse

    def test_training_positive_only_and_cross_run_reuse(self):
        warehouse = self.training_warehouse()
        with patch('benchmark.model_quality.data.subprocess.run') as upload:
            name = training_table(self.dataset, 'dssm', warehouse, environment())
            another_run = load_dataset('kuairand', self.root)
            self.assertEqual(training_table(another_run, 'dssm', warehouse, environment()), name)
            upload_training_tables(self.prepared, warehouse, environment())
            upload.assert_not_called()
        self.assertTrue(all(call.args[0].startswith(('SHOW', 'DESCRIBE', 'SELECT'))
                            for call in warehouse.execute.call_args_list))
        frame = pd.read_parquet(self.prepared / 'training/recall/split=train/part.parquet')
        self.assertTrue(frame.label.eq(1).all())
        self.assertEqual(len(frame), 2)
        self.assertEqual(len(self.dataset.train), 4)

    def test_training_requires_preparation_and_rejects_wrong_table(self):
        warehouse = Mock()
        warehouse.execute.return_value = SQLResult([], [])
        with self.assertRaisesRegex(ValueError, '--upload'):
            training_table(self.dataset, 'deepfm', warehouse, environment())
        with self.assertRaisesRegex(ValueError, 'schema/location'):
            training_table(self.dataset, 'deepfm', self.training_warehouse(wrong_location=True), environment())
        changed = copy.deepcopy(self.dataset.manifest)
        changed['fingerprint'] = 'different'
        self.assertNotEqual(training_identity(changed, 'ranking', environment()),
                            training_identity(self.dataset.manifest, 'ranking', environment()))

    def test_all_model_views_outputs_and_parameters(self):
        combinations = sum(len(config.applicable_models(name, 'all')) for name in config.DATASETS)
        self.assertEqual(combinations, 28)
        for model in config.MODELS:
            features, tasks = config.view_fields(self.dataset.definition, model)
            params = config.model_parameters(model, features, tasks, self.dataset.train, self.dataset.items, environment(), self.dataset.name)
            self.assertNotIn('label', {feature.name for feature in features})
            self.assertEqual(params['random_seed'], 123)
            if model == 'mmoe':
                self.assertEqual(params['label_columns'], 'label,long_view')
                self.assertEqual(output_columns(model, tasks), {'click': 'probs_label', 'long_view': 'probs_long_view'})
            if model == 'rocket_launching':
                self.assertEqual(output_columns(model, tasks), {'click': 'probs_light'})

    def test_history_rates_are_opt_in_and_dssm_keeps_original_features(self):
        for dataset in config.DATASETS:
            definition = config.dataset_definition(dataset)
            for model in config.applicable_models(dataset, 'all'):
                features, _ = config.view_fields(definition, model)
                rates = {f.name for f in features} & {'user_positive_rate', 'item_positive_rate'}
                expected = {'user_positive_rate', 'item_positive_rate'} if (dataset, model) in config.POSITIVE_RATE_MODELS else set()
                self.assertEqual(rates, expected)
                if model == 'dssm':
                    self.assertEqual([f.name for f in features], definition['views']['recall']['features'])

    def test_ranking_public_api_without_saved_predictions(self):
        resources = Mock()
        resources.env, resources.root, resources.name = environment(), self.root / 'ranking', 'mq_rank'
        resources.root.mkdir()
        resources.stage.return_value = nullcontext()
        resources.train_export.return_value = ['v1_export']
        resources.deploy.return_value = {'scalar': 'mq_rank_scalar'}
        resources.execute.return_value = SQLResult([], [])
        api = Mock()
        api.call.side_effect = lambda name, payload: [dict(row_id=row['row_id'], probs=.4, unused_feature='discard')
                                                     for row in payload['data']['observations']]
        observations, predictions, _, _ = run_ranking(self.dataset, 'deepfm', 'train', resources, api)
        payload = api.call.call_args.args[1]['data']['observations']
        self.assertTrue(all('label' not in row and 'source' not in row for row in payload))
        self.assertEqual(len(predictions), len(observations))
        self.assertFalse((resources.root / 'predictions.parquet').exists())
        self.assertFalse((resources.root / 'predictions').exists())
        self.assertTrue(all(set(row)=={'row_id', 'probs'} for row in predictions))

    def test_ranking_batch_boundary_and_reordered_responses(self):
        dataset = copy.copy(self.dataset)
        frame = pd.concat([dataset.valid.iloc[:1]] * 1025, ignore_index=True)
        frame['row_id'] = [str(i) for i in range(len(frame))]
        dataset.valid = frame
        resources = Mock(env=environment(), root=self.root / 'batches')
        resources.name = 'mq_batches'
        resources.root.mkdir()
        resources.stage.return_value = nullcontext()
        resources.train_export.return_value = ['v1_export']
        resources.deploy.return_value = {'scalar': 'mq_batches_scalar'}
        resources.execute.return_value = SQLResult([], [])
        api = Mock()
        api.call.side_effect = lambda name, payload: [dict(row_id=row['row_id'], probs=.4)
                                                     for row in reversed(payload['data']['observations'])]
        observations, predictions, tasks, outputs = run_ranking(dataset, 'deepfm', 'train', resources, api, save_predictions=True)
        ranking_metrics(observations, predictions, tasks, outputs)
        self.assertEqual([len(call.args[1]['data']['observations']) for call in api.call.call_args_list], [512, 512, 1])
        saved = pd.read_parquet(resources.root / 'predictions.parquet')
        self.assertEqual(len(saved), 1025)
        self.assertEqual(list(saved.columns), ['row_id', 'probs'])
        self.assertFalse((resources.root / 'predictions').exists())

    def test_retrieval_keeps_allowed_pairs_and_no_scalar_scoring(self):
        dataset = copy.copy(self.dataset)
        dataset.definition = copy.deepcopy(dataset.definition)
        dataset.definition['protocol']['candidate_scope'] = 'observed_only'
        first_query = dataset.queries.query_id.iloc[0]
        second = dataset.queries.iloc[:1].assign(query_id='other-tab', tab='2')
        dataset.queries = pd.concat([dataset.queries, second], ignore_index=True)
        observations = dataset.valid[dataset.valid.query_id == first_query].copy()
        observations = observations.assign(query_id='other-tab', tab='2', row_id=observations.row_id + '_tab2')
        dataset.valid = pd.concat([dataset.valid, observations], ignore_index=True)
        resources = Mock()
        resources.env, resources.root, resources.name = environment(), self.root / 'retrieval', 'mq_dssm'
        resources.root.mkdir()
        resources.stage.return_value = nullcontext()
        resources.train_export.return_value = ['v1_export/user', 'v1_export/item']
        resources.deploy.return_value = {'user': 'user_service', 'item': 'item_service'}
        resources.execute.return_value = SQLResult([], [])
        api = Mock()
        def call(name, payload, **kwargs):
            if name.endswith('_load_items'):
                self.assertEqual(kwargs, {'retry': False})
                return [{'catalog_id': row['catalog_id'], 'item_tower_emb': [0.] * config.DATASET_MODEL_PARAMS[('kuairand', 'dssm')]['output_dim']}
                        for row in payload['data']['catalog_input']]
            return []
        api.call.side_effect = call
        with patch('benchmark.model_quality.retrieval.APIClient', return_value=api), patch('benchmark.model_quality.retrieval.IndexAdmin') as index, patch.dict('os.environ', {}, clear=True):
            queries, responses = run_retrieval(dataset, 'train', resources, api)
            index.return_value.wait_count.assert_called_once_with('mq_dssm_vectors', 2)
        statements = '\n'.join(call.args[0] for call in resources.execute.call_args_list)
        self.assertNotIn('_score', statements)
        self.assertNotIn('popularity', statements)
        self.assertIn('unseen.score AS score', statements)
        self.assertEqual(read_json(resources.root / 'config.json')['retrieval'], {'score': 'inner_product'})
        self.assertIn('DEFINE INPUT TABLE catalog_input', statements)
        self.assertNotIn('UNION ALL', statements)
        item_call = next(call for call in api.call.call_args_list if call.args[0].endswith('_load_items'))
        self.assertEqual({row['catalog_id'] for row in item_call.args[1]['data']['catalog_input']}, {1, 2})
        self.assertTrue(all(isinstance(row['tag'], list) for row in item_call.args[1]['data']['catalog_input']))
        self.assertNotIn('{{env:UNUSED}}', statements)
        self.assertIn("'token' = ''", statements)
        self.assertIn('JOIN allowed', statements)
        self.assertIn('LIMIT 50', statements)
        inputs = [call.args[1]['data']['query_input'][0] for call in api.call.call_args_list
                  if call.args[0].endswith('_recall')]
        self.assertEqual({row['user_id'] for row in inputs}, {second.user_id.iloc[0]})
        self.assertEqual({row['tab'] for row in inputs}, {'1', '2'})
        self.assertIn('tab', resources.define.call_args.args[2]['user_features'].split(','))
        self.assertEqual(set(queries[0]['allowed']), {'a', 'b'})
        self.assertEqual(responses, {query['query_id']: [] for query in queries})
        self.assertFalse((resources.root / 'queries.json').exists())
        self.assertFalse((resources.root / 'predictions.parquet').exists())
        with patch('benchmark.model_quality.retrieval.APIClient', return_value=api), patch('benchmark.model_quality.retrieval.IndexAdmin'):
            run_retrieval(dataset, 'train', resources, api, save_predictions=True)
        saved = pd.read_parquet(resources.root / 'predictions.parquet')
        self.assertEqual(saved.query_id.tolist(), list(responses))
        self.assertTrue(saved.item_id.isna().all())
        self.assertFalse((resources.root / 'responded_queries.json').exists())


class RetrievalMetricsTest(unittest.TestCase):
    def test_macro_recall_ndcg_and_zero_positive_query(self):
        queries = [{'query_id': 'q', 'targets': ['a', 'b'], 'seen': []},
                   {'query_id': 'empty', 'targets': [], 'seen': []}]
        responses = {'q': [{'query_id': 'q', 'item_id': 'a', 'score': 2},
                           {'query_id': 'q', 'item_id': 'c', 'score': 1}], 'empty': []}
        result = retrieval_metrics(queries, responses, {'a', 'b', 'c'})
        self.assertEqual(result['recall_at_50'], .5)
        self.assertAlmostEqual(result['ndcg_at_50'], 1 / (1 + 1/np.log2(3)))
        self.assertEqual(result['zero_positive_queries'], 1)
        self.assertEqual(result['empty_lists'], 1)
        with self.assertRaises(ValueError):
            retrieval_metrics(queries, {'q': responses['q']}, {'a', 'b', 'c'})

    def test_seen_allowed_duplicate_and_order_checks(self):
        query = [{'query_id': 'q', 'targets': ['a'], 'seen': ['b'], 'allowed': ['a', 'b']}]
        for items in [['b'], ['c'], ['a', 'a']]:
            rows = [{'query_id': 'q', 'item_id': item, 'score': 1} for item in items]
            with self.assertRaises(ValueError):
                retrieval_metrics(query, {'q': rows}, {'a', 'b', 'c'})


class ResourceTest(unittest.TestCase):
    def resource(self, directory):
        client = Mock()
        client.transport_broken = False
        client.execute.return_value = SQLResult([], [])
        resource = RunResources('mq_test', environment(), directory, client)
        resource.workloads = Mock(namespace='test')
        resource.workloads.get.return_value = None
        resource.workloads.snapshot.return_value = {}
        return resource

    def test_generated_model_sql_is_readable_and_logged_as_executed(self):
        with tempfile.TemporaryDirectory() as directory:
            resource = self.resource(directory)
            resource.define([config.Feature('tag', 'ARRAY<STRING>')], [config.Task('click', 'label')],
                            {'model': 'tzrec.deepfm', 'note': "a,b; WITH ON = 'quoted'", 'token': '{{env:TEST_TOKEN}}'},
                            {'columns': {'label': {'type': 'INT'}}})
            sql = resource.client.execute.call_args.args[0]
            logged = resource.sql_log.read_text()
            self.assertIn('\n    `tag` ARRAY<STRING>,\n    `label` INT\n', sql)
            self.assertIn("\nWITH (\n    'model' = 'tzrec.deepfm',\n", sql)
            self.assertIn("'note' = 'a,b; WITH ON = ''quoted'''", sql)
            self.assertIn("'token' = '{{env:TEST_TOKEN}}'", sql)
            self.assertTrue(logged.endswith(sql + ';\n\n'))

    def test_cleanup_attempts_both_services_and_blocks_on_unknown_job(self):
        with tempfile.TemporaryDirectory() as directory:
            resource = self.resource(directory)
            resource.services = [{'name': 'user', 'deployment': 'user', 'checkpoint': 'u', 'status': 'created'},
                                 {'name': 'item', 'deployment': 'item', 'checkpoint': 'i', 'status': 'created'}]
            calls = 0
            def execute(sql, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise Indeterminate('broken read')
                return SQLResult([], [])
            resource.client.execute.side_effect = execute
            with self.assertRaises(Indeterminate):
                resource.cleanup()
            self.assertEqual(resource.services[1]['status'], 'released')
            self.assertEqual(read_json(Path(directory) / 'resources.json')['status'], 'cleanup_failed')
        with tempfile.TemporaryDirectory() as directory:
            resource = self.resource(directory)
            resource.jobs = [{'name': 'job', 'kind': 'train', 'checkpoints': ['v1'], 'status': 'submitted'}]
            with self.assertRaises(Indeterminate):
                resource.cleanup()

    def test_cleanup_drops_verified_service_and_keeps_uid_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            resource = self.resource(directory)
            owned = {'Deployment/mq-test-scalar': {'uid': 'uid1', 'owners': []}}
            resource.services = [{'name': 'mq_test_scalar', 'deployment': 'mq-test-scalar', 'checkpoint': 'v1_export',
                                  'status': 'created', 'owned': owned}]
            present = True
            def execute(sql, **kwargs):
                nonlocal present
                if sql == 'SHOW SERVICES':
                    return SQLResult(['name'], [['mq_test_scalar']] if present else [])
                if sql.startswith('DESCRIBE'):
                    return SQLResult(['key', 'value'], [['Model Name:', 'mq_test'], ['Checkpoint Name:', 'v1_export'],
                                                       ['URL:', 'http://mq-test-scalar.test.svc.cluster.local:80']])
                if sql.startswith('DROP SERVICE'):
                    present = False
                return SQLResult([], [])
            resource.client.execute.side_effect = execute
            resource.workloads.snapshot.side_effect = lambda name: owned if present else {}
            resource.workloads.check_owners.side_effect = Workloads.check_owners
            resource.cleanup()
            self.assertEqual(resource.status, 'released')
            self.assertEqual(resource.services[0]['status'], 'released')
            self.assertTrue(all(call.kwargs['timeout'] == 120 for call in resource.client.execute.call_args_list))
        with self.assertRaises(Indeterminate):
            Workloads.check_owners({'Deployment/d': {'uid': 'other', 'owners': []}},
                                   {'Deployment/d': {'uid': 'original', 'owners': []}})

    def test_short_sql_polling_and_long_lifecycle_polling(self):
        from TCLIService import ttypes
        client = HS2Client({})
        client.client = Mock()
        handle = SimpleNamespace(operationId=SimpleNamespace(guid=b'id'), hasResultSet=False)
        client.client.ExecuteStatement.return_value = SimpleNamespace(status=SimpleNamespace(statusCode=0), operationHandle=handle)
        client.client.GetOperationStatus.side_effect = [
            SimpleNamespace(status=SimpleNamespace(statusCode=0), operationState=ttypes.TOperationState.RUNNING_STATE),
            SimpleNamespace(status=SimpleNamespace(statusCode=0), operationState=ttypes.TOperationState.FINISHED_STATE)]
        with patch('benchmark.model_quality.client.time.sleep') as sleep:
            client.execute('SELECT 1')
            sleep.assert_called_once_with(.1)
        with tempfile.TemporaryDirectory() as directory:
            resource = self.resource(directory)
            resource.execute('TRAIN MODEL m', long=True)
            self.assertEqual(resource.client.execute.call_args.kwargs['poll_interval'], 1)

    def test_deploy_reads_public_workload_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            resource = self.resource(directory)
            physical = 'mq-test-scalar-85cfc61389d8f3d4a338'
            owned = {'Deployment/' + physical: {'uid': 'uid1', 'owners': []}}
            resource.workloads.snapshot.return_value = owned
            resource.workloads.get.return_value = {'metadata': {'uid': 'uid1'}, 'status': {'availableReplicas': 1}}
            def execute(sql, **kwargs):
                if sql.startswith('DESCRIBE'):
                    return SQLResult(['key', 'value'], [['Model Name:', 'mq_test'], ['Checkpoint Name:', 'v1_export'],
                                                       ['URL:', f'http://{physical}.test.svc.cluster.local:80/predict']])
                return SQLResult([], [])
            resource.client.execute.side_effect = execute
            self.assertEqual(resource.deploy(['v1_export']), {'scalar': 'mq_test_scalar'})
            self.assertEqual(resource.services[0]['deployment'], physical)

    def test_cleanup_retains_verified_ancestry_when_parent_disappears(self):
        owned = {'Deployment/d': {'uid': 'd1', 'owners': []},
                 'ReplicaSet/r': {'uid': 'r1', 'owners': ['d1']}}
        # A Pod can still be visible after its already verified ReplicaSet disappears.
        Workloads.check_owners({'Pod/p': {'uid': 'p1', 'owners': ['r1']}}, owned)
        with self.assertRaises(Indeterminate):
            Workloads.check_owners({'Pod/other': {'uid': 'p2', 'owners': ['unknown']}}, owned)


class RunnerTest(unittest.TestCase):
    def test_failure_interrupt_and_cleanup_failure_stop_later_models(self):
        for failure, cleanup_failure, code in [(ValueError('prediction failed'), None, 1),
                                               (KeyboardInterrupt(), None, 130),
                                               (None, Indeterminate('not released'), 1)]:
            with self.subTest(failure=type(failure).__name__, cleanup=bool(cleanup_failure)), tempfile.TemporaryDirectory() as directory:
                env = environment()
                env.endpoints.update(warehouse={}, api={})
                args = SimpleNamespace(model='all', dataset='kuairand', full_eval=False,
                                       save_predictions=False, data_root=Path(directory), output=Path(directory))
                dataset = SimpleNamespace(train=[1, 2], items=pd.DataFrame({'video_id': ['a']}),
                                          manifest={'fingerprint': 'data', 'preparation_version': 'v'})
                client, api = Mock(), Mock()
                client.connect.return_value = client
                resources = Mock(client=client, times={})
                resources.stage.return_value = nullcontext()
                resources.cleanup.side_effect = cleanup_failure
                with patch.object(runner, 'arguments', return_value=args), \
                     patch.object(runner, 'environment', return_value=env), \
                     patch.object(runner, 'HS2Client', return_value=client), \
                     patch.object(runner, 'APIClient', return_value=api), \
                     patch.object(runner, 'RunResources', return_value=resources), \
                     patch.object(runner, 'load_dataset', return_value=dataset), \
                     patch.object(runner, 'training_table', return_value='train'), \
                     patch.object(runner, 'run_retrieval', side_effect=failure, return_value=([], {})) as predict, \
                     patch.object(runner, 'retrieval_metrics', return_value={}), \
                     patch.object(runner.logging, 'basicConfig'), patch.object(runner.logging, 'FileHandler'):
                    self.assertEqual(runner.main(), code)
                results = read_json(next(Path(directory).glob('*/results.json')))['results']
                self.assertEqual(results[0]['status'], 'FAILED')
                self.assertTrue(all(item['status'] == 'NOT_EXECUTED' for item in results[1:]))
                predict.assert_called_once()
                resources.cleanup.assert_called_once()
                api.close.assert_called_once()
                self.assertEqual(client.close.call_count, 2)
                if cleanup_failure:
                    self.assertEqual(resources.status, 'cleanup_failed')
                    self.assertIn('not released', results[0]['cleanup_error'])


class ReportTest(unittest.TestCase):
    def test_full_matrix_is_29_rows_without_historical_comparison(self):
        results = []
        for dataset in config.DATASETS:
            for model in config.applicable_models(dataset, 'all'):
                _, tasks = config.view_fields(config.dataset_definition(dataset), model)
                results.append(dict(dataset=dataset, model=model, kind='retrieval' if model == 'dssm' else 'ranking',
                                    tasks=[task.id for task in tasks], candidate_scope='full_catalog', status='NOT_EXECUTED'))
        before = copy.deepcopy(results)
        with tempfile.TemporaryDirectory() as directory:
            write_report(directory, results, False)
            lines = (Path(directory) / 'summary.md').read_text().splitlines()
            self.assertEqual(sum(line.startswith(tuple('| ' + name for name in config.DATASETS)) for line in lines), 29)
            self.assertNotIn('baseline', '\n'.join(lines))
            self.assertEqual(results, before)



class SpeedExecutionTest(unittest.TestCase):
    def test_item_write_is_not_retried_but_scoring_read_can_retry(self):
        from benchmark.model_quality.client import APIClient, SQLFailed
        api = APIClient({'base_url': 'http://unused'})
        self.addCleanup(api.close)
        failed = Mock(status_code=500)
        success = Mock(status_code=200)
        success.json.return_value = {'data': []}
        with patch.object(api.session, 'post', side_effect=[failed, success]) as post:
            with self.assertRaises(SQLFailed):
                api.call('load_items', {'data': {}}, retry=False)
            self.assertEqual(post.call_count, 1)
        with patch.object(api.session, 'post', side_effect=[failed, success]) as post, \
             patch('benchmark.model_quality.client.time.sleep'):
            self.assertEqual(api.call('score', {'data': {}}), [])
            self.assertEqual(post.call_count, 2)

    def test_recall_parallelism_preserves_order_empty_results_and_closes_sessions(self):
        import threading
        from benchmark.model_quality.retrieval import predict_queries
        barrier = threading.Barrier(4)
        clients = []
        threads = set()
        def make_client(config):
            client = Mock()
            def call(name, payload):
                threads.add(threading.get_ident())
                barrier.wait(timeout=5)
                query_id = payload['data']['query_input'][0]['query_id']
                return [] if query_id == '0' else [{'query_id': query_id, 'item_id': 'a', 'score': 1}]
            client.call.side_effect = call
            clients.append(client)
            return client
        queries = [{'query_id': str(i), 'features': {'query_id': str(i)}, 'seen': []} for i in range(4)]
        with patch('benchmark.model_quality.retrieval.APIClient', side_effect=make_client):
            result = predict_queries(queries, [config.Feature('query_id', 'STRING')], False, 'recall', {})
        self.assertEqual(list(result), ['0', '1', '2', '3'])
        self.assertEqual(result['0'], [])
        self.assertEqual(len(threads), 4)
        for client in clients:
            client.close.assert_called_once()

    def test_recall_failure_closes_worker_sessions(self):
        from benchmark.model_quality.retrieval import predict_queries
        client = Mock()
        client.call.side_effect = ValueError('prediction failed')
        queries = [{'query_id': 'q', 'features': {'query_id': 'q'}, 'seen': []}]
        with patch('benchmark.model_quality.retrieval.APIClient', return_value=client):
            with self.assertRaisesRegex(ValueError, 'prediction failed'):
                predict_queries(queries, [config.Feature('query_id', 'STRING')], False, 'recall', {})
        client.close.assert_called_once()
