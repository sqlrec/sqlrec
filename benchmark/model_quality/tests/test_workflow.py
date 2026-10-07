"""Public workflow contracts, lossless metrics, and ambiguous-mutation recovery."""
import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from benchmark.model_quality.common import captured_result, digest, read_json, stable_id, write_json
from benchmark.model_quality.user_workflow.recipes import build, input_sql
from benchmark.model_quality.user_workflow.results import evaluate, summarize_runs
from benchmark.model_quality.metrics import recall_metrics, scalar_metrics
from benchmark.model_quality.user_workflow.runner import Runner, validate_capture, verify_plan
from benchmark.model_quality.user_workflow.specs import kubernetes_name, model_name, Feature, ModelRecipe, SQLStep, Task, WorkflowPlan, validate_config
from benchmark.model_quality.user_workflow.sql_client import HS2Client, Indeterminate, SQLFailed, SQLResult, decode_column, resolve_environment
from benchmark.model_quality.user_workflow.sql_client import APIClient
from benchmark.model_quality.user_workflow.runtime_audit import RuntimeAudit, compare_dssm, compare_pipeline


def fixture(root, family='tzrec.dssm'):
    data = root / 'prepared'; data.mkdir()
    train = pd.DataFrame([{'row_id': 't1', 'user_id': 'u1', 'video_id': 'i1', 'label': 1},
                          {'row_id': 't2', 'user_id': 'u1', 'video_id': 'i2', 'label': 0},
                          {'row_id': 't3', 'user_id': 'u2', 'video_id': 'i2', 'label': 1},
                          {'row_id': 't4', 'user_id': 'u2', 'video_id': 'i1', 'label': 1}])
    valid = pd.DataFrame([{'row_id': 'v1', 'query_id': 'q1', 'user_id': 'u1', 'video_id': 'i3', 'label': 1},
                          {'row_id': 'v2', 'query_id': 'q1', 'user_id': 'u1', 'video_id': 'i2', 'label': 0},
                          {'row_id': 'v3', 'query_id': 'q2', 'user_id': 'u2', 'video_id': 'i3', 'label': 0}])
    queries = valid[['query_id', 'user_id']].drop_duplicates()
    items = pd.DataFrame({'video_id': ['i1', 'i2', 'i3'], 'eligible': [True]*3})
    for name, frame in [('train', train), ('valid', valid), ('queries_valid', queries), ('items', items)]:
        frame.to_parquet(data / f'{name}.parquet', index=False)
    write_json(data / 'manifest.json', {'fingerprint': 'fixture', 'candidate_scope': 'full_catalog', 'files': {p.name: digest(p) for p in data.glob('*.parquet')},
                                       'splits': {'train': ['train.parquet'], 'valid': ['valid.parquet']}})
    config = {'schema_version': 3, 'execution_profile': 'user_workflow', 'mode': 'fixed_validation', 'name': 'wf_test',
              'dataset': {'path': str(data), 'storage_uri': 'jfs://myjfs/test/wf_test',
                          'user_key': 'user_id', 'item_key': 'video_id'},
              'features': [{'name': 'user_id', 'type': 'STRING', 'role': 'user'},
                           {'name': 'video_id', 'type': 'STRING', 'role': 'item'}],
              'tasks': [{'id': 'click', 'column': 'label', 'type': 'binary'}],
              'models': [{'id': 'model', 'family': family, 'params': {'output_dim': 2} if family=='tzrec.dssm' else {}}],
              'protocol': {'exclude_seen': True, 'recall_denominator': 'all_targets', 'allow_shortlist': True},
              'serving': {'entrypoint': 'sql', 'batch_size': 2, 'candidate_limit': 3},
              'evaluation': {'split': 'valid', 'ks': [1, 2]},
              'execution': {'seed': 123, 'resources': {'NAMESPACE': 'sqlrec'}},
              'sql': {'endpoints': {'sqlrec': {'host':'localhost','port':10000}, 'warehouse': {'host':'localhost','port':10001}, 'api': {'base_url':'http://localhost'},
                                    'milvus': {'url': 'http://localhost', 'token_env': 'TEST_MILVUS_SECRET'}}}}
    return config


class WorkflowContractTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        self.config = fixture(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def build(self, config=None):
        return build(config or self.config, self.root / 'run')

    def test_multiple_training_seeds_are_rejected(self):
        self.config['execution'].pop('seed')
        self.config['execution']['seeds'] = [123, 456]
        with self.assertRaisesRegex(ValueError, 'execution'):
            validate_config(self.config)

    def test_dssm_full_public_recipe_and_label_free_requests(self):
        plan = self.build(); run = self.root/'run'
        sql = '\n'.join((run/p).read_text() for p in plan['files'] if p.endswith('.sql'))
        for keyword in ['CREATE MODEL', 'TRAIN MODEL', 'EXPORT MODEL', 'CREATE SERVICE',
                        "CALL call_service", "'connector'='milvus'", 'INSERT INTO', 'CALL dedup']:
            self.assertIn(keyword, sql)
        for path in plan['files']:
            if path.startswith('requests/'):
                payload = read_json(run/path)
                self.assertNotIn('label', json.dumps(payload))
        bundle = read_json(run/'data_contract.json')
        self.assertEqual(bundle['train_rows'], 4)
        self.assertEqual(bundle['positive_train_rows'], 3)
        self.assertEqual(len(bundle['items']), 3)
        self.assertEqual(next(q['seen'] for q in bundle['queries'] if q['query_id']=='q1'), ['i1', 'i2'])
        self.assertEqual(verify_plan(run)['plan_id'], plan['plan_id'])

    def test_declared_label_type_reaches_parquet_and_model_sql(self):
        self.config['tasks'][0]['data_type'] = 'BIGINT'
        plan = self.build(); root = self.root / 'run'
        self.assertEqual(pq.read_schema(root / 'data/samples/split=train/part.parquet').field('label').type, pa.int64())
        define = next(s for s in plan['steps'] if s['kind'] == 'define')
        self.assertIn('`label` BIGINT', (root / define['files'][0]).read_text())
        self.assertEqual(plan['tasks'][0]['data_type'], 'BIGINT')
        self.assertEqual(Task('duration', 'duration', 'regression', 'DOUBLE').data_type, 'DOUBLE')

    def test_candidate_budget_detects_seen_filter_truncation(self):
        self.config['serving']['candidate_limit'] = 2
        self.build()
        budget = read_json(self.root / 'run/data_contract.json')['candidate_budget']
        self.assertTrue(budget['candidate_limited'])
        self.assertEqual(budget['required_for_complete_topk'], 3)

    def test_strict_topk_rejects_insufficient_candidate_budget(self):
        self.config['serving']['candidate_limit'] = 2
        self.config['protocol']['allow_shortlist'] = False
        with self.assertRaisesRegex(ValueError, 'after excluding seen'):
            self.build()
        self.assertFalse((self.root / 'run').exists())

    def test_compact_resource_names_keep_native_suffixes_unique(self):
        self.config['name'] = 'experiment_' + 'x' * 90
        self.config['models'] *= 2
        self.config['models'][1] = copy.deepcopy(self.config['models'][1])
        self.config['models'][1]['id'] = 'another_model'
        plan = self.build()
        names = []
        for model, resources in plan['resources']['models'].items():
            self.assertLessEqual(len(model), 40)
            for job in resources['jobs']:
                names.append(job['name'])
                self.assertLessEqual(len(job['name'] + '-headless'), 63)
            names.extend(b['deployment'] for b in resources['services'])
        self.assertEqual(len(names), len(set(names)))
        self.assertNotEqual(model_name('a__b', 'c', 1), model_name('a_b', 'c', 1))

    def test_api_profile_publishes_functions_without_direct_predict(self):
        self.config['serving']['entrypoint'] = 'sql_api'
        plan = self.build(); root = self.root/'run'
        sql = '\n'.join((root/p).read_text() for p in plan['files'] if p.endswith('.sql'))
        self.assertIn('CREATE OR REPLACE API', sql)
        self.assertNotIn('/predict', sql)
        self.assertTrue(all(not s['files'] for s in plan['steps'] if (s.get('capture') or {}).get('kind') in ('recall','scalar')))

    def test_feature_or_sql_tamper_rejected(self):
        plan = self.build(); root = self.root/'run'
        (root/next(iter(plan['files']))).write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'changed'):
            verify_plan(root)

    def test_source_checksum_checked(self):
        (Path(self.config['dataset']['path'])/'train.parquet').write_bytes(b'bad')
        with self.assertRaisesRegex(ValueError, 'checksum'):
            self.build()

    def test_invalid_config_fails_before_creating_any_output(self):
        cases = [('dataset', 'unknown_adapter_option', True), ('protocol', 'candidate_scope', 'observed_only'),
                 ('serving', 'batch_size', True), ('evaluation', 'ks', [2, 1]),
                 ('evaluation', 'metrics', ['recall_at_999']), ('execution', 'seed', True),
                 ('protocol', 'allow_shortlist', 'false')]
        for section, name, value in cases:
            with self.subTest(section=section, name=name):
                config = copy.deepcopy(self.config); config[section][name] = value
                with self.assertRaises(ValueError):
                    self.build(config)
                self.assertFalse((self.root/'run').exists())
        config = copy.deepcopy(self.config)
        config['execution']['resources']['hidden_native_option'] = True
        with self.assertRaises(ValueError):
            self.build(config)
        self.assertFalse((self.root/'run').exists())

    def test_source_candidate_protocol_cannot_be_silently_changed(self):
        path = self.root/'prepared/manifest.json'
        manifest = read_json(path)
        manifest['protocol'] = 'held-out-preference-observed-candidates'
        manifest['candidate_scope'] = 'observed_only'
        write_json(path, manifest)
        with self.assertRaisesRegex(ValueError, 'candidate scope'):
            self.build()
        self.assertFalse((self.root/'run').exists())

    def test_movielens_source_requires_training_rated_exclusion(self):
        path = self.root/'prepared/manifest.json'; manifest = read_json(path)
        manifest.update(protocol='movielens-user-time-full-catalog', exclude_train_rated_required=True)
        write_json(path, manifest); self.config['protocol']['exclude_seen'] = False
        with self.assertRaisesRegex(ValueError, 'training-observed'):
            self.build()
        self.assertFalse((self.root/'run').exists())

    def test_feature_aliases_use_the_same_columns_in_training_and_requests(self):
        self.config['features'] = [{'name':'viewer', 'source':'user_id', 'type':'STRING', 'role':'user'},
                                   {'name':'movie', 'source':'video_id', 'type':'STRING', 'role':'item'}]
        plan = self.build(); root = self.root/'run'
        train = pd.read_parquet(root/'data/samples/split=train/part.parquet')
        self.assertEqual(train.columns.tolist(), ['viewer', 'movie', 'label'])
        self.assertEqual(train.viewer.tolist(), ['u1','u1','u2','u2'])
        bundle = read_json(root/'data_contract.json')
        self.assertEqual(bundle['scalar_inputs'][0], {'row_id':'v1','viewer':'u1','movie':'i3'})
        sql = '\n'.join((root/p).read_text() for p in plan['files'] if p.endswith('.sql'))
        self.assertIn("'user_features'='viewer'", sql)
        self.assertIn("'item_features'='movie'", sql)
        self.assertIn('`viewer` STRING', sql)
        self.assertNotIn('`video_id` STRING', sql)

    def test_feature_source_alias_cannot_hide_a_label(self):
        self.config['features'][0]['source'] = 'label'
        with self.assertRaisesRegex(ValueError, 'Labels'):
            self.build()
        self.assertFalse((self.root/'run').exists())

    def test_integer_source_id_uses_the_same_string_view_for_query_binding(self):
        data = Path(self.config['dataset']['path'])
        manifest = read_json(data/'manifest.json')
        for name in ('train.parquet', 'valid.parquet', 'queries_valid.parquet'):
            frame = pd.read_parquet(data/name)
            frame['user_id'] = frame.user_id.map({'u1': 1, 'u2': 2})
            frame.to_parquet(data/name, index=False)
            manifest['files'][name] = digest(data/name)
        write_json(data/'manifest.json', manifest)
        self.build()
        bundle = read_json(self.root/'run/data_contract.json')
        self.assertEqual({r['user_id'] for r in bundle['scalar_inputs']}, {'1', '2'})
        self.assertEqual({q['features']['user_id'] for q in bundle['queries']}, {'1', '2'})
        self.assertEqual(next(q['seen'] for q in bundle['queries'] if q['query_id']=='q1'), ['i1', 'i2'])

    def test_invalid_query_bindings_fail_before_writing_training_data(self):
        data = Path(self.config['dataset']['path'])
        queries = pd.read_parquet(data/'queries_valid.parquet')
        cases = [queries.iloc[:1], queries.assign(user_id='wrong_user'),
                 pd.concat([queries, queries.iloc[:1]], ignore_index=True)]
        for invalid in cases:
            with self.subTest(rows=len(invalid)):
                invalid.to_parquet(data/'queries_valid.parquet', index=False)
                manifest = read_json(data/'manifest.json')
                manifest['files']['queries_valid.parquet'] = digest(data/'queries_valid.parquet')
                write_json(data/'manifest.json', manifest)
                with self.assertRaisesRegex(ValueError, 'quer|Query'):
                    self.build()
                self.assertFalse((self.root/'run').exists())

    def test_resource_types_match_the_public_product_config(self):
        self.config['execution']['resources'].update(pod_cpu_cores=1, pod_cpu_limit='1500m')
        validated = validate_config(self.config)
        self.assertEqual(validated['execution']['resources']['pod_cpu_limit'], '1500m')
        for invalid in (True, 0.5, '1'):
            self.config['execution']['resources']['pod_cpu_cores'] = invalid
            with self.subTest(cpu_request=invalid), self.assertRaises(ValueError):
                validate_config(self.config)
        self.config['execution']['resources']['pod_cpu_cores'] = 1
        self.config['features'][0]['type'] = False
        with self.assertRaises(ValueError):
            validate_config(self.config)

    def test_null_gauc_group_is_rejected_before_training(self):
        self.config['evaluation']['gauc_group'] = 'cohort'
        data = Path(self.config['dataset']['path'])
        valid = pd.read_parquet(data/'valid.parquet').assign(cohort=['a', None, 'b'])
        valid.to_parquet(data/'valid.parquet', index=False)
        manifest = read_json(data/'manifest.json'); manifest['files']['valid.parquet'] = digest(data/'valid.parquet')
        write_json(data/'manifest.json', manifest)
        with self.assertRaisesRegex(ValueError, 'Null GAUC group'):
            self.build()
        self.assertFalse((self.root/'run').exists())

    def test_report_separates_quality_and_displays_configured_k(self):
        for selected in (None, ['recall_at_2']):
            with self.subTest(metrics=selected):
                config = copy.deepcopy(self.config)
                if selected:
                    config['evaluation']['metrics'] = selected
                root = self.root / ('selected' if selected else 'default')
                plan = build(config, root)
                journal = {'plan_id': plan['plan_id'], 'formal_transport': False,
                           'status': 'succeeded', 'steps': {}}
                for step in plan['steps']:
                    record = {'status': 'succeeded'}
                    capture = step.get('capture') or {}
                    if capture.get('kind') in ('scalar', 'recall'):
                        rows = ([{'row_id': i, 'score': 0.5} for i in capture['row_ids']]
                                if capture['kind'] == 'scalar' else [])
                        path = f'execution/{step["id"]}.json'
                        write_json(root/path, {'results': [{'role': 'prediction', 'source': 'public_sql', 'rows': rows}]})
                        record.update(artifact=path, sha256=digest(root/path))
                    journal['steps'][step['id']] = record
                write_json(root/'execution/journal.json', journal)
                report = evaluate(root)
                self.assertEqual(report['workflow_status'], 'completed')
                self.assertEqual(report['parameters_status'], 'unverified')
                self.assertEqual(report['quality_status'], 'not_assessed')
                self.assertEqual(report['status'], 'test_only')
                text = (root/'results/report.md').read_text()
                self.assertIn('| recall_at_2 |', text)
                self.assertNotIn('| recall_at_20 |', text)
                if selected:
                    self.assertNotIn('| ndcg_at_2 |', text)
                    self.assertNotIn('| auc |', text)
                else:
                    self.assertIn('| recall_at_1 |', text)
                    self.assertIn('| ndcg_at_2 |', text)
                self.assertIn('ndcg_at_2', report['results'][0]['retrieval'])

    def test_explicit_stage_metadata_and_v3_readonly_compatibility(self):
        plan = self.build(); root = self.root/'run'
        train = next(s for s in plan['steps'] if s['kind']=='train')
        self.assertEqual((train['model_id'], train['recipe_id'], train['seed']), ('wf_test_model_s123','model',123))
        for step in plan['steps']:
            self.assertEqual(len(step['files']), len(step['result_roles']))
        # A genuine v3 layout has no typed stage/role/recipe metadata.
        plan['format_version'] = 3
        plan['config']['execution']['seeds'] = [plan['config']['execution'].pop('seed')]
        for step in plan['steps']:
            for field in ('kind','model_id','recipe_id','seed','result_roles'):
                step.pop(field)
            if step.get('capture'):
                step['capture'].pop('result_role', None)
        for evaluation in plan['evaluations']:
            evaluation.pop('recipe_id')
        plan.pop('plan_id'); plan['plan_id'] = stable_id(plan)
        write_json(root/'resolved_plan.json', plan)
        before = (root/'resolved_plan.json').read_bytes()
        adapted = verify_plan(root)
        self.assertEqual(adapted['plan_id'], plan['plan_id'])
        self.assertEqual(next(s['model_id'] for s in adapted['steps'] if s['kind']=='train'), 'wf_test_model_s123')
        self.assertEqual((root/'resolved_plan.json').read_bytes(), before)

    def test_labels_cannot_be_features(self):
        self.config['features'].append({'name': 'label', 'type': 'INT', 'role': 'item'})
        with self.assertRaisesRegex(ValueError, 'Labels'):
            self.build()

    def test_private_sampler_rejected_before_resources(self):
        self.config['models'][0]['params']['sampler'] = 'local_negative'
        with self.assertRaisesRegex(ValueError, 'unsupported_public_workflow'):
            self.build()
        self.assertFalse((self.root/'run').exists())

    def test_parameters_for_other_families_are_not_silently_ignored(self):
        self.config['models'][0]['params']['cb_depth'] = 6
        with self.assertRaisesRegex(ValueError, 'unsupported_public_workflow'):
            self.build()

    def test_non_sql_entrypoint_rejected(self):
        self.config['serving']['entrypoint'] = 'native'
        with self.assertRaises(ValueError):
            self.build()

    def test_test_labels_and_unbound_native_eval_paths_are_not_opened(self):
        self.config['evaluation']['split']='test'
        with self.assertRaisesRegex(ValueError,'valid-only'):
            self.build()
        self.config['evaluation']['split']='valid'
        self.config['models'][0]['params']['eval_input_path']='jfs://myjfs/test.parquet'
        with self.assertRaisesRegex(ValueError,'frozen validation'):
            self.build()

    def test_tree_requires_explicit_float_view(self):
        self.config['models'][0]['family'] = 'gbdt.xgboost'
        self.config['models'][0]['params'] = {}
        with self.assertRaisesRegex(ValueError, 'float feature'):
            self.build()

    def test_real_multitask_output_binding(self):
        recipe = ModelRecipe('multi', 'tzrec.mmoe')
        tasks = [Task('conversion', 'buy'), Task('duration', 'seconds', 'regression')]
        recipe.validate([Feature('x', 'FLOAT')], tasks)
        self.assertEqual(recipe.outputs(tasks), {'conversion': 'probs_buy', 'duration': 'y_seconds'})
        self.assertEqual(recipe.parameters([Feature('x','FLOAT')], tasks, 123)['task.seconds.type'], 'regression')
        with self.assertRaises(ValueError):
            recipe.validate([Feature('x', 'FLOAT')], tasks[:1])

    def test_empty_inputs_are_real_empty_tables(self):
        sql = input_sql('seen', [Feature('item_id','STRING')], [])
        self.assertIn('WHERE FALSE', sql)
        self.assertIn('FROM (VALUES(1))', sql)
        self.assertIn("'a''b'", input_sql('x', [Feature('id','STRING')], [{'id': "a'b"}]))

    def test_partial_report_never_passes_or_scores_a_partial_batch_set(self):
        self.config['models']=[{'id':'rank','family':'tzrec.wide_and_deep','params':{}}]
        plan=self.build(); root=self.root/'run'
        journal={'plan_id':plan['plan_id'],'formal_transport':False,'status':'running','steps':{}}
        bundle=read_json(root/'data_contract.json')
        labels={r['row_id']:r['label'] for r in bundle['observations']}
        scalar_steps=[s for s in plan['steps'] if (s.get('capture') or {}).get('kind')=='scalar']
        for step in scalar_steps:
            rows=[{'row_id':i,'probs':0.9 if labels[i] else 0.1} for i in step['capture']['row_ids']]
            path=f'execution/{step["id"]}.json'; write_json(root/path,{'results':[{'source':'public_sql','rows':rows,'role':'prediction'}]})
            journal['steps'][step['id']]={'status':'succeeded','artifact':path,'sha256':digest(root/path)}
        write_json(root/'execution/journal.json',journal)
        with self.assertRaisesRegex(ValueError,'not all succeeded'):
            evaluate(root)
        report=evaluate(root,diagnostic=True)
        self.assertFalse(report['formal'])
        self.assertEqual(report['status'],'incomplete_workflow')
        self.assertEqual(report['results'][0]['scalar']['click']['auc'],1)
        journal['steps'][scalar_steps[-1]['id']]['status']='running'
        write_json(root/'execution/journal.json',journal)
        report=evaluate(root,diagnostic=True)
        self.assertEqual(report['results'][0]['status'],'incomplete_scalar_predictions')
        self.assertNotIn('scalar',report['results'][0])


class MetricsTest(unittest.TestCase):
    def test_auc_uses_identity_and_no_probability_on_raw_scores(self):
        truth = [{'row_id': 'a', 'label': 1}, {'row_id': 'b', 'label': 0}]
        pred = [{'row_id': 'b', 'score': -2}, {'row_id': 'a', 'score': 3}]
        tasks = [{'id':'click', 'column':'label','type':'binary'}]
        result = scalar_metrics(truth, pred, tasks, {'click':'score'}, False)['click']
        self.assertEqual(result['auc'], 1)
        self.assertIsNone(result['logloss'])
        with self.assertRaises(ValueError):
            scalar_metrics(truth, pred, tasks, {'click':'score'}, True)
        with self.assertRaises(ValueError):
            scalar_metrics(truth, pred[:1], tasks, {'click':'score'}, False)

    def test_multitask_regression_and_binary(self):
        truth = [{'row_id':'a','buy':1,'seconds':3}, {'row_id':'b','buy':0,'seconds':1}]
        pred = [{'row_id':'b','p':0.2,'y':2}, {'row_id':'a','p':0.8,'y':4}]
        tasks = [{'id':'buy','column':'buy','type':'binary'}, {'id':'duration','column':'seconds','type':'regression'}]
        result = scalar_metrics(truth, pred, tasks, {'buy':'p','duration':'y'}, True)
        self.assertEqual(result['buy']['auc'], 1)
        self.assertEqual(result['duration']['rmse'], 1)

    def bundle(self):
        return {'queries': [{'query_id':'q1','seen':['a']}, {'query_id':'q2','seen':[]}],
                'items': [{'item_id':'a'}, {'item_id':'b'}, {'item_id':'c'}],
                'targets': {'q1':['b','outside'], 'q2':[]}}

    def test_all_target_denominator_and_zero_positive_empty_list(self):
        responses = {'q1':[{'query_id':'q1','item_id':'b','score':2}], 'q2':[]}
        result, _ = recall_metrics(self.bundle(), responses, [1,2])
        self.assertEqual(result['recall_at_1'], 0.5)
        self.assertEqual(result['micro_recall_at_2'], 0.5)
        self.assertEqual(result['zero_positive_queries'], 1)
        self.assertEqual(result['empty_lists'], 1)
        self.assertEqual(result['unretrievable_target_rate'], 0.5)
        with self.assertRaisesRegex(ValueError, 'Shortlist'):
            recall_metrics(self.bundle(), responses, [1,2], False)

    def test_missing_response_not_converted_into_empty_list(self):
        with self.assertRaisesRegex(ValueError, 'Missing query'):
            recall_metrics(self.bundle(), {'q1':[]}, [1])

    def test_illegal_or_unsorted_results_fail(self):
        for rows in [[{'query_id':'q1','item_id':'a','score':1}],
                     [{'query_id':'q1','item_id':'b','score':1}, {'query_id':'q1','item_id':'c','score':2}]]:
            with self.assertRaises(ValueError):
                recall_metrics(self.bundle(), {'q1':rows,'q2':[]}, [1,2])

    def test_null_service_output_and_vector_schema_rejected(self):
        with self.assertRaisesRegex(ValueError, 'null'):
            validate_capture({'kind':'scalar','row_ids':['a'],'outputs':{'x':'probs'}}, [{'rows':[{'row_id':'a','probs':None}]}])
        with self.assertRaisesRegex(ValueError, 'dimension'):
            validate_capture({'kind':'vectors','dimension':2,'rows':1},
                             [{'rows':[{'catalog_id':1,'item_tower_emb':[0.1]}]}, {'rows':[]}])

    def test_result_roles_and_exact_vector_identity_are_enforced(self):
        capture = {'kind':'scalar', 'result_role':'prediction', 'row_ids':['a'], 'outputs':{'x':'probs'}}
        evidence = [{'role':'prediction', 'rows':[{'row_id':'a','probs':.7}]},
                    {'role':'completion_proof','rows':[{'status':'succeeded'}]}]
        validate_capture(capture, evidence)
        self.assertEqual(captured_result(capture, evidence)['rows'][0]['probs'], .7)
        with self.assertRaisesRegex(ValueError, 'exactly one'):
            captured_result(capture, evidence[1:])
        with self.assertRaisesRegex(ValueError, 'exactly one'):
            captured_result(capture, evidence+evidence[:1])
        vectors = {'kind':'vectors','result_role':'vectors','dimension':2,'rows':1,'catalog_ids':[1]}
        with self.assertRaisesRegex(ValueError, 'identities'):
            validate_capture(vectors, [{'role':'vectors','rows':[{'catalog_id':99,'item_tower_emb':[.1,.2]}]}])


class TransportAndRecoveryTest(unittest.TestCase):
    def test_function_replay_depends_on_stage_kind_not_identifier_suffix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root/'function.sql').write_text('CREATE OR REPLACE SQL FUNCTION isolated;')
            step = SQLStep('renamed_stage', 'sqlrec', ['function.sql'], 'safe', kind='function', result_roles=['sql'])
            plan = WorkflowPlan('isolated', 'fixture', {'sql':{'endpoints':{'sqlrec':{}}},'execution':{}},
                                [], [], [step], {'function.sql':digest(root/'function.sql')}, []).to_dict()
            write_json(root/'resolved_plan.json', plan)
            class Fake:
                submissions = 0
                def __init__(self, config): pass
                def connect(self): return self
                def close(self): pass
                def execute(self, sql, **kwargs):
                    if sql.startswith('CREATE'):
                        Fake.submissions += 1
                    return SQLResult([], [])
            self.assertEqual(Runner(root, client_factory=Fake).run()['status'], 'succeeded')
            self.assertEqual(Runner(root, client_factory=Fake).run()['status'], 'succeeded')
            self.assertEqual(Fake.submissions, 2)

    def test_external_table_recovery_verifies_schema_location_and_partition(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / 'table.sql').write_text('CREATE EXTERNAL TABLE `observations` (`uid` STRING, `label` BIGINT);')
            contract = {'kind': 'table_schema', 'name': 'observations',
                        'columns': {'uid': 'STRING', 'label': 'BIGINT'}, 'partitions': {'split': 'STRING'},
                        'location': 'jfs://shared/run/samples'}
            step = SQLStep('table', 'warehouse', ['table.sql'], 'never', capture=contract,
                           kind='table_create', result_roles=['sql'])
            plan = WorkflowPlan('isolated', 'fixture', {'sql': {'endpoints': {'warehouse': {}}}, 'execution': {}},
                                [], [], [step], {'table.sql': digest(root / 'table.sql')}, []).to_dict()
            write_json(root / 'resolved_plan.json', plan)
            class SQL:
                location = contract['location']; label_type = 'bigint'; partition_sections = False
                def __init__(self, config): pass
                def connect(self): return self
                def close(self): pass
                def execute(self, sql, **kwargs):
                    if sql.startswith('DESCRIBE FORMATTED'):
                        if SQL.partition_sections:
                            return SQLResult(['col_name', 'data_type', 'comment'], [
                                ['# Detailed Partition Information', '', ''], ['Location', SQL.location, ''],
                                ['# Storage Information', '', ''], ['Location', contract['location'], '']])
                        return SQLResult(['col_name', 'data_type'], [['Location:', SQL.location]])
                    return SQLResult(['col_name', 'data_type'], [['uid', 'string'], ['label', SQL.label_type],
                        ['split', 'string'], ['', ''], ['# Partition Information', ''], ['split', 'string']])
            runner = Runner(root, client_factory=SQL)
            try:
                self.assertTrue(runner.reconcile_table(plan['steps'][0])['reconciled'])
                SQL.label_type = 'int'
                with self.assertRaisesRegex(ValueError, 'schema'): runner.reconcile_table(plan['steps'][0])
                SQL.label_type = 'bigint'; SQL.location = 'jfs://another/table'
                with self.assertRaisesRegex(ValueError, 'location'): runner.reconcile_table(plan['steps'][0])
                partition = copy.deepcopy(plan['steps'][0])
                partition['capture'].update(kind='table_partition', partition={'split': 'train'},
                                             partition_location=contract['location'] + '/split=train')
                SQL.location = contract['location'] + '/split=train'
                self.assertTrue(runner.reconcile_table(partition)['reconciled'])
                SQL.partition_sections = True
                self.assertTrue(runner.reconcile_table(partition)['reconciled'])
                SQL.location = contract['location'] + '/split=wrong'
                with self.assertRaisesRegex(ValueError, 'location'): runner.reconcile_table(partition)
            finally:
                runner.close()

    def test_readonly_api_readiness_retries_preserve_statuses(self):
        api = APIClient({'base_url':'http://localhost','readiness_timeout':10})
        bad = SimpleNamespace(status_code=500)
        good = SimpleNamespace(status_code=200, json=lambda:{'data':[{'score':0.2}]})
        with patch.object(api.session,'post',side_effect=[bad,good]), patch('time.sleep'):
            self.assertEqual(api.call('score',{'data':{}}),[{'score':0.2}])
        self.assertEqual(api.last_call['attempt_statuses'],[500,200]); api.close()

    def test_bad_api_input_is_not_retried(self):
        api = APIClient({'base_url':'http://localhost'})
        with patch.object(api.session,'post',return_value=SimpleNamespace(status_code=400)) as post:
            with self.assertRaisesRegex(SQLFailed,'HTTP 400'):
                api.call('score',{'data':{}})
            self.assertEqual(post.call_count,1)
        api.close()

    def test_interrupted_rpc_discards_the_stream_before_cleanup(self):
        closed = []
        client = HS2Client({})
        client.transport = SimpleNamespace(close=lambda: closed.append(True))
        client.session = object()
        class Wire:
            def ExecuteStatement(self, request): raise KeyboardInterrupt()
            def CloseOperation(self, request): raise AssertionError('Interrupted stream must not be reused')
        client.client = Wire()
        with self.assertRaises(KeyboardInterrupt): client.execute('SELECT 1')
        self.assertTrue(client.transport_broken)
        self.assertIsNone(client.transport)
        self.assertIsNone(client.session)
        client.close()
        self.assertEqual(closed, [True])
    def test_thrift_null_bitmask_preserves_position(self):
        from TCLIService import ttypes
        column = ttypes.TColumn(doubleVal=ttypes.TDoubleColumn(values=[0.3, 0, 0.9], nulls=b'\x02'))
        self.assertEqual(decode_column(column), [0.3,None,0.9])

    def test_only_client_delimiter_removed_and_local_results_fetched_once(self):
        from TCLIService import ttypes
        class Wire:
            def ExecuteStatement(self, req):
                self.statement = req.statement
                handle = SimpleNamespace(operationId=SimpleNamespace(guid=b'guid'), hasResultSet=True)
                return SimpleNamespace(status=SimpleNamespace(statusCode=0), operationHandle=handle)
            def GetOperationStatus(self, req):
                return SimpleNamespace(status=SimpleNamespace(statusCode=0), operationState=ttypes.TOperationState.FINISHED_STATE)
            def GetResultSetMetadata(self, req):
                return SimpleNamespace(status=SimpleNamespace(statusCode=0), schema=SimpleNamespace(columns=[SimpleNamespace(columnName='v')]))
            def FetchResults(self, req):
                self.fetched = getattr(self,'fetched',0)+1
                return SimpleNamespace(status=SimpleNamespace(statusCode=0), hasMoreRows=False,
                    results=SimpleNamespace(rows=[], columns=[ttypes.TColumn(stringVal=ttypes.TStringColumn(values=['a;b'], nulls=b''))]))
            def CloseOperation(self, req):
                pass
        client = HS2Client({}); wire = Wire(); client.client=wire; client.session=object()
        self.assertEqual(client.execute("SELECT 'a;b';\n").records(), [{'v':'a;b'}])
        self.assertEqual(wire.statement, "SELECT 'a;b'")
        self.assertEqual(wire.fetched, 1)

    def test_environment_secret_only_resolved_at_execution(self):
        os.environ['TEST_WORKFLOW_SECRET'] = "a'b"
        try:
            self.assertEqual(resolve_environment("'{{env:TEST_WORKFLOW_SECRET}}'"), "'a''b'")
        finally:
            del os.environ['TEST_WORKFLOW_SECRET']

    def test_ambiguous_train_never_resubmitted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root/'train.sql').write_text('TRAIN MODEL isolated;')
            step = SQLStep('train', 'sqlrec', ['train.sql'], 'verify',
                           [{'kind':'checkpoint','name':'isolated','checkpoint':'v1','type':'origin'}],
                           kind='train', model_id='isolated', result_roles=['mutation'])
            plan = WorkflowPlan('isolated','fixture',{'sql':{'endpoints':{'sqlrec':{}}}, 'execution': {}},[],[],[step],
                                {'train.sql':digest(root/'train.sql')},[]).to_dict()
            write_json(root/'resolved_plan.json', plan)
            class Fake:
                submissions=0; complete=False
                def __init__(self, config):
                    pass
                def connect(self):
                    return self
                def close(self):
                    pass
                def execute(self, sql, **kwargs):
                    if sql.startswith('TRAIN'):
                        Fake.submissions += 1; kwargs['on_submitted']('submitted')
                        raise Indeterminate('Connection lost after submission')
                    status = 'succeeded' if Fake.complete else 'created'
                    return SQLResult(['col_name','data_type'], [['Status:',status],['Checkpoint Type:','origin']])
            with self.assertRaises(Indeterminate):
                Runner(root, client_factory=Fake).run()
            with self.assertRaises(Indeterminate):
                Runner(root, client_factory=Fake).run()
            self.assertEqual(Fake.submissions, 1)
            Fake.complete=True
            result=Runner(root, client_factory=Fake).run()
            self.assertEqual(result['status'],'succeeded')
            self.assertFalse(result['formal_transport'])
            self.assertEqual(Fake.submissions, 1)
            reconciled = result['steps']['train']
            self.assertEqual(digest(root / reconciled['artifact']), reconciled['sha256'])


class RuntimeAuditTest(unittest.TestCase):
    pipeline = '''train_config { num_epochs: 1 }
        data_config { batch_size: 64 num_workers: 1 }
        feature_configs { id_feature { feature_name: "user_id" embedding_dim: 8 hash_bucket_size: 128 } }
        feature_configs { id_feature { feature_name: "video_id" embedding_dim: 8 hash_bucket_size: 128 } }
        model_config { feature_groups { group_name: "user" feature_names: "user_id" }
        feature_groups { group_name: "item" feature_names: "video_id" }
        dssm { output_dim: 16 in_batch_negative: true user_tower { input: "user" mlp { hidden_units: 32 hidden_units: 16 } }
        item_tower { input: "item" mlp { hidden_units: 32 hidden_units: 16 } } } }'''
    features = [{'name':'user_id','type':'STRING','role':'user'}, {'name':'video_id','type':'STRING','role':'item'}]
    params = {'version':'0.1.15-cpu','batch_size':64,'num_workers':1,'embedding_dim':8,
              'output_dim':16,'user_hidden_units':'32,16','column.user_id.bucket_size':128}
    job = {'image':'sqlrec/tzrec:0.1.15-cpu','seed_env':{'TORCH_MANUAL_SEED':'123','NUMPY_MANUAL_SEED':'123'}}

    def auditor(self, root):
        return RuntimeAudit(root, {'plan_id': 'test_plan', 'config': {'execution': {
            'resources': {'NAMESPACE': 'sqlrec'}, 'runtime_audit': {'job_timeout': 4, 'poll_interval': 1}}}})

    def test_job_poll_waits_for_public_job_without_resubmitting_training(self):
        with tempfile.TemporaryDirectory() as root:
            audit = self.auditor(root)
            with patch.object(audit, '_capture_job_once', side_effect=['job_not_observed', None]) as read, \
                    patch('benchmark.model_quality.user_workflow.runtime_audit.time.monotonic', side_effect=[0, 0, 1, 1, 2]), \
                    patch('benchmark.model_quality.user_workflow.runtime_audit.time.sleep') as sleep:
                audit.capture_job('model')
            self.assertEqual(read.call_count, 2)
            sleep.assert_called_once_with(1)
            self.assertFalse((Path(root)/'execution/runtime/model_job_capture.json').exists())

    def test_job_poll_deadline_records_missing_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            audit = self.auditor(root)
            with patch.object(audit, '_capture_job_once', return_value='job_not_observed') as read, \
                    patch('benchmark.model_quality.user_workflow.runtime_audit.time.monotonic', side_effect=[0, 0, 4]):
                audit.capture_job('model')
            self.assertEqual(read.call_count, 1)
            proof = read_json(Path(root)/'execution/runtime/model_job_capture.json')
            self.assertEqual((proof['status'], proof['reason']), ('missing', 'job_not_observed'))
            self.assertFalse((Path(root)/'execution/runtime/model_job.json').exists())

    def test_public_audit_read_errors_do_not_fabricate_job_evidence(self):
        for error, reason in ((OSError('unavailable'), 'kubernetes_read_failed'),
                              (ValueError('malformed response'), 'invalid_kubernetes_evidence')):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as root:
                audit = self.auditor(root)
                with patch.object(audit, '_capture_job_once', side_effect=error):
                    audit.capture_job('model')
                proof = read_json(Path(root)/'execution/runtime/model_job_capture.json')
                self.assertEqual((proof['status'], proof['reason']), ('missing', reason))
                self.assertFalse((Path(root)/'execution/runtime/model_job.json').exists())

    def test_job_and_configmap_share_the_read_timeout_budget(self):
        with tempfile.TemporaryDirectory() as root:
            audit = self.auditor(root)
            job = {'metadata': {'name': 'model-v1-job', 'uid': 'job_uid'}, 'spec': {'template': {'spec': {
                'containers': [{'image': self.job['image'], 'env': [{'name': k, 'value': v} for k, v in self.job['seed_env'].items()]}],
                'volumes': [{'configMap': {'name': 'submitted'}}]}}}}
            responses = [SimpleNamespace(returncode=0, stdout=json.dumps(job)),
                         SimpleNamespace(returncode=0, stdout=json.dumps({'data': {'pipeline.config': self.pipeline}}))]
            with patch('benchmark.model_quality.user_workflow.runtime_audit.subprocess.run', side_effect=responses) as read, \
                    patch('benchmark.model_quality.user_workflow.runtime_audit.time.monotonic', side_effect=[10, 13]):
                self.assertIsNone(audit._capture_job_once('model', 5))
            self.assertEqual([c.kwargs['timeout'] for c in read.call_args_list], [5, 2])
            proof = read_json(Path(root)/'execution/runtime/model_job.json')
            self.assertEqual(proof['seed_env'], self.job['seed_env'])
            self.assertEqual(digest(proof['pipeline_artifact']), proof['pipeline_sha256'])

    def test_actual_configuration_and_training_seed_verified(self):
        result = compare_dssm(self.pipeline,self.params,self.features,123,self.job)
        self.assertEqual(result['status'],'verified')
        self.assertTrue(result['seed_verified'])

    def test_sql_parameter_storage_does_not_hide_effective_mismatch(self):
        result = compare_dssm(self.pipeline.replace('batch_size: 64','batch_size: 8192'),self.params,self.features,123,self.job)
        self.assertEqual(result['status'],'mismatch')
        self.assertEqual(result['mismatches'][0]['parameter'],'batch_size')

    def test_missing_training_job_seed_evidence_does_not_pass(self):
        self.assertEqual(compare_dssm(self.pipeline,self.params,self.features,123)['status'],'mismatch')

    def test_unverified_public_parameters_do_not_pass(self):
        params = dict(self.params,sparse_lr=0.001)
        result = compare_dssm(self.pipeline,params,self.features,123,self.job)
        self.assertEqual(result['status'],'mismatch')
        self.assertIn('sparse_lr', [r['parameter'] for r in result['mismatches']])


class CompactConfigurationTest(unittest.TestCase):
    def test_all_ten_yaml_configs_resolve_without_hidden_model_defaults(self):
        import yaml
        from benchmark.model_quality.user_workflow.configuration import resolve_config
        folder = Path(__file__).resolve().parents[1] / 'configs'
        env = {'NODE_IP': '127.0.0.1', 'BENCH_STORAGE_ROOT': 'jfs://shared/bench',
               'BENCH_MODEL_BASE_URI': 'jfs://shared/models', 'NAMESPACE': 'review'}
        configs = sorted(folder.glob('*.yaml'))
        self.assertEqual(len(configs), 10)
        for path in configs:
            with self.subTest(config=path.name):
                value = yaml.safe_load(path.read_text())
                resolved = resolve_config(value, path, 'smoke', 'review_01', env)
                self.assertEqual(resolved['execution']['resources']['NAMESPACE'], 'review')
                self.assertEqual(resolved['experiment']['run_id'], 'review_01')
                for submitted, frozen in zip(value['models'], resolved['models']):
                    self.assertEqual({k:v for k,v in frozen['params'].items() if k != 'version'}, submitted['params'])
                for task in resolved['tasks']:
                    self.assertIn(task['data_type'], ('INT', 'BIGINT', 'FLOAT', 'DOUBLE'))


class TreeRuntimeAuditTest(unittest.TestCase):
    def test_each_tree_family_verifies_actual_native_parameters_and_seed(self):
        for family, native_key in [('gbdt.catboost', 'iterations'), ('gbdt.lightgbm', 'num_iterations'),
                                   ('gbdt.xgboost', 'num_iterations')]:
            with self.subTest(family=family):
                features = [{'name': 'count', 'type': 'FLOAT'}]
                tasks = [{'id': 'click', 'column': 'label', 'type': 'binary'}]
                recipe = {'family': family, 'params': {'version': 'review-cpu', 'num_iterations': 10}}
                pipeline = {'model_type': family.split('.')[1], 'feature_columns': ['count'],
                            'label_columns': 'label', 'categorical_features': [],
                            'params': {'random_seed': 123, native_key: 10}}
                job = {'image': 'sqlrec/gbdt:review-cpu'}
                self.assertEqual(compare_pipeline(json.dumps(pipeline), recipe, features, tasks, 123, job)['status'], 'verified')
                pipeline['params'][native_key] = 99
                self.assertEqual(compare_pipeline(json.dumps(pipeline), recipe, features, tasks, 123, job)['status'], 'mismatch')


class ResourceLifecycleTest(unittest.TestCase):
    """Fakes exercise scheduling order and ownership, never a real cluster."""
    def setUp(self):
        from benchmark.model_quality.user_workflow.infrastructure import WorkloadAdmin
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        self.events = []; self.services = {}; self.physical = {}
        self.fail_create = None; self.fail_score = False; self.fail_drop = False
        self.block_barrier = False; self.interrupt_score = False
        owner = self
        class SQL:
            def __init__(self, config): pass
            def connect(self): return self
            def close(self): pass
            def execute(self, sql, **kwargs):
                if sql == 'SHOW SERVICES':
                    return SQLResult(['name'], [[n] for n in owner.services])
                if sql.startswith('SHOW '):
                    return SQLResult([], [])
                if sql.startswith('CREATE SERVICE'):
                    name = sql.split('`')[1]
                    if name == owner.fail_create:
                        raise SQLFailed('Rejected before creating the second tower')
                    binding = owner.bindings[name]
                    owner.services[name] = binding
                    owner.physical[name] = {'Deployment/' + binding['deployment']: {'uid': 'uid-' + name, 'owners': []}}
                    owner.events.append('create:' + name)
                elif sql.startswith('DESCRIBE FORMATTED SERVICE'):
                    name = sql.split('`')[1]; b = owner.services[name]
                    return SQLResult(['col_name', 'data_type'], [
                        ['Model Name:', b['model']], ['Checkpoint Name:', b['checkpoint']],
                        ['URL:', 'http://' + b['deployment'] + '.sqlrec.svc.cluster.local:80/predict']])
                elif sql.startswith('DROP SERVICE'):
                    name = sql.split('`')[1]
                    owner.events.append('drop:' + name); owner.services.pop(name)
                    if owner.fail_drop:
                        owner.fail_drop = False
                        raise Indeterminate('Lost response after DROP')
                elif sql.startswith('SCORE'):
                    owner.events.append(sql)
                    if owner.interrupt_score:
                        raise KeyboardInterrupt()
                    if owner.fail_score:
                        raise ValueError('Prediction coverage failed')
                return SQLResult([], [])
        class Workloads:
            def __init__(self, execution): pass
            def get(self, *args, **kwargs): return None
            def snapshot(self, binding): return owner.physical.get(binding['name'], {})
            verify_owners = staticmethod(WorkloadAdmin.verify_owners)
            def wait_absent(self, binding, owned):
                WorkloadAdmin.verify_owners(self.snapshot(binding), owned)
                if owner.block_barrier:
                    raise Indeterminate('Pod still terminating')
                owner.events.append('released:' + binding['name'])
                owner.physical.pop(binding['name'], None)
                return {'source': 'public_kubernetes_readonly', 'absent': True}
            def jobs_quiet(self, jobs): return []
        self.client_factory, self.workload_factory = SQL, Workloads

    def tearDown(self):
        self.tmp.cleanup()

    def plan(self, towers=('scalar',)):
        steps, files, resources = [], {}, {'models': {}}
        self.bindings = {}
        for model in ('model_a', 'model_b'):
            resources['models'][model] = {'services': [], 'jobs': []}
            for tower in towers:
                name = model + '_' + tower; checkpoint = 'v1_export' + ('/' + tower if tower != 'scalar' else '')
                binding = {'name': name, 'model': model, 'checkpoint': checkpoint,
                           'namespace': 'sqlrec', 'deployment': kubernetes_name(name), 'step_id': name + '_service'}
                self.bindings[name] = binding; resources['models'][model]['services'].append(binding)
                path = name + '.sql'; (self.root / path).write_text('CREATE SERVICE `' + name + '`;')
                files[path] = digest(self.root / path)
                steps.append(SQLStep(binding['step_id'], 'sqlrec', [path], 'verify',
                    checks=[{'kind': 'service', 'name': name, 'model': model, 'checkpoint': checkpoint}],
                    kind='service', model_id=model, result_roles=['sql']))
            path = model + '_score.sql'; (self.root / path).write_text('SCORE ' + model)
            files[path] = digest(self.root / path)
            steps.append(SQLStep(model + '_score', 'sqlrec', [path], kind='sql', model_id=model, result_roles=['sql']))
        plan = WorkflowPlan('isolated', 'fixture', {'execution': {}, 'sql': {'endpoints': {'sqlrec': {}}}},
                            [], [], steps, files, [], format_version=5, resources=resources).to_dict()
        write_json(self.root / 'resolved_plan.json', plan)
        return plan

    def runner(self):
        return Runner(self.root, client_factory=self.client_factory, workload_factory=self.workload_factory)

    def test_release_barrier_precedes_next_model_and_resume_skips_predictions(self):
        self.plan(); self.runner().run()
        self.assertLess(self.events.index('released:model_a_scalar'), self.events.index('create:model_b_scalar'))
        self.assertFalse(self.services); self.assertFalse(self.physical)
        self.assertFalse((self.root / 'results/report.json').exists())
        count = len(self.events)
        self.runner().run()
        self.assertFalse(any(e.startswith(('create:', 'drop:', 'SCORE')) for e in self.events[count:]))
        journal = read_json(self.root / 'execution/journal.json')
        self.assertEqual(journal['models']['model_a']['cleanup_status'], 'released')

    def test_partial_dssm_deploy_releases_created_tower(self):
        self.plan(('user', 'item')); self.fail_create = 'model_a_item'
        with self.assertRaises(SQLFailed): self.runner().run()
        self.assertIn('released:model_a_user', self.events)
        self.assertNotIn('drop:model_a_item', self.events)
        self.assertFalse(any('model_b' in e for e in self.events))
        self.assertFalse(self.services)

    def test_prediction_failure_and_interruption_always_release(self):
        self.plan(); self.fail_score = True
        with self.assertRaises(ValueError): self.runner().run()
        self.assertIn('released:model_a_scalar', self.events)
        self.assertFalse(self.services)
        # A separate immutable run is needed after partial predictions were released.
        with self.assertRaises(Indeterminate): self.runner().run()

    def test_ctrl_c_releases_service(self):
        self.plan(); self.interrupt_score = True
        with self.assertRaises(KeyboardInterrupt): self.runner().run()
        self.assertIn('released:model_a_scalar', self.events)

    def test_interrupted_training_cleanup_accepts_failed_checkpoint_only_when_job_is_quiet(self):
        run = self.root / 'run'; plan = build(fixture(self.root), run)
        train = next(s for s in plan['steps'] if s['kind'] == 'train')
        class SQL:
            status = 'failed'
            def __init__(self, config): pass
            def connect(self): return self
            def close(self): pass
            def execute(self, sql, **kwargs):
                if sql.startswith('DESCRIBE FORMATTED MODEL'):
                    return SQLResult(['key', 'value'], [['Status:', SQL.status], ['Checkpoint Type:', 'origin']])
                return SQLResult([], [])
        class Workloads:
            quiet = True; job = None
            def __init__(self, execution): pass
            def get(self, *args): return self.job
            def jobs_quiet(self, jobs):
                if not self.quiet:
                    raise Indeterminate('Training Pod remains active')
                return [{'quiet': True, **job} for job in jobs]
        runner = Runner(run, client_factory=SQL, workload_factory=Workloads)
        runner.journal['steps'][train['id']] = {'status': 'indeterminate'}; runner.save()
        with self.assertRaises(Indeterminate): runner.checks(train)
        self.assertEqual(runner.cleanup_services()['status'], 'succeeded')
        self.assertEqual(read_json(run / 'execution/journal.json')['steps'][train['id']]['status'], 'indeterminate')
        for status, quiet in [('running', True), ('failed', False)]:
            SQL.status, Workloads.quiet = status, quiet
            with self.subTest(status=status, quiet=quiet), self.assertRaises(Indeterminate):
                Runner(run, client_factory=SQL, workload_factory=Workloads).cleanup_services()
        job_name = plan['resources']['models'][train['model_id']]['jobs'][0]['name']
        write_json(run / f'execution/runtime/{train["model_id"]}_job.json', {'job': job_name, 'uid': 'original-job'})
        SQL.status, Workloads.quiet = 'created', True
        Workloads.job = {'metadata': {'uid': 'original-job'},
                         'status': {'conditions': [{'type': 'Failed', 'status': 'True'}]}}
        self.assertEqual(Runner(run, client_factory=SQL, workload_factory=Workloads).cleanup_services()['status'], 'succeeded')
        Workloads.job['metadata']['uid'] = 'replacement-job'
        with self.assertRaises(Indeterminate):
            Runner(run, client_factory=SQL, workload_factory=Workloads).cleanup_services()

    def test_failed_physical_barrier_blocks_next_model(self):
        self.plan(); self.block_barrier = True
        with self.assertRaises(Indeterminate): self.runner().run()
        self.assertFalse(any('model_b' in e for e in self.events))
        self.assertEqual(read_json(self.root / 'execution/journal.json')['status'], 'cleanup_failed')
        self.block_barrier = False
        self.runner().cleanup_services()
        self.assertEqual(self.events.count('drop:model_a_scalar'), 1)

    def test_lost_drop_response_reconciles_by_absence_without_another_drop(self):
        self.plan(); self.fail_drop = True
        with self.assertRaises(Indeterminate): self.runner().run()
        self.runner().cleanup_services(); self.runner().cleanup_services()
        self.assertEqual(self.events.count('drop:model_a_scalar'), 1)

    def test_reused_uid_is_not_adopted_or_deleted(self):
        self.plan(); self.block_barrier = True
        with self.assertRaises(Indeterminate): self.runner().run()
        b = self.bindings['model_a_scalar']
        self.physical[b['name']]['Deployment/' + b['deployment']]['uid'] = 'foreign-uid'
        self.block_barrier = False
        with self.assertRaises(Indeterminate): self.runner().cleanup_services()
        self.assertEqual(self.events.count('drop:model_a_scalar'), 1)

    def test_recovery_requires_ready_deployment_not_only_service_metadata(self):
        plan = self.plan(); name = 'model_a_scalar'
        self.services[name] = self.bindings[name]
        runner = self.runner()
        step = next(s for s in plan['steps'] if s['id'] == self.bindings[name]['step_id'])
        try:
            with self.assertRaisesRegex(Indeterminate, 'deployment completion'):
                runner.checks(step, completion=True)
            with patch.object(runner.workloads, 'get', return_value={'spec': {'replicas': 1}, 'status': {'availableReplicas': 1}}):
                self.assertTrue(runner.checks(step, completion=True))
        finally:
            runner.close()

    def test_suite_summary_distinguishes_cleanup_failure_and_not_executed(self):
        self.plan(); self.block_barrier = True
        with self.assertRaises(Indeterminate): self.runner().run()
        later = self.root / 'later'
        summary = summarize_runs(self.root / 'suite', [self.root, later], [self.root], [later])
        self.assertEqual([r['status'] for r in summary['runs']], ['cleanup_failed', 'not_executed'])
        self.assertEqual(summary['not_executed_runs'], 1)

    def test_completed_predictions_do_not_hide_a_later_blocked_model(self):
        plan = self.plan(); self.runner().run()
        path = self.root / 'execution/journal.json'
        journal = read_json(path)
        journal['models']['model_b'].update(cleanup_status='blocked')
        journal['resource_status'] = 'blocked'
        write_json(path, journal)
        first_score = next(s['id'] for s in plan['steps'] if s['model_id'] == 'model_a' and s['kind'] == 'sql')
        with self.assertRaises(Indeterminate): self.runner().run(through=first_score)
        self.assertEqual(read_json(path)['resource_status'], 'blocked')
        self.runner().cleanup_services()
        self.assertEqual(read_json(path)['resource_status'], 'succeeded')

    def test_resource_manifest_cannot_omit_a_service(self):
        plan = self.plan()
        plan['resources']['models']['model_a']['services'] = []
        plan.pop('plan_id'); plan['plan_id'] = stable_id(plan)
        write_json(self.root / 'resolved_plan.json', plan)
        with self.assertRaisesRegex(ValueError, 'Incomplete service'):
            verify_plan(self.root)


class WorkloadBarrierTest(unittest.TestCase):
    def test_active_training_pods_block_even_a_completed_job(self):
        from benchmark.model_quality.user_workflow.infrastructure import WorkloadAdmin
        admin = WorkloadAdmin({})
        job = {'status': {'conditions': [{'type': 'Complete', 'status': 'True'}]}}
        with patch.object(admin, 'get', side_effect=[job, {'items': [{'status': {'phase': 'Running'}}]}]):
            with self.assertRaises(Indeterminate):
                admin.jobs_quiet([{'name': 'train-job', 'namespace': 'sqlrec'}])
        with patch.object(admin, 'get', side_effect=[job, {'items': [{'status': {'phase': 'Succeeded'}}]}]):
            self.assertTrue(admin.jobs_quiet([{'name': 'train-job', 'namespace': 'sqlrec'}])[0]['quiet'])

    def test_deletion_waits_for_pods_after_deployment_disappears(self):
        from benchmark.model_quality.user_workflow.infrastructure import WorkloadAdmin
        admin = WorkloadAdmin({})
        owned = {'Deployment/d': {'uid': 'd', 'owners': []}, 'ReplicaSet/r': {'uid': 'r', 'owners': ['d']},
                 'Pod/p': {'uid': 'p', 'owners': ['r']}}
        with patch.object(admin, 'snapshot', side_effect=[{'Pod/p': owned['Pod/p']}, {}]), patch('time.sleep') as sleep:
            self.assertTrue(admin.wait_absent({}, owned)['absent'])
            sleep.assert_called_once()

    def test_foreign_controller_or_uid_is_rejected(self):
        from benchmark.model_quality.user_workflow.infrastructure import WorkloadAdmin
        owned = {'Deployment/d': {'uid': 'd', 'owners': []}}
        for snapshot in ({'Deployment/d': {'uid': 'new-d', 'owners': []}},
                         {'Pod/foreign': {'uid': 'p', 'owners': ['some-other-controller']}}):
            with self.assertRaises(Indeterminate): WorkloadAdmin.verify_owners(snapshot, owned)


if __name__ == '__main__':
    unittest.main()
