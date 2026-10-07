"""Automatic setup/cache regressions with temporary files and fake deployments."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from benchmark.model_quality.bootstrap import finalize, stage, write
from benchmark.model_quality.common import arrow_type, digest, read_json, write_json
from benchmark.model_quality.user_workflow.configuration import read_dataset_definition, resolve_config
from benchmark.model_quality.user_workflow.data_setup import prepare_dataset, prepare_suite
from benchmark.model_quality.user_workflow.infrastructure import IndexAdmin

PACKAGE = Path(__file__).resolve().parents[1]
ENV = {'NODE_IP': '127.0.0.1', 'BENCH_STORAGE_ROOT': 'jfs://shared/quality', 'BENCH_MODEL_BASE_URI': 'jfs://shared/models'}


class DataSetupTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(); self.root = Path(self.temporary.name)
        self.config_path = self.root / 'dataset.yaml'
        definition, _ = read_dataset_definition(PACKAGE / 'configs/datasets/movielens100k.yaml')
        definition.update(id='custom_dataset', name='Custom dataset')
        definition['preparation']['adapter'] = 'custom.adapter'
        self.config_path.write_text(yaml.safe_dump(definition))
        self.definition, self.metadata = read_dataset_definition(self.config_path)
        (self.root / 'adapter.py').write_text('# adapter version 1\n')
        self.downloads = 0; self.preparations = 0
        def downloader(dataset, output, config_path):
            self.downloads += 1; output.mkdir(parents=True, exist_ok=True)
            for name in self.definition['source']['files'].values():
                (output / name).write_text('fixture input\n')
            write_json(output / 'download.json', {'spec': self.definition['source'],
                'files': {name: digest(output / name) for name in self.definition['source']['files'].values()}})
        def preparer(raw, output, config_path):
            self.preparations += 1
            definition, metadata = read_dataset_definition(config_path)
            fields = [(name, arrow_type(column['type'])) for name, column in definition['columns'].items()]
            row = {name: [] if column['type'].startswith('ARRAY<') else 'id' if column['type'] == 'STRING'
                   else False if column['type'] == 'BOOLEAN' else 1 for name, column in definition['columns'].items()}
            table = pa.Table.from_pylist([row], schema=pa.schema(fields))
            for name in ('train.parquet', 'valid.parquet', 'test.parquet', 'items.parquet', 'queries_valid.parquet', 'queries_test.parquet'):
                pq.write_table(table, output / name)
            write_json(output / 'manifest.json', {'dataset': definition['name'], 'dataset_definition': definition,
                'candidate_scope': definition['protocol']['candidate_scope'], 'max_users': None,
                'preparation_seed': definition['preparation']['seed'], **metadata,
                'splits': {split: [split + '.parquet'] for split in ('train', 'valid', 'test')},
                'files': {p.name: digest(p) for p in output.glob('*.parquet')}, 'fingerprint': 'fixture'})
        self.adapter = SimpleNamespace(__name__='custom.adapter', __file__=str(self.root / 'adapter.py'), prepare=preparer)
        self.patches = [patch('benchmark.model_quality.user_workflow.data_setup.download', side_effect=downloader),
                        patch('benchmark.model_quality.user_workflow.data_setup.load_adapter', return_value=self.adapter)]
        for mocked in self.patches: mocked.start()

    def tearDown(self):
        for mocked in reversed(self.patches): mocked.stop()
        self.temporary.cleanup()

    def test_empty_cache_prepares_once_and_second_run_reuses(self):
        first = prepare_dataset(self.config_path, self.root / 'data')
        second = prepare_dataset(self.config_path, self.root / 'data')
        self.assertEqual((self.downloads, self.preparations), (1, 1))
        self.assertEqual(first['path'], second['path'])
        self.assertEqual(second['status'], 'reused')
        self.assertIsNone(read_json(Path(first['path']) / 'manifest.json')['max_users'])

    def test_corrupted_cache_is_rebuilt_and_preserved(self):
        first = prepare_dataset(self.config_path, self.root / 'data')
        target = Path(first['path']); (target / 'train.parquet').write_bytes(b'corrupted')
        second = prepare_dataset(self.config_path, self.root / 'data')
        self.assertEqual(second['status'], 'prepared')
        self.assertEqual(self.preparations, 2)
        self.assertTrue(list(target.parent.glob(target.name + '_invalid_*')))

    def test_adapter_change_creates_a_new_version(self):
        first = prepare_dataset(self.config_path, self.root / 'data')
        (self.root / 'adapter.py').write_text('# adapter version 2\n')
        second = prepare_dataset(self.config_path, self.root / 'data')
        self.assertNotEqual(first['path'], second['path'])
        self.assertTrue(Path(first['path']).is_dir())
        self.assertEqual(self.preparations, 2)

    def test_failed_preparation_never_publishes_a_complete_cache(self):
        config = self.root / 'experiment.yaml'
        config.write_text(yaml.safe_dump({'dataset': {'config': 'dataset.yaml', 'view': 'recall'},
            'models': [{'family': 'tzrec.dssm'}]}))
        suite = self.root / 'suite'; write_json(suite / 'setup.json', {'stages': {}})
        with patch.dict(os.environ, {**ENV, 'BENCH_DATA_ROOT': str(self.root / 'data')}, clear=True), \
                patch.object(self.adapter, 'prepare', side_effect=ValueError('invalid raw inputs')):
            with self.assertRaisesRegex(ValueError, 'invalid raw inputs'):
                prepare_suite([config], suite)
        self.assertEqual(read_json(suite / 'prepared_inputs.json')['status'], 'failed')
        self.assertEqual(read_json(suite / 'setup.json')['datasets'][0]['status'], 'failed')
        self.assertFalse(list((self.root / 'data/prepared').rglob('manifest.json')))
        self.assertFalse(list((self.root / 'data/prepared').rglob('.preparing-*')))

    def test_single_seed_is_preserved_and_multiple_seeds_are_rejected(self):
        config = self.root / 'experiment.yaml'
        value = {'dataset': {'config': 'dataset.yaml', 'view': 'recall'},
                 'models': [{'family': 'tzrec.dssm'}], 'execution': {'seed': 42}}
        self.assertEqual(resolve_config(value, config, env=ENV)['execution']['seed'], 42)
        value['execution'] = {'seeds': [42]}
        resolved = resolve_config(value, config, env=ENV)
        self.assertEqual(resolved['execution']['seed'], 42)
        self.assertNotIn('seeds', resolved['execution'])
        for execution in ({'seeds': [42, 123]}, {'seed': 42, 'seeds': [42]}):
            value['execution'] = execution
            with self.assertRaisesRegex(ValueError, 'Only one execution seed'):
                resolve_config(value, config, env=ENV)

    def test_duplicate_dataset_configs_prepare_once_and_bind_build_paths(self):
        configs = []
        for name in ('first', 'second'):
            path = self.root / (name + '.yaml')
            path.write_text(yaml.safe_dump({'dataset': {'config': 'dataset.yaml', 'view': 'recall'},
                'models': [{'family': 'tzrec.dssm', 'params': {'output_dim': 8}}]}))
            configs.append(path)
        suite = self.root / 'suite'; write_json(suite / 'setup.json', {'stages': {}})
        with patch.dict(os.environ, {**ENV, 'BENCH_DATA_ROOT': str(self.root / 'data')}, clear=True):
            inputs = prepare_suite(configs, suite)
            resolved = resolve_config(yaml.safe_load(configs[0].read_text()), configs[0], prepared_inputs=inputs)
            self.assertEqual(resolved['execution']['seed'], 123)
            config_key = str(configs[0].resolve())
            self.assertEqual(resolved['dataset']['path'], inputs['configs'][config_key]['path'])
            full = resolve_config(yaml.safe_load(configs[0].read_text()), configs[0], profile='full', prepared_inputs=inputs)
            self.assertNotIn('max_train_users', full['dataset'])
            value = yaml.safe_load(configs[0].read_text()); value['models'][0]['params']['output_dim'] = 16
            configs[0].write_text(yaml.safe_dump(value))
            again = prepare_suite(configs, suite)
        self.assertEqual(self.preparations, 1)
        self.assertEqual(inputs['configs'][config_key]['path'], again['configs'][config_key]['path'])
        self.assertEqual(len(inputs['datasets']), 1)
        bad = copy.deepcopy(inputs); bad['status'] = 'failed'
        with self.assertRaisesRegex(ValueError, 'did not complete'):
            resolve_config(yaml.safe_load(configs[0].read_text()), configs[0], env=ENV, prepared_inputs=bad)


class SetupReportTest(unittest.TestCase):
    def test_setup_failure_still_reports_not_executed_workflows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write(root / 'setup.json', {'workflow_paths': [str(root / 'model')], 'stages': {}})
            stage(root, 'dependencies', 'failed', 'logs/dependencies.log')
            finalize(root, 1)
            report = read_json(root / 'summary.json')
            self.assertEqual(report['status'], 'failed')
            self.assertEqual(report['not_executed_runs'], 1)
            self.assertTrue((root / 'summary.md').is_file())

    def test_interrupted_workflow_is_distinct_from_not_executed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); first, later = root / 'first', root / 'later'
            write(root / 'setup.json', {'workflow_paths': [str(first), str(later)], 'stages': {}})
            write(first / 'execution/journal.json', {'resource_status': 'blocked', 'models': {}})
            stage(root, 'first', 'running')
            finalize(root, 130)
            report = read_json(root / 'summary.json')
            self.assertEqual([r['status'] for r in report['runs']], ['cleanup_failed', 'not_executed'])
            self.assertEqual(report['setup']['stages']['first']['status'], 'interrupted')

    def test_milvus_can_use_no_token_and_auth_errors_are_actionable(self):
        admin = IndexAdmin({'url': 'http://milvus', 'token_env': 'TEST_MILVUS_TOKEN'})
        response = SimpleNamespace(status_code=200, raise_for_status=lambda: None, json=lambda: {'code': 0, 'data': []})
        with patch.dict(os.environ, {}, clear=True), patch('requests.post', return_value=response) as post:
            self.assertEqual(admin.request('collections/list', {}), [])
            self.assertEqual(post.call_args.kwargs['headers'], {})
        response.status_code = 401
        with patch.dict(os.environ, {}, clear=True), patch('requests.post', return_value=response):
            with self.assertRaisesRegex(ValueError, 'TEST_MILVUS_TOKEN'):
                admin.request('collections/list', {})


class ShellOrchestrationTest(unittest.TestCase):
    def suite(self, directory, fail_cleanup=False, fail_setup=False):
        """Copy the Bash entrypoint into an entirely fake repository."""
        root = Path(directory); script = root / 'benchmark/model_quality/run_tests.sh'
        script.parent.mkdir(parents=True); script.write_text((PACKAGE / 'run_tests.sh').read_text())
        configs = script.parent / 'configs'; configs.mkdir()
        for original in (PACKAGE / 'configs').glob('*.yaml'): (configs / original.name).write_text('{}')
        (root / 'deploy').mkdir(); (root / 'deploy/env.sh').write_text('export FAKE_DEPLOY_ENV=loaded\n')
        python = root / '.venv/bin/python'; python.parent.mkdir(parents=True)
        python.write_text('#!' + sys.executable + '\n' + textwrap.dedent('''
            import json, os, pathlib, sys
            args = sys.argv[1:]
            def option(name):
                return args[args.index(name) + 1] if name in args else None
            log = pathlib.Path(os.environ['FAKE_EVENT_LOG'])
            with log.open('a') as stream: stream.write(json.dumps(args) + '\\n')
            if 'unittest' in args: sys.exit(0)
            action = args[2]
            root = pathlib.Path(option('--run')); root.mkdir(parents=True, exist_ok=True)
            if action == 'preflight' and os.environ.get('FAIL_SETUP'): sys.exit(1)
            if action == 'build':
                assert os.environ.get('FAKE_DEPLOY_ENV') == 'loaded'
                assert '--prepared-inputs' in args
                (root / 'resolved_plan.json').write_text('{}')
            if action == 'prepare-data': (root / 'prepared_inputs.json').write_text('{}')
            if action == 'cleanup-services' and os.environ.get('FAIL_CLEANUP'): sys.exit(1)
            if action == 'finalize': (root / 'summary.md').write_text('fake summary')
        '''))
        python.chmod(0o755)
        env = dict(os.environ); env.pop('PYTHON_BIN', None)
        env.pop('FAIL_CLEANUP', None); env.pop('FAIL_SETUP', None)
        env['FAKE_EVENT_LOG'] = str(root / 'events.jsonl')
        if fail_cleanup: env['FAIL_CLEANUP'] = '1'
        if fail_setup: env['FAIL_SETUP'] = '1'
        result = subprocess.run(['bash', str(script)], cwd=directory, env=env, capture_output=True, text=True, timeout=30)
        events = [json.loads(line) for line in (root / 'events.jsonl').read_text().splitlines()]
        return result, events, root

    def test_no_arguments_runs_unit_setup_and_all_ten_configs(self):
        with tempfile.TemporaryDirectory() as directory:
            result, events, root = self.suite(directory)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            actions = [e[2] for e in events]
            self.assertIn('discover', actions)
            self.assertEqual(actions.count('prepare-data'), 1)
            self.assertEqual(actions.count('build'), 10)
            self.assertLess(actions.index('preflight'), actions.index('prepare-data'))
            self.assertLess(actions.index('prepare-data'), actions.index('build'))
            self.assertTrue(list(root.glob('benchmark/model_quality/runs/*/summary.md')))

    def test_cleanup_failure_blocks_later_configs_and_summarizes(self):
        with tempfile.TemporaryDirectory() as directory:
            result, events, _ = self.suite(directory, fail_cleanup=True)
            self.assertNotEqual(result.returncode, 0)
            actions = [e[2] for e in events]
            self.assertEqual(actions.count('build'), 1)
            summary = next(e for e in events if e[2] == 'summarize')
            self.assertEqual(summary.count('--not-executed'), 9)
            self.assertIn('finalize', actions)

    def test_setup_failure_prevents_model_execution_but_finalizes_report(self):
        with tempfile.TemporaryDirectory() as directory:
            result, events, root = self.suite(directory, fail_setup=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn('build', [e[2] for e in events])
            self.assertTrue(list(root.glob('benchmark/model_quality/runs/*/summary.md')))


if __name__ == '__main__':
    unittest.main()
