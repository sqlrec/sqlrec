"""Read-only deployment checks and cached preparation from dataset YAML declarations."""
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import importlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import uuid
from urllib.parse import urlsplit

import pyarrow.parquet as pq
import requests
import yaml

from ..common import arrow_type, digest, read_json, stable_id, write_json
from ..download_data import download
from .configuration import read_dataset_definition, resolve_config
from .datasets import PreparedDatasetAdapter
from .infrastructure import IndexAdmin
from .sql_client import HS2Client


def experiments(paths, profile, run_id):
    return [(Path(path).resolve(), resolve_config(yaml.safe_load(Path(path).read_text()), path, profile, run_id))
            for path in paths]


def preflight(paths, root, profile='smoke', run_id=None):
    """Fail before large downloads; do not deploy, upgrade or create model resources."""
    root = Path(root); setup = read_json(root / 'setup.json'); proofs = []
    seen = set()
    def checked(name, key, action):
        if (name, key) in seen:
            return
        print(f'Checking {name}.', flush=True)
        try:
            result = action()
        except Exception as error:
            proofs.append({'check': name, 'status': 'failed', 'error_type': type(error).__name__})
            setup['checks'] = proofs; write_json(root / 'setup.json', setup)
            raise ValueError(f'Preflight failed: {name}; review logs/preflight.log') from error
        proofs.append({'check': name, 'status': 'succeeded', 'evidence': result})
        setup['checks'] = proofs; write_json(root / 'setup.json', setup); seen.add((name, key))
    for _, config in experiments(paths, profile, run_id):
        for name in ('sqlrec', 'warehouse'):
            endpoint = config['sql']['endpoints'][name]
            def sql_check(endpoint=endpoint):
                client = HS2Client({**endpoint, 'rpc_timeout': min(endpoint.get('rpc_timeout', 120), 30)})
                try:
                    client.connect(); result = client.execute('SELECT 1', timeout=30)
                    if not result.rows or result.rows[0][0] != 1:
                        raise ValueError('Public SQL probe did not return 1')
                    return {'source': result.source, 'connected': True}
                finally:
                    client.close()
            checked(name + ' SQL connection', stable_id(endpoint), sql_check)
        if config['serving']['entrypoint'] == 'sql_api':
            endpoint = config['sql']['endpoints']['api']
            def api_check(endpoint=endpoint):
                headers = {}
                if endpoint.get('authorization_env'):
                    headers['Authorization'] = os.environ[endpoint['authorization_env']]
                response = requests.get(endpoint['base_url'].rstrip('/') + '/metrics', headers=headers, timeout=30)
                response.raise_for_status()
                return {'source': 'public_sqlrec_http', 'http_status': response.status_code}
            checked('SQLRec HTTP connection', stable_id(endpoint), api_check)
        if any(m['family'] == 'tzrec.dssm' for m in config['models']):
            endpoint = config['sql']['endpoints']['milvus']
            checked('Milvus connection/authentication', stable_id(endpoint),
                    lambda endpoint=endpoint: {'source': 'public_milvus_admin_readonly',
                        'collection_count': len(IndexAdmin(endpoint).request('collections/list', {}))})
        execution = config['execution']; audit = execution.get('runtime_audit', {})
        command = audit.get('kubectl', ['kubectl'])
        namespaces = {execution[k]['NAMESPACE'] for k in ('resources', 'service_resources') if k in execution}
        for namespace in sorted(namespaces):
            def cluster_check(namespace=namespace, command=command):
                for kind in ('deployments', 'services', 'jobs', 'configmaps', 'replicasets', 'pods'):
                    verbs = ('get', 'list') if kind in ('replicasets', 'pods') else ('get',)
                    for verb in verbs:
                        result = subprocess.run(command + ['auth', 'can-i', verb, kind, '-n', namespace],
                                                capture_output=True, text=True, timeout=30)
                        if result.returncode or result.stdout.strip() != 'yes':
                            raise ValueError(f'Kubernetes permission missing: {verb} {kind} in {namespace}')
                return {'source': 'public_kubernetes_readonly', 'namespace': namespace, 'read_permissions': True}
            checked('Kubernetes read permissions', stable_id(command, namespace), cluster_check)
        command = ([os.environ['HADOOP_BIN']] if os.environ.get('HADOOP_BIN') else audit.get('hadoop', ['hadoop']))
        uris = [config['dataset']['storage_uri']]
        if audit.get('model_base_uri'):
            uris.append(audit['model_base_uri'])
        for uri in uris:
            parsed = urlsplit(uri); fs_root = f'{parsed.scheme}://{parsed.netloc}/'
            def filesystem_check(command=command, fs_root=fs_root):
                if not shutil.which(command[0]):
                    raise ValueError('Hadoop client unavailable; check deploy/env.sh client paths')
                subprocess.run(command + ['fs', '-ls', fs_root], check=True, capture_output=True, timeout=60)
                return {'source': 'public_hadoop_client', 'filesystem': fs_root, 'readable': True}
            checked('Shared filesystem', stable_id(command, fs_root), filesystem_check)
    return proofs


@contextmanager
def preparation_lock(data_root, identity):
    path = data_root / '.locks' / (identity + '.lock'); path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def valid_raw(raw, definition):
    try:
        receipt = read_json(raw / 'download.json')
        return receipt['spec'] == definition['source'] and all(
            receipt['files'].get(name) == digest(raw / name) for name in definition['source']['files'].values())
    except (OSError, KeyError, ValueError, TypeError):
        return False


def load_adapter(name):
    return importlib.import_module(name)


def validate_prepared(path, definition, metadata, cache_identity=None):
    """Verify provenance, full-cohort semantics, file coverage and SQL types."""
    adapter = PreparedDatasetAdapter({'path': str(path)}); manifest = adapter.manifest
    if (manifest.get('dataset') != definition['name'] or manifest.get('candidate_scope') != definition['protocol']['candidate_scope'] or
        manifest.get('dataset_definition') != definition or manifest.get('dataset_config_sha256') != metadata['dataset_config_sha256'] or
        manifest.get('preparation_seed') != definition['preparation']['seed'] or manifest.get('max_users') is not None):
        raise ValueError('Prepared cache does not match the declared complete dataset')
    if cache_identity is not None and manifest.get('cache_identity') != cache_identity:
        raise ValueError('Prepared cache code/source identity changed')
    required_files = ['train.parquet', 'items.parquet', 'queries_valid.parquet', 'queries_test.parquet']
    for split in ('train', 'valid', 'test'):
        if not manifest['splits'].get(split):
            raise ValueError('Prepared cache has an empty split contract')
        required_files.extend(manifest['splits'][split])
    adapter.require_files(required_files)
    features = {name for name, column in definition['columns'].items() if 'role' in column}
    labels = {task['column'] for task in definition['tasks'].values()}
    for name in manifest['files']:
        if not name.endswith('.parquet'):
            continue
        schema = pq.read_schema(Path(path) / name)
        expected = set()
        if any(name in manifest['splits'][split] for split in ('train', 'valid', 'test')):
            expected = features | labels | {definition['preparation']['row_key']}
        elif name.startswith('queries_'):
            expected = set(definition['preparation']['query_features']) | {definition['dataset']['query_key']}
        elif name == 'items.parquet':
            expected = set(definition['preparation']['item_features']) | {definition['dataset']['item_key']}
        if expected - set(schema.names):
            raise ValueError(f'Prepared cache has missing declared columns: {name}')
        for field in schema:
            column = definition['columns'].get(field.name)
            if not column or field.type != arrow_type(column['type']):
                raise ValueError(f'Prepared cache column type differs from dataset YAML: {field.name}')
    return manifest


def prepare_dataset(definition_path, data_root, explicit_path=None):
    definition, metadata = read_dataset_definition(definition_path)
    if 'source' not in definition or 'preparation' not in definition:
        raise ValueError('Automatic data setup needs dataset source and preparation declarations')
    if explicit_path:
        path = Path(explicit_path).resolve()
        manifest = validate_prepared(path, definition, metadata)
        return {'dataset': definition['id'], 'path': str(path), **metadata,
                'status': 'reused_explicit', 'fingerprint': manifest['fingerprint']}
    data_root = Path(data_root).resolve(); identity = definition['id']
    with preparation_lock(data_root, identity):
        raw = data_root / 'raw' / identity
        if valid_raw(raw, definition):
            print(f'Reusing raw dataset: {identity}.', flush=True)
        else:
            print(f'Downloading/checking raw dataset: {identity}.', flush=True)
            download(None, raw, definition_path)
        adapter = load_adapter(definition['preparation']['adapter'])
        package = Path(__file__).parents[1]
        sources = {adapter.__name__: digest(adapter.__file__),
                   'prepare.py': digest(package / 'prepare.py'), 'common.py': digest(package / 'common.py')}
        raw_files = {name: digest(raw / name) for name in definition['source']['files'].values()}
        cache_identity = stable_id(definition, metadata['dataset_config_sha256'], sources, raw_files)
        target = data_root / 'prepared' / identity / cache_identity
        manifest = None
        if (target / 'manifest.json').exists():
            try:
                manifest = validate_prepared(target, definition, metadata, cache_identity)
            except Exception as error:
                print(f'Invalid prepared cache for {identity}: {type(error).__name__}; rebuilding.', flush=True)
        if manifest is not None:
            status = 'reused'
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix='.preparing-', dir=target.parent))
            try:
                print(f'Preparing complete dataset: {identity}.', flush=True)
                adapter.prepare(raw, temporary, config_path=definition_path)
                manifest = read_json(temporary / 'manifest.json')
                manifest['cache_identity'] = cache_identity; manifest['preparation_sources'] = sources
                write_json(temporary / 'manifest.json', manifest)
                validate_prepared(temporary, definition, metadata, cache_identity)
                if target.exists():
                    # Only a managed invalid version is moved. Historical data is retained.
                    suffix = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S') + '_' + uuid.uuid4().hex[:8]
                    target.rename(target.with_name(target.name + '_invalid_' + suffix))
                temporary.rename(target)
            finally:
                if temporary.exists():
                    shutil.rmtree(temporary)
            status = 'prepared'
        print(f'Prepared dataset {identity}: {status}; {target}', flush=True)
        return {'dataset': identity, 'path': str(target), **metadata, 'status': status,
                'cache_identity': cache_identity, 'fingerprint': manifest['fingerprint']}


def prepare_suite(paths, root, profile='smoke', run_id=None):
    root = Path(root); data_root = Path(os.environ.get('BENCH_DATA_ROOT', Path(__file__).parents[1] / 'data'))
    inputs = {'format_version': 1, 'status': 'running', 'configs': {}, 'datasets': {}}
    write_json(root / 'prepared_inputs.json', inputs)
    prepared = {}
    for path, _ in experiments(paths, profile, run_id):
        value = yaml.safe_load(path.read_text()); dataset = value['dataset']
        if 'config' not in dataset:
            raise ValueError('Automatic suite preparation requires dataset.config referencing a dataset YAML')
        definition_path = Path(dataset['config'])
        if not definition_path.is_absolute():
            definition_path = path.parent / definition_path
        definition_path = definition_path.resolve()
        key = (str(definition_path), dataset.get('path'))
        if key not in prepared:
            try:
                prepared[key] = prepare_dataset(definition_path, data_root, dataset.get('path'))
            except Exception as error:
                inputs['status'] = 'failed'; inputs['error_type'] = type(error).__name__
                write_json(root / 'prepared_inputs.json', inputs)
                setup = read_json(root / 'setup.json')
                setup['datasets'] = list(inputs['datasets'].values()) + [
                    {'dataset_config': str(definition_path), 'status': 'failed', 'error_type': type(error).__name__}]
                write_json(root / 'setup.json', setup)
                raise
            entry = prepared[key]
            inputs['datasets'][stable_id(key)] = entry
        inputs['configs'][str(path)] = prepared[key]
        write_json(root / 'prepared_inputs.json', inputs)
        setup = read_json(root / 'setup.json'); setup['datasets'] = list(inputs['datasets'].values())
        write_json(root / 'setup.json', setup)
    inputs['status'] = 'succeeded'; write_json(root / 'prepared_inputs.json', inputs)
    return inputs
