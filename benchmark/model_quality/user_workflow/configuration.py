"""Expand experiment configs using declared dataset YAML and deployment environment.

Only build reads these defaults. Resume uses the immutable resolved plan.
Credential values are never copied into an experiment or SQL template.
"""
import copy
from hashlib import sha256
import os
import re
from pathlib import Path
import xml.etree.ElementTree as ET

import yaml

from .specs import FAMILIES, identifier, validate_config, validate_dataset_definition


def merge(base, override):
    """Explicit experiment values win; no YAML inheritance or interpolation."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        result[key] = merge(result[key], value) if isinstance(result.get(key), dict) and isinstance(value, dict) else copy.deepcopy(value)
    return result


def read_dataset_definition(path):
    """One reader for download, preparation and experiment configuration."""
    path = Path(path).resolve()
    source = path.read_bytes()
    definition = validate_dataset_definition(yaml.safe_load(source))
    return definition, {'dataset_config': str(path), 'dataset_config_sha256': sha256(source).hexdigest()}


def load_dataset_definition(dataset, experiment_path):
    """Load an explicit YAML reference; no dataset or view registry lives here."""
    if 'config' not in dataset or not isinstance(dataset['config'], str) or not dataset['config']:
        raise ValueError('Compact experiments require dataset.config')
    path = Path(dataset['config'])
    if not path.is_absolute():
        path = Path(experiment_path).resolve().parent / path
    definition, provenance = read_dataset_definition(path)
    view_name = dataset.get('view')
    if not isinstance(view_name, str) or view_name not in definition['views']:
        raise ValueError(f'Choose a declared dataset view: {sorted(definition["views"])}')
    view = definition['views'][view_name]
    features = [{'name': name, **definition['columns'][name]} for name in view['features']]
    tasks, semantics = [], {}
    for name in view['tasks']:
        entry = dict(definition['tasks'][name])
        if 'description' in entry:
            semantics[name] = entry.pop('description')
        tasks.append({'id': name, **entry, 'data_type': definition['columns'][entry['column']]['type']})
    preset = {'dataset': {**definition['dataset'], 'manifest_name': definition['name'], 'task_semantics': semantics},
              'features': features, 'tasks': tasks,
              'protocol': merge(definition['protocol'], view.get('protocol', {})),
              'evaluation': merge(definition['evaluation'], view.get('evaluation', {}))}
    metadata = {'dataset_id': definition['id'], 'feature_view': view_name, **provenance}
    return preset, metadata, view.get('models')


def filesystem_uri(env):
    if env.get('BENCH_FS_URI'):
        uri = env['BENCH_FS_URI'].rstrip('/')
        if uri.startswith('file:') or not re.match(r'^[a-z][a-z0-9+.-]*://\S+', uri):
            raise ValueError('BENCH_FS_URI must identify a shared filesystem')
        return uri
    candidates = []
    for key, suffix in (('HADOOP_CONF_DIR', 'core-site.xml'), ('HADOOP_HOME', 'etc/hadoop/core-site.xml'),
                        ('CONF_DIR', 'hadoop/core-site.xml')):
        if env.get(key):
            candidates.append(Path(env[key]) / suffix)
    for path in candidates:
        if path.is_file():
            for prop in ET.parse(path).getroot().findall('property'):
                if prop.findtext('name') == 'fs.defaultFS':
                    uri = (prop.findtext('value') or '').strip().rstrip('/')
                    if not uri.startswith('file:') and re.match(r'^[a-z][a-z0-9+.-]*://\S+', uri):
                        return uri
    raise ValueError('Set BENCH_FS_URI or BENCH_STORAGE_ROOT/BENCH_MODEL_BASE_URI, or configure fs.defaultFS in Hadoop core-site.xml')


def environment_defaults(env):
    host = env.get('NODE_IP')
    if not host:
        raise ValueError('NODE_IP is missing; source deploy/env.sh before building a compact config')
    # Hadoop commands and local Java paths are reader/upload settings only.
    hadoop = env.get('HADOOP_BIN') or (str(Path(env['HADOOP_HOME']) / 'bin/hadoop') if env.get('HADOOP_HOME') else 'hadoop')
    fs = filesystem_uri(env) if not (env.get('BENCH_STORAGE_ROOT') and env.get('BENCH_MODEL_BASE_URI')) else None
    storage = env.get('BENCH_STORAGE_ROOT') or fs + '/model_quality'
    audit = {'hadoop': [hadoop], 'kubectl': [env.get('KUBECTL_BIN', 'kubectl')],
             'model_base_uri': env.get('BENCH_MODEL_BASE_URI') or fs + '/user/sqlrec/models',
             'job_timeout': 10, 'poll_interval': 1}
    if env.get('JAVA_HOME'):
        audit['java_home'] = env['JAVA_HOME']
    namespace = env.get('NAMESPACE', 'sqlrec')
    execution = {'seed': 123, 'operation_timeout': 3600, 'index_timeout': 120, 'runtime_audit': audit,
                 'resources': {'NAMESPACE': namespace, 'pod_cpu_cores': int(env.get('BENCH_TRAIN_CPU', '2')),
                               'pod_memory': env.get('BENCH_TRAIN_MEMORY', '4Gi')},
                 'service_resources': {'NAMESPACE': namespace, 'pod_cpu_cores': int(env.get('BENCH_SERVICE_CPU', '1')),
                                       'pod_memory': env.get('BENCH_SERVICE_MEMORY', '2Gi')}}
    address = f'[{host}]' if ':' in host and not host.startswith('[') else host
    endpoints = {
        'sqlrec': {'host': host, 'port': int(env.get('SQLREC_THRIFT_PORT', '30000')), 'auth': 'NOSASL', 'database': 'default'},
        'warehouse': {'host': host, 'port': int(env.get('KYUUBI_PORT', '30009')), 'auth': 'NONE', 'username': 'anonymous', 'database': 'default'},
        'api': {'base_url': f'http://{address}:{int(env.get("SQLREC_REST_PORT", "30001"))}'},
        'milvus': {'url': f'http://{address}:{int(env.get("MILVUS_PORT", "30022"))}', 'token_env': 'SQLREC_BENCH_MILVUS_TOKEN'},
    }
    return storage, execution, {'endpoints': endpoints}


def resolve_config(value, config_path, profile=None, run_id=None, env=None, prepared_inputs=None):
    """Return the full schema-v3 contract; legacy complete configs stay loadable."""
    if not isinstance(value, dict):
        raise ValueError('Experiment config must be a mapping')
    env = os.environ if env is None else env
    config = copy.deepcopy(value)
    execution = config.get('execution', {})
    if isinstance(execution, dict) and 'seeds' in execution:
        legacy = execution.pop('seeds')
        if 'seed' in execution or not isinstance(legacy, list) or len(legacy) != 1:
            raise ValueError('Only one execution seed is supported; use seed: 123')
        execution['seed'] = legacy[0]
    compact = isinstance(config.get('dataset'), dict) and 'config' in config['dataset']
    metadata = copy.deepcopy(config.get('experiment', {}))
    if not isinstance(metadata, dict):
        raise ValueError('experiment metadata must be a mapping')
    if compact:
        allowed = {'schema_version', 'execution_profile', 'mode', 'name', 'dataset', 'features', 'tasks',
                   'models', 'protocol', 'serving', 'evaluation', 'execution', 'sql'}
        if set(config) - allowed:
            raise ValueError(f'Unknown experiment fields: {sorted(set(config) - allowed)}')
        ds = config['dataset']
        preset, dataset_metadata, compatible = load_dataset_definition(ds, config_path)
        ds.pop('config'); ds.pop('view', None)
        metadata.update(dataset_metadata)
        dataset_id = dataset_metadata['dataset_id']; view = dataset_metadata['feature_view']
        storage, execution, sql = environment_defaults(env)
        name = identifier(config.get('name', Path(config_path).stem))
        data_root = Path(env.get('BENCH_DATA_ROOT', 'benchmark/model_quality/data'))
        base = {'schema_version': 3, 'execution_profile': 'user_workflow', 'mode': 'fixed_validation', 'name': name,
                'dataset': {'adapter': 'prepared', 'path': str(data_root / dataset_id),
                            'storage_uri': storage.rstrip('/') + '/' + name},
                'serving': {'entrypoint': 'sql_api', 'batch_size': 32,
                                      'candidate_limit': 16384},
                'evaluation': {'split': 'valid', 'ks': []},
                'execution': execution, 'sql': sql}
        config = merge(merge(base, preset), config)
        if not isinstance(config.get('models'), list) or not config['models']:
            raise ValueError('models must be a nonempty list')
        for model in config['models']:
            if not isinstance(model, dict) or not isinstance(model.get('family'), str) or model['family'] not in FAMILIES:
                raise ValueError('Each model must declare a supported family')
            family = model['family']
            if compatible is not None and family not in compatible:
                raise ValueError(f'{family} is incompatible with declared dataset view {view}')
            model.setdefault('id', family.split('.')[1])
            version = env.get('BENCH_TZREC_VERSION' if family.startswith('tzrec.') else 'BENCH_GBDT_VERSION',
                              env.get('SQLREC_VERSION', '0.1.15') + '-cpu')
            params = {'version': version}
            if not isinstance(model.get('params', {}), dict):
                raise ValueError('Model params must be a mapping')
            model['params'] = merge(params, model.get('params', {}))
    profile = profile or ('smoke' if compact else None)
    if profile:
        if profile not in ('smoke', 'full'):
            raise ValueError('Unknown run profile')
        dataset = config['dataset']
        if profile == 'smoke':
            dataset.setdefault('max_train_users', 32)
            dataset.setdefault('max_eval_rows', 256)
            if any(m['family'] == 'tzrec.dssm' for m in config['models']):
                dataset.setdefault('max_eval_queries', 8)
        elif any(dataset.get(k) is not None for k in ('max_train_users', 'max_eval_queries', 'max_eval_rows')):
            raise ValueError('full profile forbids sample limits; remove them from the experiment config')
        metadata['profile'] = profile
    if run_id:
        if not re.fullmatch(r'[a-z0-9_]{1,32}', run_id):
            raise ValueError('run_id must contain 1-32 lowercase letters, digits or underscores')
        config['name'] = identifier(config['name'] + '_' + run_id)
        config['dataset']['storage_uri'] = config['dataset']['storage_uri'].rstrip('/') + '/' + run_id
        metadata['run_id'] = run_id
    if metadata:
        config['experiment'] = metadata
    if prepared_inputs is not None:
        if prepared_inputs.get('format_version') != 1 or prepared_inputs.get('status') != 'succeeded':
            raise ValueError('Suite data preparation did not complete')
        entry = prepared_inputs['configs'].get(str(Path(config_path).resolve()))
        if entry is None or entry['dataset_config_sha256'] != metadata.get('dataset_config_sha256'):
            raise ValueError('Prepared inputs do not match the selected dataset configuration')
        if ds_path := config['dataset'].get('path'):
            if 'config' not in value.get('dataset', {}) or 'path' in value['dataset']:
                if Path(ds_path).resolve() != Path(entry['path']).resolve():
                    raise ValueError('Prepared inputs differ from explicit dataset.path')
        config['dataset']['path'] = entry['path']
    return validate_config(config)
