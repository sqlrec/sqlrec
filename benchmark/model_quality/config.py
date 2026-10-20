"""Fixed benchmark choices. Change model parameters and budgets here."""
from dataclasses import dataclass
from pathlib import Path
import os
import xml.etree.ElementTree as ET

import yaml

from .common import digest

PACKAGE = Path(__file__).parent
DATASETS = ('movielens100k', 'kuairand', 'kuairec', 'criteo1m')
SEED = 123
EVAL_ROWS = 20_000
EVAL_QUERIES = 1_000
API_BATCH = 512  # Prediction request rows; independent of training batch_size.
ITEM_BATCH = 256  # Item embeddings per catalog-loading request.
RECALL_WORKERS = 4
K = 50
TREE_MODELS = ('catboost', 'lightgbm', 'xgboost')
MODELS = ('dssm', 'wide_and_deep', 'deepfm', 'rocket_launching', *TREE_MODELS, 'mmoe')
NEURAL_PARAMS = dict(embedding_dim=8, batch_size=64, num_workers=1, num_epochs=1)
MODEL_PARAMS = {
    'dssm': dict(user_hidden_units='32,16', item_hidden_units='32,16', output_dim=16,
                 negative_sampling='pool', negative_sample_count=256,
                 negative_pool_size=4096, item_id_column='video_id'),
    'wide_and_deep': dict(hidden_units='64,32'),
    'deepfm': dict(hidden_units='64,32'),
    'rocket_launching': dict(booster_hidden_units='64,32', light_hidden_units='32,16'),
    'mmoe': dict(num_expert=4, expert_hidden_units='64,32', task_hidden_units='16'),
    'catboost': dict(objective='binary', num_iterations=100, max_depth=6, learning_rate=.05),
    'lightgbm': dict(objective='binary', num_iterations=100, num_leaves=31, learning_rate=.05),
    'xgboost': dict(objective='binary', num_iterations=100, max_depth=6, learning_rate=.05),
}
# Fixed dataset budgets and validation-selected overrides; evaluation never searches parameters.
DATASET_MODEL_PARAMS = {
    **{('criteo1m', model): {
        'batch_size': 256,
        **{f'column.i{i}.normalizer': 'method=log10,threshold=1,default=0' for i in range(1, 14)},
    } for model in ('wide_and_deep', 'deepfm', 'rocket_launching')},
    ('kuairand', 'dssm'): dict(embedding_dim=16, batch_size=256, num_epochs=6, sparse_lr=.01,
                             user_hidden_units='64,32', item_hidden_units='64,32', output_dim=32),
    ('movielens100k', 'dssm'): dict(embedding_dim=16, batch_size=256, num_epochs=3, sparse_lr=.01,
                                 user_hidden_units='64,32', item_hidden_units='64,32', output_dim=32),
    ('movielens100k', 'deepfm'): dict(embedding_dim=16, batch_size=256, num_epochs=6, sparse_lr=.03),
    ('movielens100k', 'wide_and_deep'): dict(embedding_dim=16, batch_size=256, num_epochs=6, sparse_lr=.03),
    ('movielens100k', 'rocket_launching'): dict(embedding_dim=16, batch_size=256, num_epochs=6, sparse_lr=.03),
    ('movielens100k', 'lightgbm'): dict(num_iterations=300, max_depth=4, num_leaves=15,
                                    min_data_in_leaf=100, feature_fraction=1., bagging_fraction=1.,
                                    l2_regularization=5.),
    ('kuairec', 'wide_and_deep'): {
        'embedding_dim': 16, 'batch_size': 256, 'sparse_lr': .01,
        'column.duration_s.normalizer': 'method=log10,threshold=1,default=0',
    },
    ('kuairec', 'rocket_launching'): {
        'embedding_dim': 16, 'batch_size': 256, 'sparse_lr': .01,
        'column.duration_s.normalizer': 'method=log10,threshold=1,default=0',
    },
    ('kuairec', 'deepfm'): {
        'embedding_dim': 16, 'batch_size': 256, 'sparse_lr': .03,
        'column.duration_s.normalizer': 'method=log10,threshold=1,default=0',
    },
    ('kuairand', 'rocket_launching'): {
        'batch_size': 256, 'share_hidden_units': '64',
        'booster_hidden_units': '32,16', 'light_hidden_units': '16',
        'column.duration_s.normalizer': 'method=log10,threshold=1,default=0',
    },
}


# Only validation-selected models use the two smoothed history rates.
POSITIVE_RATE_MODELS = {
    ('movielens100k', 'deepfm'), ('movielens100k', 'rocket_launching'),
    ('movielens100k', 'catboost'), ('movielens100k', 'xgboost'),
    ('kuairec', 'wide_and_deep'), ('kuairec', 'deepfm'),
    ('kuairec', 'rocket_launching'), ('kuairec', 'lightgbm'),
}


@dataclass(frozen=True)
class Feature:
    name: str
    type: str
    role: str = 'context'


@dataclass(frozen=True)
class Task:
    id: str
    column: str


def read_dataset_definition(path):
    path = Path(path)
    value = yaml.safe_load(path.read_text())
    if value['id'] not in DATASETS:
        raise ValueError('Unknown bundled dataset')
    return value, {'dataset_config_sha256': digest(path)}


def dataset_definition(name):
    if name not in DATASETS:
        raise ValueError(f'Unknown dataset: {name}')
    return read_dataset_definition(PACKAGE / 'configs/datasets' / f'{name}.yaml')[0]


def model_view(model, definition):
    try:
        return definition['models'][model]
    except KeyError:
        raise ValueError(f"{model} is unavailable on {definition['id']}") from None


def applicable_models(dataset, model):
    supported = dataset_definition(dataset)['models']
    if model == 'all':
        return [name for name in MODELS if name in supported]
    if model not in supported:
        raise ValueError(f'{model} is unavailable on {dataset}')
    return [model]


def view_fields(definition, model):
    view = definition['views'][model_view(model, definition)]
    use_rates = (definition['id'], model) in POSITIVE_RATE_MODELS
    features = [Feature(name, **definition['columns'][name]) for name in view['features']
                if use_rates or name not in ('user_positive_rate', 'item_positive_rate')]
    tasks = [Task(name, definition['tasks'][name]['column']) for name in view['tasks']]
    if {f.name for f in features} & ({t.column for t in tasks} | {'row_id', 'query_id', 'feature_id', 'source'}):
        raise ValueError('Labels and audit identities cannot be model features')
    return features, tasks


def model_parameters(model, features, tasks, train, items, environment, dataset):
    family = ('gbdt.' if model in TREE_MODELS else 'tzrec.') + model
    params = {'model': family, 'label_columns': ','.join(t.column for t in tasks), 'random_seed': SEED}
    if model not in TREE_MODELS:
        params.update(NEURAL_PARAMS)
        # Cover train categories and, for DSSM, the complete served item catalog.
        for feature in features:
            if feature.type not in ('STRING', 'ARRAY<STRING>'):
                continue
            values = train[feature.name]
            if model == 'dssm' and feature.role == 'item':
                import pandas as pd
                values = pd.concat([values, items[feature.name]], ignore_index=True)
            if feature.type.startswith('ARRAY'):
                values = values.explode()
            capacity = max(2, int(values.dropna().nunique()) * 20)
            params[f'column.{feature.name}.bucket_size'] = min(1 << (capacity - 1).bit_length(), 4_194_304)
    params.update(MODEL_PARAMS[model])
    params.update(DATASET_MODEL_PARAMS.get((dataset, model), {}))
    params['version'] = environment.versions['gbdt' if model in TREE_MODELS else 'tzrec']
    if model == 'dssm':
        params['user_features'] = ','.join(f.name for f in features if f.role == 'user')
        params['item_features'] = ','.join(f.name for f in features if f.role == 'item')
    if model == 'mmoe':
        params.update({f'task.{t.column}.type': 'binary' for t in tasks})
    return params


@dataclass
class Environment:
    endpoints: dict
    storage: str
    train_resources: dict
    service_resources: dict
    versions: dict
    hadoop: str
    kubectl: str


def environment():
    host = os.environ.get('NODE_IP')
    if not host:
        raise ValueError('Source deploy/env.sh first (NODE_IP is missing)')
    fs = os.environ.get('BENCH_FS_URI', '').rstrip('/')
    storage = os.environ.get('BENCH_STORAGE_ROOT', '')
    if not storage and not fs:
        for base, suffix in [('HADOOP_CONF_DIR', 'core-site.xml'), ('HADOOP_HOME', 'etc/hadoop/core-site.xml')]:
            path = Path(os.environ.get(base, '/nonexistent')) / suffix
            if path.is_file():
                for prop in ET.parse(path).getroot().findall('property'):
                    if prop.findtext('name') == 'fs.defaultFS':
                        fs = (prop.findtext('value') or '').rstrip('/')
    storage = storage or fs + '/model_quality'
    if '://' not in storage or storage.startswith('file:'):
        raise ValueError('Set BENCH_STORAGE_ROOT or BENCH_FS_URI to shared storage')
    address = f'[{host}]' if ':' in host and not host.startswith('[') else host
    endpoints = {
        'sqlrec': dict(host=host, port=int(os.environ.get('SQLREC_THRIFT_PORT', 30000)), auth='NOSASL'),
        'warehouse': dict(host=host, port=int(os.environ.get('KYUUBI_PORT', 30009)), auth='NONE', username='anonymous'),
        'api': dict(base_url=f'http://{address}:{os.environ.get("SQLREC_REST_PORT", 30001)}'),
        'milvus': dict(url=f'http://{address}:{os.environ.get("MILVUS_PORT", 30022)}', token_env='SQLREC_BENCH_MILVUS_TOKEN'),
    }
    if os.environ.get('AUTH_ENABLED', 'true').lower() == 'true':
        # Environment is serialized to environment.json. Store references only.
        credentials = dict(username_env='AUTH_USERNAME', password_env='AUTH_PASSWORD')
        endpoints['sqlrec'].update(auth='CUSTOM', **credentials)
        endpoints['api'].update(**credentials)
    namespace = os.environ.get('NAMESPACE', 'sqlrec')
    train = dict(NAMESPACE=namespace, pod_cpu_cores=int(os.environ.get('BENCH_TRAIN_CPU', 2)),
                 pod_memory=os.environ.get('BENCH_TRAIN_MEMORY', '4Gi'))
    service = dict(NAMESPACE=namespace, pod_cpu_cores=int(os.environ.get('BENCH_SERVICE_CPU', 1)),
                   pod_memory=os.environ.get('BENCH_SERVICE_MEMORY', '2Gi'))
    if min(train['pod_cpu_cores'], service['pod_cpu_cores']) <= 0:
        raise ValueError('CPU resources must be positive')
    version = os.environ.get('SQLREC_VERSION', '0.1.15') + '-cpu'
    versions = {name: os.environ.get(f'BENCH_{name.upper()}_VERSION', version) for name in ('tzrec', 'gbdt')}
    hadoop = os.environ.get('HADOOP_BIN') or str(Path(os.environ.get('HADOOP_HOME', '')) / 'bin/hadoop')
    return Environment(endpoints, storage.rstrip('/'), train, service, versions,
                       hadoop if os.environ.get('HADOOP_HOME') or os.environ.get('HADOOP_BIN') else 'hadoop',
                       os.environ.get('KUBECTL_BIN', 'kubectl'))
