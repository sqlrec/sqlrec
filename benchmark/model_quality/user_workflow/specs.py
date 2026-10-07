"""Explicit public-workflow contracts and validation, independent of model runtimes."""
from __future__ import annotations

import re
import copy
from pathlib import Path
from urllib.parse import urlsplit
from dataclasses import asdict, dataclass, field
from typing import Any

from ..common import finite, stable_id


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,119}', value):
        raise ValueError(f'Invalid SQL identifier: {value!r}')
    return value


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def model_name(prefix, recipe, seed):
    # Reserve room for native export Jobs, ConfigMaps and headless Services.
    value = f'{prefix}_{recipe}_s{seed}'
    canonical = re.sub(r'_+', '_', value).strip('_')
    if len(value) <= 40 and canonical == value:
        return identifier(value)
    return identifier(canonical[:27].rstrip('_') + '_' + stable_id(value)[:12])


def kubernetes_name(value):
    """Match the public runtime's normalization, including its 63-byte limit."""
    value = re.sub(r'[^a-z0-9.-]', '-', value.lower()).strip('.-')
    return re.sub(r'[.-]+', '-', value)[:63].rstrip('.-')


def sql_type(value):
    if not isinstance(value, str):
        raise ValueError('Feature type must be a SQL type string')
    value = value.upper().replace(' ', '')
    aliases = {'VARCHAR': 'STRING', 'INTEGER': 'INT'}
    value = aliases.get(value, value)
    if value not in {'STRING', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'BOOLEAN',
                     'ARRAY<STRING>', 'ARRAY<FLOAT>', 'ARRAY<DOUBLE>', 'ARRAY<BIGINT>'}:
        raise ValueError(f'Unsupported interchange type: {value}')
    return value


@dataclass(frozen=True)
class Feature:
    name: str
    type: str
    role: str = 'context'
    source: str | None = None

    def __post_init__(self):
        identifier(self.name)
        object.__setattr__(self, 'type', sql_type(self.type))
        if self.role not in ('user', 'item', 'context'):
            raise ValueError('Feature role must be user, item or context')
        if self.source is not None and (not isinstance(self.source, str) or not self.source):
            raise ValueError('Feature source must be a nonempty dataset column name')


@dataclass(frozen=True)
class Task:
    id: str
    column: str
    type: str = 'binary'
    data_type: str | None = None

    def __post_init__(self):
        identifier(self.id); identifier(self.column)
        if self.type not in ('binary', 'regression'):
            raise ValueError('Task must be binary or regression')
        typ = sql_type(self.data_type or ('INT' if self.type == 'binary' else 'FLOAT'))
        if typ not in ({'INT', 'BIGINT'} if self.type == 'binary' else {'FLOAT', 'DOUBLE'}):
            raise ValueError('Label SQL type is incompatible with task semantics')
        object.__setattr__(self, 'data_type', typ)


FAMILIES = {'tzrec.dssm', 'tzrec.wide_and_deep', 'tzrec.deepfm', 'tzrec.mmoe',
            'tzrec.rocket_launching', 'gbdt.catboost', 'gbdt.lightgbm', 'gbdt.xgboost'}
BASE_PARAMS = {'version', 'image'}
TZREC_PARAMS = {'batch_size', 'num_workers', 'num_epochs', 'embedding_dim', 'num_buckets',
                'sparse_lr', 'dense_lr', 'nnodes', 'nproc_per_node', 'mixed_precision', 'eval_input_path'}
FAMILY_PARAMS = {
    'tzrec.dssm': {'user_hidden_units', 'item_hidden_units', 'output_dim'},
    'tzrec.wide_and_deep': {'hidden_units'}, 'tzrec.deepfm': {'hidden_units'},
    'tzrec.mmoe': {'num_expert', 'expert_hidden_units', 'task_hidden_units'},
    'tzrec.rocket_launching': {'booster_hidden_units', 'light_hidden_units', 'share_hidden_units'},
    'gbdt.catboost': {'objective', 'metric', 'num_iterations', 'learning_rate', 'max_depth',
                     'l2_regularization', 'cb_iterations', 'cb_depth', 'cb_l2_leaf_reg'},
    'gbdt.lightgbm': {'objective', 'metric', 'num_iterations', 'learning_rate', 'max_depth', 'num_leaves',
                     'feature_fraction', 'bagging_fraction', 'bagging_freq', 'min_data_in_leaf', 'l2_regularization'},
    'gbdt.xgboost': {'objective', 'metric', 'num_iterations', 'learning_rate', 'max_depth',
                   'feature_fraction', 'bagging_fraction', 'min_child_weight', 'l2_regularization'}}
FEATURE_OPTIONS = {'bucket_size', 'embedding_dim', 'normalizer', 'default_value', 'separator',
                   'value_dim', 'boundaries', 'embedding', 'autodis.num_channels'}


@dataclass(frozen=True)
class ModelRecipe:
    id: str
    family: str
    params: dict[str, Any] = field(default_factory=dict)

    def validate(self, features, tasks):
        identifier(self.id)
        if self.family not in FAMILIES:
            raise ValueError(f'unsupported_public_workflow: {self.family}')
        if not features or len({f.name for f in features}) != len(features):
            raise ValueError('Features must be nonempty and unique')
        if not tasks or len({t.id for t in tasks}) != len(tasks) or len({t.column for t in tasks}) != len(tasks):
            raise ValueError('Task identities and label columns must be unique')
        names = {f.name for f in features}
        reserved = {t.column for t in tasks} | {'row_id', 'query_id', 'split', 'source', 'eligible', 'catalog_id', 'mq_item_identity'}
        if names & reserved or {f.source or f.name for f in features} & reserved:
            raise ValueError('Labels and audit identities cannot be model features')
        if self.family == 'tzrec.mmoe':
            if len(tasks) < 2:
                raise ValueError('MMoE needs at least two real tasks')
        elif len(tasks) != 1 or tasks[0].type != 'binary':
            raise ValueError('This public recipe requires one binary task')
        if self.family == 'tzrec.dssm' and ({f.role for f in features} != {'user', 'item'}):
            raise ValueError('DSSM requires explicit user/item features, no implicit tower assignment')
        if self.family in ('gbdt.lightgbm', 'gbdt.xgboost') and any(f.type not in ('FLOAT', 'DOUBLE') for f in features):
            raise ValueError('LightGBM/XGBoost public models require explicit float feature views')
        if self.family == 'gbdt.catboost' and any(f.type not in ('STRING', 'FLOAT', 'DOUBLE', 'INT', 'BIGINT') for f in features):
            raise ValueError('CatBoost public recipe requires scalar features')
        allowed = BASE_PARAMS | FAMILY_PARAMS[self.family] | (TZREC_PARAMS if self.family.startswith('tzrec.') else set())
        for key in self.params:
            if key in allowed:
                if key == 'objective' and self.params[key] != 'binary':
                    raise ValueError('This public GBDT serving recipe supports binary probabilities only')
                continue
            if self.family.startswith('tzrec.') and key.startswith('column.'):
                parts = key.split('.', 2)
                if len(parts) == 3 and parts[1] in names and parts[2] in FEATURE_OPTIONS:
                    continue
            raise ValueError(f'unsupported_public_workflow parameter: {key}; no protobuf/native override')

    def parameters(self, features, tasks, seed):
        self.validate(features, tasks)
        p = {'model': self.family, 'label_columns': ','.join(t.column for t in tasks),
             **{k: str(v) for k, v in self.params.items()}, 'random_seed': str(seed)}
        if self.family == 'tzrec.dssm':
            p.update(user_features=','.join(f.name for f in features if f.role == 'user'),
                     item_features=','.join(f.name for f in features if f.role == 'item'))
        if self.family == 'tzrec.mmoe':
            p.update({f'task.{t.column}.type': t.type for t in tasks})
        return p

    def outputs(self, tasks):
        if self.family == 'tzrec.dssm':
            return {'query': 'user_tower_emb', 'item': 'item_tower_emb'}
        return {t.id: (f'probs_{t.column}' if t.type == 'binary' else f'y_{t.column}')
                if self.family == 'tzrec.mmoe' else 'probs_light'
                if self.family == 'tzrec.rocket_launching' else 'probs' for t in tasks}


@dataclass
class SQLStep:
    id: str
    endpoint: str
    files: list[str]
    replay: str = 'verify'  # safe, verify, never
    checks: list[dict] = field(default_factory=list)
    capture: dict | None = None
    kind: str = 'sql'
    model_id: str | None = None
    recipe_id: str | None = None
    seed: int | None = None
    result_roles: list[str] = field(default_factory=list)


@dataclass
class WorkflowPlan:
    name: str
    fingerprint: str
    config: dict
    features: list[dict]
    tasks: list[dict]
    steps: list[SQLStep]
    files: dict[str, str]
    evaluations: list[dict]
    format_version: int = 4
    execution_profile: str = 'user_workflow'
    resources: dict = field(default_factory=dict)

    def to_dict(self):
        value = asdict(self)
        value['plan_id'] = stable_id(value)
        return value


@dataclass(frozen=True)
class ProtocolRecipe:
    """Declare the public candidate semantics independently of feature columns."""
    candidate_scope: str = 'full_catalog'
    exclude_seen: bool = False
    recall_denominator: str = 'all_targets'
    allow_shortlist: bool = True
    history: str = 'frozen_train'
    version: int = 1

    def validate(self, retrieval):
        if type(self.version) is not int or self.version != 1:
            raise ValueError('Unsupported protocol version')
        if self.history != 'frozen_train':
            raise ValueError('unsupported_protocol: only frozen_train history is implemented')
        for name in ('exclude_seen', 'allow_shortlist'):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f'protocol.{name} must be boolean')
        if self.candidate_scope not in ('full_catalog', 'observed_only'):
            raise ValueError('unsupported_protocol: candidate_scope must be full_catalog or observed_only')
        if retrieval and self.recall_denominator != 'all_targets':
            raise ValueError('Recall denominator must preserve all held-out positives')

    def validate_source(self, manifest, retrieval):
        if not retrieval:
            return
        declared = manifest.get('candidate_scope')
        if declared != self.candidate_scope:
            raise ValueError('unsupported_protocol: dataset candidate scope differs from the public SQL recipe')
        if manifest.get('exclude_seen_required', manifest.get('exclude_train_rated_required')) and not self.exclude_seen:
            raise ValueError('Dataset protocol requires excluding training-observed items')


def _mapping(value, path, allowed, required=()):
    if not isinstance(value, dict):
        raise ValueError(f'{path} must be a mapping')
    if any(not isinstance(key, str) for key in value):
        raise ValueError(f'{path} field names must be strings')
    unknown = set(value) - set(allowed)
    missing = set(required) - set(value)
    if unknown or missing:
        raise ValueError(f'{path}: unknown fields {sorted(unknown)}; missing fields {sorted(missing)}')
    return value


def _positive(value, path, integer=False, zero=False):
    valid = type(value) is int if integer else finite(value)
    if not valid or value < 0 or (value == 0 and not zero):
        raise ValueError(f'{path} must be a {"nonnegative" if zero else "positive"} {"integer" if integer else "number"}')


def _text(value, path):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{path} must be a nonempty string')


def validate_dataset_definition(value):
    """Validate declarative datasets without a dataset-name registry or branches."""
    fields = {'schema_version', 'id', 'name', 'dataset', 'columns', 'tasks', 'views', 'protocol', 'evaluation'}
    _mapping(value, 'dataset definition', fields | {'source', 'preparation'}, fields)
    definition = copy.deepcopy(value)
    if type(definition['schema_version']) is not int or definition['schema_version'] != 1:
        raise ValueError('Dataset definitions require schema_version=1')
    identifier(definition['id']); _text(definition['name'], 'dataset name')
    _mapping(definition['dataset'], 'dataset defaults',
             {'adapter', 'user_key', 'item_key', 'query_key', 'hash_bucket_multiplier', 'hash_bucket_max'})
    for section, factory, allowed, required in (
            ('columns', Feature, {'type', 'role'}, {'type'}),
            ('tasks', Task, {'column', 'type', 'description'}, {'column'})):
        catalog = definition[section]
        if not isinstance(catalog, dict) or not catalog:
            raise ValueError(f'Dataset {section} must be a nonempty mapping')
        for name, entry in catalog.items():
            identifier(name); _mapping(entry, section + '.' + name, allowed, required)
            if section == 'columns':
                factory(name=name, **entry)
                entry['type'] = sql_type(entry['type'])
            else:
                task = {key: val for key, val in entry.items() if key != 'description'}
                factory(id=name, **task)
                if 'description' in entry:
                    _text(entry['description'], 'task description')
                column = entry['column']
                if column not in definition['columns'] or 'role' in definition['columns'][column]:
                    raise ValueError('Task labels must reference declared non-feature columns')
                types = {'INT', 'BIGINT'} if task.get('type', 'binary') == 'binary' else {'FLOAT', 'DOUBLE'}
                if definition['columns'][column]['type'] not in types:
                    raise ValueError('Label column type differs from task semantics')
    if 'source' in definition:
        source = _mapping(definition['source'], 'dataset source',
                          {'version', 'url', 'homepage', 'md5', 'sha256', 'files'}, {'version', 'url', 'files'})
        for name in ('version', 'url', 'homepage'):
            if name in source:
                _text(source[name], 'source.' + name)
        url = urlsplit(source['url'])
        archive = Path(url.path).name
        if url.scheme not in ('http', 'https') or not url.netloc or archive in ('', '.', '..'):
            raise ValueError('Source URL must identify an HTTP(S) archive')
        checksums = set(source) & {'md5', 'sha256'}
        if len(checksums) != 1:
            raise ValueError('Source must pin exactly one archive checksum')
        algorithm = next(iter(checksums))
        checksum = source[algorithm]
        if not isinstance(checksum, str) or not re.fullmatch(r'[0-9a-f]{' + str(32 if algorithm == 'md5' else 64) + '}', checksum):
            raise ValueError('Invalid source archive checksum')
        files = source['files']
        if not isinstance(files, dict) or not files:
            raise ValueError('source.files must map input roles to archive basenames')
        for role, name in files.items():
            identifier(role)
            if not isinstance(name, str) or Path(name).name != name or name in ('', '.', '..', archive, 'download.json'):
                raise ValueError('Source inputs must have safe, distinct basenames')
        if len(files) != len(set(files.values())):
            raise ValueError('Duplicate source input filenames')
    if 'preparation' in definition:
        validate_preparation(definition)
    _mapping(definition['protocol'], 'dataset protocol',
             {'candidate_scope', 'exclude_seen', 'recall_denominator', 'allow_shortlist', 'history', 'version'}, {'candidate_scope'})
    _mapping(definition['evaluation'], 'dataset evaluation', {'split', 'ks', 'gauc_group', 'metrics', 'slice_by'})
    if not isinstance(definition['views'], dict) or not definition['views']:
        raise ValueError('Dataset views must be a nonempty mapping')
    for name, view in definition['views'].items():
        identifier(name)
        _mapping(view, 'view.' + name, {'features', 'tasks', 'models', 'protocol', 'evaluation'}, {'features', 'tasks'})
        for section in ('features', 'tasks'):
            selected = view[section]
            catalog = definition['columns'] if section == 'features' else definition['tasks']
            if not isinstance(selected, list) or not selected or any(not isinstance(s, str) or s not in catalog for s in selected) or len(selected) != len(set(selected)):
                raise ValueError(f'view.{name}.{section} must select unique catalog entries')
            if section == 'features' and any('role' not in catalog[s] for s in selected):
                raise ValueError('View features require an explicit column role')
        if 'models' in view:
            selected = view['models']
            if not isinstance(selected, list) or not selected or any(not isinstance(s, str) or s not in FAMILIES for s in selected) or len(selected) != len(set(selected)):
                raise ValueError('View models must select supported families')
        if 'protocol' in view:
            _mapping(view['protocol'], 'view protocol', set(definition['protocol']) |
                     {'candidate_scope', 'exclude_seen', 'recall_denominator', 'allow_shortlist', 'history', 'version'})
        if 'evaluation' in view:
            _mapping(view['evaluation'], 'view evaluation', {'split', 'ks', 'gauc_group', 'metrics', 'slice_by'})
    return definition


def validate_preparation(definition):
    """Validate shared preparation operations using only declared column references."""
    fields = {'adapter', 'seed', 'primary_task', 'row_key', 'time_key', 'query_group',
              'query_features', 'item_features', 'defaults', 'array_keys', 'statistics'}
    prep = _mapping(definition['preparation'], 'preparation', fields | {'date_key', 'parameters'}, fields)
    _text(prep['adapter'], 'preparation.adapter')
    if type(prep['seed']) is not int or not 0 <= prep['seed'] < 2**32:
        raise ValueError('Preparation seed must be a uint32 integer')
    if prep['primary_task'] not in definition['tasks'] or definition['tasks'][prep['primary_task']].get('type', 'binary') != 'binary':
        raise ValueError('Shared recommendation preparation requires a declared primary binary task')
    columns = definition['columns']
    def column(name):
        if not isinstance(name, str) or name not in columns:
            raise ValueError(f'Preparation references an undeclared column: {name!r}')
        return columns[name]
    keys = definition['dataset']
    for name in ('user_key', 'item_key', 'query_key'):
        column(keys.get(name))
    if len({keys[name] for name in ('user_key', 'item_key', 'query_key')}) != 3:
        raise ValueError('User, item and query identities must be distinct')
    if column(keys['user_key']).get('role') != 'user' or column(keys['item_key']).get('role') != 'item':
        raise ValueError('User/item identities require matching feature roles')
    for name, types in (('row_key', {'STRING'}), ('time_key', {'BIGINT'}), ('date_key', {'INT', 'BIGINT'})):
        if name in prep and column(prep[name])['type'] not in types:
            raise ValueError('Invalid preparation identity/time/date column type')
    if column(keys['query_key'])['type'] != 'STRING':
        raise ValueError('Prepared query hashes require a STRING column')
    if any('role' in column(name) for name in (prep['row_key'], keys['query_key'])):
        raise ValueError('Observation and query hashes must be audit columns, not model features')
    for name in ('query_group', 'query_features', 'item_features'):
        selected = prep[name]
        if not isinstance(selected, list) or not selected or len(selected) != len(set(selected)):
            raise ValueError(f'preparation.{name} must select unique columns')
        for entry in selected:
            column(entry)
    if keys['user_key'] not in prep['query_group'] or not set(prep['query_group']) <= set(prep['query_features']):
        raise ValueError('Query features must include the user and all query grouping columns')
    if keys['query_key'] in prep['query_features'] or any(column(c).get('role') == 'item' for c in prep['query_features']):
        raise ValueError('Query features cannot contain item features or the generated query hash')
    if any(column(c).get('role') != 'item' for c in prep['item_features']) or keys['item_key'] in prep['item_features']:
        raise ValueError('Static item features must reference item-role columns excluding the item key')
    defaults = prep['defaults']
    if not isinstance(defaults, dict):
        raise ValueError('preparation.defaults must be a mapping')
    for name in defaults:
        if column(name).get('role') is None:
            raise ValueError('Preparation defaults must target feature columns')
    array_keys = prep['array_keys']
    if not isinstance(array_keys, dict):
        raise ValueError('preparation.array_keys must be a mapping')
    for target, origin in array_keys.items():
        if column(target)['type'] != 'STRING' or not column(origin)['type'].startswith('ARRAY<') or origin not in prep['item_features']:
            raise ValueError('Array grouping keys must map STRING outputs to static item arrays')
    statistics = prep['statistics']
    if not isinstance(statistics, list) or len(statistics) != 2:
        raise ValueError('Preparation requires user and item feedback statistics')
    outputs, stat_keys = [], []
    for entry in statistics:
        _mapping(entry, 'preparation statistic', {'key', 'count', 'positive', 'seen'}, {'key', 'count', 'positive', 'seen'})
        stat_keys.append(entry['key']); column(entry['key'])
        role = column(entry['key']).get('role')
        for name in ('count', 'positive'):
            spec = column(entry[name])
            if spec['type'] not in ('FLOAT', 'DOUBLE') or spec.get('role') != role:
                raise ValueError('Feedback statistics must be floating features in the matching tower')
        if column(entry['seen'])['type'] != 'BOOLEAN' or 'role' in column(entry['seen']):
            raise ValueError('Seen flags must be BOOLEAN audit columns')
        outputs.extend(entry[name] for name in ('count', 'positive', 'seen'))
    if set(stat_keys) != {keys['user_key'], keys['item_key']} or len(outputs) != len(set(outputs)):
        raise ValueError('Statistics require distinct user/item keys and output columns')
    if set(outputs) & (set(prep['item_features']) | set(prep['query_group']) | set(array_keys)):
        raise ValueError('Generated statistics cannot overwrite static features or query identities')
    if any(c in {t['column'] for t in definition['tasks'].values()} for c in prep['query_features']):
        raise ValueError('Query features cannot include task labels')
    if 'parameters' in prep and not isinstance(prep['parameters'], dict):
        raise ValueError('Adapter parameters must be a mapping')


def validate_config(value):
    """Validate the entire declarative recipe before opening data or creating files."""
    fields = {'schema_version', 'execution_profile', 'mode', 'name', 'dataset', 'features',
              'tasks', 'models', 'protocol', 'serving', 'evaluation', 'execution', 'sql'}
    _mapping(value, 'config', fields | {'experiment'}, fields)
    config = copy.deepcopy(value)
    if type(config['schema_version']) is not int or config['schema_version'] != 3 or config['execution_profile'] != 'user_workflow':
        raise ValueError('Only schema_version=3 / user_workflow is supported')
    if config['mode'] != 'fixed_validation':
        raise ValueError('First release supports a frozen fixed_validation run; selection is separate')
    prefix = identifier(config['name'])
    dataset = _mapping(config['dataset'], 'dataset',
                       {'adapter', 'path', 'storage_uri', 'user_key', 'item_key', 'query_key',
                        'max_train_users', 'max_eval_queries', 'max_eval_rows',
                        'hash_bucket_multiplier', 'hash_bucket_max', 'manifest_name', 'task_semantics'}, {'path', 'storage_uri'})
    dataset.setdefault('adapter', 'prepared')
    if dataset['adapter'] != 'prepared':
        raise ValueError('unsupported_dataset_adapter: prepare raw data before building a public workflow')
    for name in ('path', 'storage_uri', 'user_key', 'item_key', 'query_key', 'manifest_name'):
        if name in dataset:
            _text(dataset[name], 'dataset.' + name)
    for name in ('max_train_users', 'max_eval_queries', 'max_eval_rows', 'hash_bucket_multiplier', 'hash_bucket_max'):
        if name in dataset:
            _positive(dataset[name], 'dataset.' + name, integer=True)
    if dataset.get('max_train_users') and not dataset.get('user_key'):
        raise ValueError('User sampling requires an explicit user_key')
    if dataset.get('hash_bucket_max', 4194304) >= 2**31:
        raise ValueError('hash_bucket_max must fit the public int32 bucket size')
    semantics = dataset.get('task_semantics', {})
    if not isinstance(semantics, dict):
        raise ValueError('dataset.task_semantics must be a mapping')
    for task, description in semantics.items():
        identifier(task); _text(description, 'dataset.task_semantics.' + task)
    definitions = {}
    for section, allowed, required, factory in (
            ('features', {'name', 'type', 'role', 'source'}, {'name', 'type'}, Feature),
            ('tasks', {'id', 'column', 'type', 'data_type'}, {'id', 'column'}, Task),
            ('models', {'id', 'family', 'params'}, {'id', 'family'}, ModelRecipe)):
        if not isinstance(config[section], list) or not config[section]:
            raise ValueError(f'{section} must be a nonempty list')
        definitions[section] = []
        for item in config[section]:
            _mapping(item, section, allowed, required)
            for key, param in item.items():
                if key != 'params' and not (key == 'source' and param is None):
                    _text(param, section + '.' + key)
            definitions[section].append(factory(**item))
    features, tasks, models = (definitions[k] for k in ('features', 'tasks', 'models'))
    if len({m.id for m in models}) != len(models):
        raise ValueError('Models must be nonempty and unique')
    for model in models:
        if not isinstance(model.params, dict):
            raise ValueError('Model params must be a mapping')
        if any(not isinstance(key, str) for key in model.params):
            raise ValueError('Model parameter names must be strings')
        model.validate(features, tasks)
        if 'eval_input_path' in model.params:
            raise ValueError('unsupported_public_workflow: native eval path is not bound to a frozen validation table')
        for key, param in model.params.items():
            if not isinstance(param, (str, int, float, bool)) or param is None:
                raise ValueError(f'Model parameter {key} must be a scalar')
            if isinstance(param, (int, float)) and not isinstance(param, bool) and not finite(param):
                raise ValueError(f'Model parameter {key} must be finite')
            if key in ('image', 'version'):
                _text(param, 'model.' + key)
            integer = key in {'batch_size', 'num_workers', 'num_epochs', 'embedding_dim', 'num_buckets',
                              'output_dim', 'nnodes', 'nproc_per_node', 'num_iterations', 'num_leaves',
                              'cb_iterations', 'cb_depth', 'num_expert'} or key.endswith(('.bucket_size', '.embedding_dim', '.value_dim', '.num_channels'))
            numeric = key in {'sparse_lr', 'dense_lr', 'learning_rate'}
            if integer or numeric:
                try:
                    converted = int(param) if integer else float(param)
                except (ValueError, TypeError):
                    raise ValueError(f'Invalid numeric model parameter: {key}') from None
                if isinstance(param, bool) or (integer and str(converted) != str(param)):
                    raise ValueError(f'Model parameter {key} must be an integer')
                _positive(converted, 'model.' + key, integer=integer, zero=key == 'num_workers')
    evaluation = _mapping(config['evaluation'], 'evaluation', {'split', 'ks', 'gauc_group', 'metrics', 'slice_by'})
    evaluation.setdefault('split', 'valid'); evaluation.setdefault('ks', [])
    if evaluation['split'] != 'valid':
        raise ValueError('First release is valid-only; test requires a locked-selection/fixed-regression profile')
    ks = evaluation['ks']
    if not isinstance(ks, list) or any(type(k) is not int or k <= 0 for k in ks) or ks != sorted(set(ks)):
        raise ValueError('ks must be sorted unique positive integers')
    if 'gauc_group' in evaluation:
        _text(evaluation['gauc_group'], 'evaluation.gauc_group')
    slices = evaluation.get('slice_by', [])
    if not isinstance(slices, list) or any(not isinstance(s, str) for s in slices) or len(set(slices)) != len(slices):
        raise ValueError('evaluation.slice_by must select unique audit columns')
    for column in slices:
        identifier(column)
    retrieval = any(m.family == 'tzrec.dssm' for m in models)
    if 'metrics' in evaluation:
        available = {'auc', 'ap'} if any(t.type == 'binary' for t in tasks) else set()
        if evaluation.get('gauc_group') and any(t.type == 'binary' for t in tasks):
            available.add('gauc')
        if any(m.family != 'tzrec.dssm' for m in models) and any(t.type == 'binary' for t in tasks):
            available.update(('logloss', 'brier'))
        if any(t.type == 'regression' for t in tasks):
            available.update(('rmse', 'mae'))
        if retrieval:
            available.update(f'{metric}_at_{k}' for metric in ('recall', 'micro_recall', 'ndcg') for k in ks)
        selected = evaluation['metrics']
        if not isinstance(selected, list) or not selected or any(not isinstance(m, str) for m in selected) or len(set(selected)) != len(selected) or set(selected) - available:
            raise ValueError(f'evaluation.metrics must select supported metrics: {sorted(available)}')
    protocol = _mapping(config['protocol'], 'protocol',
                        {'candidate_scope', 'exclude_seen', 'recall_denominator', 'allow_shortlist', 'history', 'version'})
    recipe = ProtocolRecipe(**protocol); recipe.validate(retrieval)
    config['protocol'] = asdict(recipe)
    serving = _mapping(config['serving'], 'serving',
                       {'entrypoint', 'batch_size', 'candidate_limit', 'vector_connector'}, {'entrypoint', 'batch_size'})
    if serving['entrypoint'] not in ('sql', 'sql_api'):
        raise ValueError('Formal predictions require public SQL or SQL API')
    _positive(serving['batch_size'], 'serving.batch_size', integer=True)
    if retrieval:
        if not dataset.get('user_key') or not dataset.get('item_key'):
            raise ValueError('DSSM requires dataset user_key and item_key')
        if serving.get('vector_connector', 'milvus') != 'milvus':
            raise ValueError('unsupported_public_workflow vector connector')
        limit = serving.get('candidate_limit')
        _positive(limit, 'serving.candidate_limit', integer=True)
        if not ks or not max(ks) <= limit <= 16384:
            raise ValueError('Milvus candidate_limit must cover max K and be <=16384')
    elif dataset.get('max_eval_queries'):
        raise ValueError('max_eval_queries requires a retrieval query protocol')
    execution = _mapping(config['execution'], 'execution',
                         {'seed', 'resources', 'service_resources', 'operation_timeout', 'index_timeout', 'runtime_audit', 'cleanup_timeout'},
                         {'seed', 'resources'})
    seed = execution['seed']
    if type(seed) is not int or not 0 <= seed < 2**31:
        raise ValueError('Execution seed must be a nonnegative int32 value')
    for model in models:
        identifier(model_name(prefix, model.id, seed) + '_recall_function')
    resources = {'NAMESPACE', 'pod_cpu_cores', 'pod_memory', 'pod_cpu_limit', 'pod_memory_limit', 'replicas'}
    for name in ('resources', 'service_resources'):
        if name not in execution:
            continue
        resource = _mapping(execution[name], 'execution.' + name, resources, {'NAMESPACE'})
        _text(resource['NAMESPACE'], name + '.NAMESPACE')
        for key, param in resource.items():
            if key in ('pod_cpu_cores', 'replicas'):
                _positive(param, name + '.' + key, integer=True)
            else:
                _text(param, name + '.' + key)
    for name in ('operation_timeout', 'index_timeout', 'cleanup_timeout'):
        if name in execution:
            _positive(execution[name], 'execution.' + name)
    if 'runtime_audit' in execution:
        audit = _mapping(execution['runtime_audit'], 'runtime_audit',
                         {'hadoop', 'kubectl', 'java_home', 'model_base_uri', 'job_timeout', 'poll_interval'})
        for name in ('hadoop', 'kubectl'):
            if name in audit and (not isinstance(audit[name], list) or not audit[name] or any(not isinstance(s, str) or not s for s in audit[name])):
                raise ValueError(f'runtime_audit.{name} must be a nonempty argument list')
        for name in ('java_home', 'model_base_uri'):
            if name in audit:
                _text(audit[name], 'runtime_audit.' + name)
        for name in ('job_timeout', 'poll_interval'):
            if name in audit:
                _positive(audit[name], 'runtime_audit.' + name)
    sql = _mapping(config['sql'], 'sql', {'endpoints'}, {'endpoints'})
    endpoints = _mapping(sql['endpoints'], 'sql.endpoints', {'sqlrec', 'warehouse', 'api', 'milvus'}, {'sqlrec', 'warehouse'})
    for name, endpoint in endpoints.items():
        allowed = {'host', 'port', 'database', 'auth', 'username', 'password_env', 'configuration', 'rpc_timeout'} if name in ('sqlrec', 'warehouse') else (
                  {'base_url', 'authorization_env', 'timeout', 'readiness_timeout'} if name == 'api' else {'url', 'token_env'})
        _mapping(endpoint, 'sql.endpoints.' + name, allowed)
        for key, param in endpoint.items():
            if key in ('port', 'rpc_timeout', 'timeout', 'readiness_timeout'):
                _positive(param, name + '.' + key, integer=key == 'port')
                if key == 'port' and param > 65535:
                    raise ValueError('Invalid endpoint port')
            elif key == 'configuration':
                if not isinstance(param, dict):
                    raise ValueError('HS2 configuration must be a mapping')
            else:
                _text(param, name + '.' + key)
        required = {'host', 'port'} if name in ('sqlrec', 'warehouse') else (
                   {'base_url'} if name == 'api' else {'url', 'token_env'})
        if ((name in ('sqlrec', 'warehouse')) or (name == 'api' and serving['entrypoint'] == 'sql_api') or (name == 'milvus' and retrieval)) and required - set(endpoint):
            raise ValueError(f'Missing endpoint fields: {name}: {sorted(required - set(endpoint))}')
        if name in ('sqlrec', 'warehouse') and endpoint.get('auth', 'NOSASL').upper() not in ('NOSASL', 'NONE', 'LDAP'):
            raise ValueError('Supported HS2 auth: NOSASL, NONE, LDAP')
        if name in ('sqlrec', 'warehouse') and endpoint.get('auth', 'NOSASL').upper() == 'LDAP' and not endpoint.get('password_env'):
            raise ValueError('LDAP requires password_env')
    if serving['entrypoint'] == 'sql_api' and 'api' not in endpoints:
        raise ValueError('sql_api requires an API endpoint')
    if retrieval and 'milvus' not in endpoints:
        raise ValueError('DSSM requires a Milvus endpoint')
    if 'experiment' in config:
        experiment = _mapping(config['experiment'], 'experiment',
                              {'dataset_id', 'feature_view', 'profile', 'run_id', 'dataset_config', 'dataset_config_sha256'})
        for key, param in experiment.items():
            _text(param, 'experiment.' + key)
        if 'dataset_id' in experiment:
            identifier(experiment['dataset_id'])
        if experiment.get('profile') not in (None, 'smoke', 'full'):
            raise ValueError('Unsupported experiment profile')
        if experiment.get('profile') == 'full' and any(dataset.get(k) is not None for k in ('max_train_users', 'max_eval_queries', 'max_eval_rows')):
            raise ValueError('Full runs cannot contain dataset sampling limits')
    return config
