"""Freeze an existing prepared dataset into public tables and label-free requests."""
from pathlib import Path
import math
from string import Formatter

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..common import arrow_type, digest, load_split, read_json, stable_id, write_json
from .specs import Feature, ProtocolRecipe, Task


def records(frame):
    # Arrow converts numpy arrays/scalars into JSON-compatible interchange values.
    return pa.Table.from_pandas(frame, preserve_index=False).to_pylist()


def task_descriptions(tasks, descriptions, manifest):
    """Resolve only declared description fields against scalar manifest metadata."""
    result = {}
    for task in tasks:
        description = descriptions.get(task.id, task.column)
        for _, key, specification, conversion in Formatter().parse(description):
            if key is None:
                continue
            if specification or conversion or not key.isidentifier() or key not in manifest or not isinstance(manifest[key], (str, int, float, bool)):
                raise ValueError(f'Task description references unavailable/non-scalar manifest field: {key}')
        result[task.id] = description.format_map(manifest)
    return result


class PreparedDatasetAdapter:
    """Read manifest-covered artifacts; raw adapters declare their own columns."""
    def __init__(self, config):
        self.root = Path(config['path']).resolve()
        if config.get('adapter', 'prepared') != 'prepared':
            raise ValueError('unsupported_dataset_adapter: prepare raw data first')
        if not (self.root / 'manifest.json').is_file():
            raise ValueError(f'Prepared dataset is missing at {self.root}; run benchmark/model_quality/run_tests.sh for automatic data setup')
        self.manifest = read_json(self.root / 'manifest.json')
        self.row_key = self.manifest.get('dataset_definition', {}).get('preparation', {}).get('row_key', 'row_id')
        for name, expected in self.manifest['files'].items():
            path = (self.root / name).resolve()
            if not path.is_relative_to(self.root) or digest(path) != expected:
                raise ValueError(f'Dataset checksum mismatch: {name}')

    def require_files(self, names):
        if any(name not in self.manifest['files'] for name in names):
            raise ValueError('Dataset inputs must be covered by the prepared manifest')

    def split(self, split):
        self.require_files(self.manifest['splits'][split])
        frame = load_split(self.root, split)
        if self.row_key != 'row_id':
            if 'row_id' in frame:
                raise ValueError('Prepared observation identity conflicts with the public row_id alias')
            frame = frame.rename(columns={self.row_key: 'row_id'})
        if frame.row_id.isna().any():
            raise ValueError('Null observation identity')
        return frame

    def table(self, name):
        self.require_files([name])
        return pd.read_parquet(self.root / name)


class FeatureView:
    """Select/alias/type columns once for both training and public requests.

    This is a static view, not a hidden fitting/normalization pipeline.
    """
    def __init__(self, features):
        self.features = features

    def apply(self, frame, roles=None):
        result = frame.copy()
        for feature in self.features:
            if roles and feature.role not in roles:
                continue
            source = feature.source or feature.name
            if source not in frame:
                raise ValueError(f'Feature view missing source column: {source}')
            column = pa.Array.from_pandas(frame[source]).cast(arrow_type(feature.type))
            if pa.types.is_floating(column.type):
                if any(v is not None and not np.isfinite(v) for v in column.to_pylist()):
                    raise ValueError(f'Nonfinite feature: {feature.name}')
            if pa.types.is_list(column.type) and pa.types.is_floating(column.type.value_type):
                if any(v is not None and not np.isfinite(v) for row in column.to_pylist() if row is not None for v in row):
                    raise ValueError(f'Nonfinite array feature: {feature.name}')
            result[feature.name] = column.to_pylist()
        return result


def prepare(config, output):
    cfg = config['dataset']; adapter = PreparedDatasetAdapter(cfg)
    manifest = adapter.manifest
    experiment = config.get('experiment', {})
    if cfg.get('manifest_name') and manifest.get('dataset') != cfg['manifest_name']:
        raise ValueError('Prepared manifest does not match dataset.manifest_name')
    if experiment.get('profile') == 'full' and manifest.get('max_users') is not None:
        raise ValueError('Full runs require prepared data without a max_users cohort limit')
    features = [Feature(**f) for f in config['features']]
    tasks = [Task(**t) for t in config['tasks']]
    split = config['evaluation'].get('split', 'valid')
    if split not in ('valid', 'test'):
        raise ValueError('Evaluation split must be valid or test')
    retrieval = any(m['family'] == 'tzrec.dssm' for m in config['models'])
    protocol = ProtocolRecipe(**config['protocol'])
    protocol.validate_source(manifest, retrieval)
    train = adapter.split('train'); observations = adapter.split(split)
    if set(train.row_id) & set(observations.row_id):
        raise ValueError('Training and evaluation identities overlap')
    train['row_id'] = train.row_id.astype(str)
    observations['row_id'] = observations.row_id.astype(str)
    if train.row_id.duplicated().any() or observations.row_id.duplicated().any() or set(train.row_id) & set(observations.row_id):
        raise ValueError('String-normalized observation identities overlap')
    user_key = cfg.get('user_key')
    max_users = cfg.get('max_train_users')
    if max_users is not None:
        if not user_key or type(max_users) is not int or max_users <= 0:
            raise ValueError('User sampling requires an explicit user_key and positive limit')
        users = sorted(train[user_key].unique(), key=lambda x: stable_id('cohort', x))[:int(max_users)]
        train = train[train[user_key].isin(users)]
        observations = observations[observations[user_key].isin(users)]
    if not len(train) or not len(observations):
        raise ValueError('Empty train/evaluation cohort')
    source_columns = [f.source or f.name for f in features]
    required = [f.name for f in features] + [t.column for t in tasks]
    for frame in (train, observations):
        if set(source_columns + [t.column for t in tasks]) - set(frame):
            raise ValueError('Dataset does not implement declared features/tasks')
        for task in tasks:
            if task.type == 'binary' and not np.isin(frame[task.column], [0, 1]).all():
                raise ValueError('Binary tasks need real non-null hard 0/1 labels')
            if not np.isfinite(frame[task.column].to_numpy(dtype=float)).all():
                raise ValueError('Nonfinite labels')
    # Shared static schema conversion is frozen before both Parquet and requests.
    for key in (cfg.get('user_key'), cfg.get('item_key'), 'row_id'):
        if key and (key not in train or train[key].isna().any() or key not in observations or observations[key].isna().any()):
            raise ValueError(f'Missing/null dataset identity: {key}')
        if key and any(f.name == key and f.source not in (None, key) for f in features):
            raise ValueError('Feature aliases cannot overwrite dataset identity columns')
    view = FeatureView(features)
    train = view.apply(train); observations = view.apply(observations)
    bundle = {'split': split, 'source_manifest': manifest, 'train_rows': len(train),
              'sample_kind': 'integration_smoke' if max_users else 'validation_sample' if cfg.get('max_eval_queries') or cfg.get('max_eval_rows')
              else 'prepared_cohort' if manifest.get('max_users') else 'prepared_dataset',
              'train_users': sorted(map(str, train[user_key].unique())) if user_key else None,
              'protocol': config['protocol'], 'experiment': experiment,
              'task_semantics': task_descriptions(tasks, cfg.get('task_semantics', {}), manifest),
              'source_evaluation_rows': len(observations), 'feature_statistics': {}}
    catalog = None
    if retrieval:
        item_key = cfg['item_key']; query_key = cfg.get('query_key', 'query_id')
        queries = adapter.table(f'queries_{split}.parquet')
        if query_key not in observations or observations[query_key].isna().any():
            raise ValueError('Missing/null observation query identity')
        if query_key not in queries or user_key not in queries or queries[[query_key, user_key]].isna().any().any():
            raise ValueError('Missing/null query identity or user binding')
        if not set(observations[query_key]).issubset(set(queries[query_key])):
            raise ValueError('Prepared queries do not cover evaluation observations')
        queries = queries[queries[query_key].isin(observations[query_key])]
        queries = view.apply(queries, roles={'user'})
        bindings = queries.set_index(query_key)[user_key]
        if not bindings.index.is_unique or not observations[user_key].eq(observations[query_key].map(bindings)).all():
            raise ValueError('Query user binding differs from evaluation observations')
        queries = queries.iloc[sorted(range(len(queries)), key=lambda i: stable_id('query', queries.iloc[i][query_key]))]
        if cfg.get('max_eval_queries') is not None:
            limit = cfg['max_eval_queries']
            if type(limit) is not int or limit <= 0:
                raise ValueError('max_eval_queries must be a positive integer')
            queries = queries.head(limit)
        observations = observations[observations[query_key].isin(queries[query_key])]
        if queries[query_key].astype(str).duplicated().any() or not len(queries):
            raise ValueError('Query identities must be nonempty and unique')
        items = adapter.table('items.parquet')
        if 'eligible' in items:
            items = items[items.eligible]
        if items[item_key].isna().any() or items[item_key].astype(str).duplicated().any() or not len(items):
            raise ValueError('Catalog identities must be nonempty and unique')
        items = items.sort_values(item_key).reset_index(drop=True)
        if protocol.candidate_scope == 'observed_only' and config['serving']['candidate_limit'] < len(items):
            raise ValueError('observed_only requires retrieving the entire catalog before public SQL candidate filtering; increase candidate_limit (maximum 16384)')
        items['catalog_id'] = np.arange(1, len(items) + 1, dtype=np.int64)
        user_features = [f.name for f in features if f.role == 'user']
        item_features = [f.name for f in features if f.role == 'item']
        items = view.apply(items, roles={'item'})
        catalog = items
        if protocol.exclude_seen:
            seen = train.groupby(user_key)[item_key].agg(lambda s: sorted(set(map(str, s)))).to_dict()
        else:
            seen = {}
        query_rows = []
        for q in records(queries):
            ids = seen.get(q[user_key], [])
            query_rows.append({'query_id': str(q[query_key]),
                               'features': {c: q[c] for c in user_features}, 'seen': ids})
        catalog_ids = set(map(str, items[item_key]))
        max_seen = max((len(set(q['seen']) & catalog_ids) for q in query_rows), default=0)
        required_budget = len(items) if protocol.candidate_scope == 'observed_only' else min(
            len(items), max(config['evaluation']['ks']) + max_seen)
        limit = min(config['serving']['candidate_limit'], max(len(items), max(config['evaluation']['ks'])))
        config['serving']['candidate_limit'] = limit
        limited = limit < required_budget
        if limited and not protocol.allow_shortlist:
            raise ValueError('candidate_limit cannot cover K after excluding seen items; increase it or allow_shortlist')
        bundle['candidate_budget'] = {'limit': limit, 'required_for_complete_topk': required_budget,
                                      'max_seen_in_catalog': max_seen, 'candidate_limited': limited}
        # One group operation replaces a full observation scan for every query.
        positive = observations[observations[tasks[0].column] == 1]
        grouped = positive.groupby(positive[query_key].astype(str))[item_key].agg(lambda values: sorted(set(map(str, values))))
        targets = {q['query_id']: grouped.get(q['query_id'], []) for q in query_rows}
        if protocol.candidate_scope == 'observed_only':
            # Membership comes from observations, independently of all labels.
            candidates = observations.groupby(observations[query_key].astype(str))[item_key].agg(
                lambda values: sorted(set(map(str, values))))
            for query in query_rows:
                query['candidates'] = candidates.get(query['query_id'], [])
        retrieval_slices = {}
        for column in config['evaluation'].get('slice_by', []):
            if column not in observations or observations[column].isna().any():
                raise ValueError(f'Missing/null audit slice column: {column}')
            for value, frame in observations.groupby(column, sort=True):
                grouped_targets = frame[frame[tasks[0].column] == 1].groupby(
                    frame[query_key].astype(str))[item_key].agg(lambda values: sorted(set(map(str, values))))
                ids = sorted(set(frame[query_key].astype(str)))
                retrieval_slices[f'{column}={value}'] = {'query_ids': ids,
                    'targets': {qid: grouped_targets.get(qid, []) for qid in ids}}
        bundle.update(queries=query_rows, targets=targets,
                      retrieval_slices=retrieval_slices, catalog_rows=len(items),
                      items=[{'catalog_id': r['catalog_id'], 'item_id': str(r[item_key]),
                              'features': {c: r[c] for c in item_features}} for r in records(items)])
    # Size categorical buckets against the frozen cohort and the served catalog.
    # The rate below is a uniform-hash estimate, not measured TZRec collisions.
    for feature in features:
        if feature.type not in ('STRING', 'ARRAY<STRING>'):
            continue
        values = train[feature.name]
        if catalog is not None and feature.role == 'item':
            values = pd.concat([values, catalog[feature.name]], ignore_index=True)
        if feature.type == 'ARRAY<STRING>':
            values = values.explode()
        cardinality = int(values.dropna().nunique())
        stats = {'cardinality': cardinality, 'scope': 'train_and_served_catalog' if catalog is not None and feature.role == 'item' else 'train', 'models': {}}
        for model in config['models']:
            if not model['family'].startswith('tzrec.'):
                continue
            params = model.setdefault('params', {})
            key = f'column.{feature.name}.bucket_size'
            if cfg.get('hash_bucket_multiplier') and key not in params and 'num_buckets' not in params:
                desired = max(2, cardinality * cfg['hash_bucket_multiplier'])
                params[key] = min(1 << (desired - 1).bit_length(), cfg.get('hash_bucket_max', 4194304))
            buckets = int(params.get(key, params.get('num_buckets', 1000000)))
            estimate = 0.0 if cardinality <= 1 else 1.0 if buckets == 1 else -math.expm1((cardinality - 1) * math.log1p(-1.0 / buckets))
            stats['models'][model['id']] = {'bucket_size': buckets, 'estimated_shared_bucket_rate': estimate,
                                           'estimate_assumption': 'independent uniform hash; not measured collisions'}
        bundle['feature_statistics'][feature.name] = stats
    schema = pa.schema([(f.name, arrow_type(f.type)) for f in features] +
                       [(t.column, arrow_type(t.data_type)) for t in tasks])
    files = {}; tables = [('samples', train)]
    if retrieval:
        positive = train[train[tasks[0].column] == 1]
        if len(positive) < 2:
            raise ValueError('DSSM requires multiple training positives')
        tables.append(('positive', positive))
        bundle['positive_train_rows'] = len(positive)
    # All schema conversion succeeds before any output file is created.
    tables = [(name, pa.Table.from_pandas(frame[required], preserve_index=False).cast(schema)) for name, frame in tables]
    group = config['evaluation'].get('gauc_group')
    if group and group not in observations:
        raise ValueError(f'Missing GAUC group column: {group}')
    if group and observations[group].isna().any():
        raise ValueError(f'Null GAUC group identity: {group}')
    # Cap paired scoring only; retrieval targets/candidates retain all observations
    # for the selected queries. Row selection is independent of labels.
    limit = cfg.get('max_eval_rows')
    if limit is not None:
        order = sorted(range(len(observations)), key=lambda i: stable_id('scalar', observations.iloc[i]['row_id']))
        observations = observations.iloc[order[:limit]]
    slice_columns = config['evaluation'].get('slice_by', [])
    for column in slice_columns:
        if column not in observations or observations[column].isna().any():
            raise ValueError(f'Missing/null audit slice column: {column}')
    extra_columns = [c for c in (user_key, group, *slice_columns) if c and c not in required and c != 'row_id']
    observation_columns = list(dict.fromkeys(['row_id'] + required + extra_columns))
    bundle['observations'] = records(observations[observation_columns])
    bundle['scalar_inputs'] = records(observations[['row_id'] + [f.name for f in features]])
    bundle['evaluation_rows'] = len(observations)
    for name, table in tables:
        relative = f'data/{name}/split=train/part.parquet'
        target = output / relative; target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, target); files[relative] = digest(target)
    bundle['fingerprint'] = stable_id(manifest['fingerprint'], files, config['dataset'],
                                      [r['row_id'] for r in bundle['observations']],
                                      bundle.get('queries'), bundle.get('items'))
    write_json(output / 'data_contract.json', bundle)
    files['data_contract.json'] = digest(output / 'data_contract.json')
    return bundle, files
