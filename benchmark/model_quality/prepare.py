"""Recommendation preparation algorithms driven by a dataset's own declarations."""
from __future__ import annotations

import json
from pathlib import Path
import argparse
import shutil
import tempfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .common import arrow_type, binary, digest, provenance, require_columns, stable_id, write_json
from .config import DATASETS, PACKAGE, dataset_definition, read_dataset_definition


def load_preparation_config(path, adapter):
    config, metadata = read_dataset_definition(path)
    if config.get('preparation', {}).get('adapter') != adapter:
        raise ValueError('Dataset configuration does not declare this preparation adapter')
    if 'source' not in config:
        raise ValueError('Raw preparation requires a declared dataset source')
    return config, metadata


def preparation_seed(config, override):
    seed = config['preparation']['seed'] if override is None else override
    if type(seed) is not int or not 0 <= seed < 2**32:
        raise ValueError('Preparation seed must be a uint32 integer')
    return seed


def split_percentages(values):
    if not isinstance(values, (list, tuple)) or len(values) != 3 or any(type(v) is not int or v <= 0 for v in values) or sum(values) != 100:
        raise ValueError('split_percent must contain three positive integer percentages summing to 100')
    return values[0], values[0] + values[1]


def write_frame(path, frame, config):
    """Use the same column types that the public SQL feature views select."""
    frame = frame.copy()
    unknown = set(frame.columns) - set(config['columns'])
    if unknown:
        raise ValueError(f'Undeclared prepared columns: {sorted(unknown)}')
    for task in config['tasks'].values():
        if task.get('type', 'binary') == 'binary' and task['column'] in frame:
            frame[task['column']] = binary(frame[task['column']])
    table = pa.Table.from_pandas(frame, preserve_index=False)
    for i, field in enumerate(table.schema):
        type_name = config['columns'][field.name]['type']
        if type_name in ('FLOAT', 'DOUBLE') and not np.isfinite(frame[field.name]).all():
            raise ValueError(f'Nonfinite feature: {field.name}')
        # Also pins large_string and all-empty arrays to the public FG schema.
        table = table.set_column(i, field.name, table.column(i).cast(arrow_type(type_name), safe=True))
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def string_ids(frame, columns):
    for column in columns:
        if frame[column].isna().any():
            raise ValueError(f'Null identifier: {column}')
        frame[column] = frame[column].astype(str)
    return frame


def counts(train, statistic, label):
    result = train.groupby(statistic['key'])[label].agg(['size', 'sum'])
    result.columns = [statistic['count'], statistic['positive']]
    return np.log1p(result.astype('float64'))


def positive_rates(frame, config):
    """Laplace-smoothed rates from the already past-only/leave-one-out counts."""
    for statistic in config['preparation']['statistics']:
        if 'rate' in statistic and statistic['count'] in frame:
            positives = np.expm1(frame[statistic['positive']])
            shows = np.expm1(frame[statistic['count']])
            frame[statistic['rate']] = (positives + 1) / (shows + 2)
    return frame


def past_counts(train, statistic, label, time_key):
    """Exclude all feedback at the same timestamp, independent of row order."""
    at_time = train.groupby([statistic['key'], time_key], sort=True)[label].agg(['size', 'sum'])
    cumulative = at_time.groupby(level=0).cumsum() - at_time
    cumulative.columns = [statistic['count'], statistic['positive']]
    return np.log1p(cumulative.astype('float64')).reset_index()


def fill_features(frame, config):
    for column, default in config['preparation']['defaults'].items():
        if column not in frame:
            continue
        if config['columns'][column]['type'].startswith('ARRAY<'):
            frame[column] = frame[column].map(lambda v: list(v) if isinstance(v, (list, np.ndarray)) else list(default))
        else:
            frame[column] = frame[column].fillna(default)
    for output, origin in config['preparation']['array_keys'].items():
        if origin in frame:
            frame[output] = frame[origin].map(lambda values: json.dumps(sorted(values), separators=(',', ':')))
    return frame


def enrich(frame, items, snapshots, config, temporal_train=False):
    prep, keys = config['preparation'], config['dataset']
    label = config['tasks'][prep['primary_task']]['column']
    static = [keys['item_key'], *prep['item_features']]
    result = frame.merge(items[static], on=keys['item_key'], how='left', validate='many_to_one')
    result = fill_features(result, config)
    for statistic in prep['statistics']:
        key = statistic['key']
        if temporal_train:
            history = past_counts(frame, statistic, label, prep['time_key'])
            result = result.merge(history, on=[key, prep['time_key']], how='left', validate='many_to_one')
        else:
            result = result.merge(snapshots[key], on=key, how='left', validate='many_to_one')
        for name in ('count', 'positive'):
            result[statistic[name]] = result[statistic[name]].fillna(0)
        result[statistic['seen']] = result[key].isin(snapshots[key].index)
    groups = result[prep['query_group']].drop_duplicates().copy()
    groups[keys['query_key']] = [stable_id(*values) for values in groups.itertuples(index=False, name=None)]
    return result.merge(groups, on=prep['query_group'], validate='many_to_one')


def describe(frame, config):
    prep, keys = config['preparation'], config['dataset']
    label = config['tasks'][prep['primary_task']]['column']
    date = prep.get('date_key')
    seen = {s['key']: s['seen'] for s in prep['statistics']}
    return {'rows': len(frame), 'users': int(frame[keys['user_key']].nunique()),
            'items': int(frame[keys['item_key']].nunique()),
            'positive_rate': float(frame[label].mean()) if len(frame) else None,
            'min_date': int(frame[date].min()) if len(frame) and date else None,
            'max_date': int(frame[date].max()) if len(frame) and date else None,
            'cold_user_rows': int((~frame[seen[keys['user_key']]]).sum()),
            'cold_item_rows': int((~frame[seen[keys['item_key']]]).sum())}


def save_dataset(output, train, splits, items, manifest, config, temporal=True, seed=None):
    output = Path(output)
    if (output / 'manifest.json').exists():
        raise FileExistsError(f'Prepared dataset already exists: {output}; use a new output directory')
    prep, keys = config['preparation'], config['dataset']
    seed = preparation_seed(config, seed)
    label = config['tasks'][prep['primary_task']]['column']
    row_key, item_key, query_key = prep['row_key'], keys['item_key'], keys['query_key']
    require_columns(train, [label, keys['user_key'], item_key, prep['time_key'], row_key, *prep['query_group']])
    binary(train[label])
    if train.empty or train[label].nunique() != 2:
        raise ValueError('Training exposures must contain positive and negative labels')
    if train[row_key].isna().any() or train[row_key].duplicated().any():
        raise ValueError('Training exposure identities must be non-null and unique')
    if items[item_key].isna().any() or items[item_key].duplicated().any():
        raise ValueError('Item metadata identities must be non-null and unique')
    snapshots = {s['key']: counts(train, s, label) for s in prep['statistics']}
    samples = enrich(train, items, snapshots, config, temporal_train=temporal)
    if not temporal:
        # Pair holdout features must not include the pair's own feedback.
        for statistic in prep['statistics']:
            for name in ('count', 'positive'):
                column = statistic[name]
                subtract = 1 if name == 'count' else samples[label]
                samples[column] = np.log1p(np.maximum(0, np.expm1(samples[column]) - subtract))
    samples = positive_rates(samples, config)
    samples = samples.sample(frac=1, random_state=seed).reset_index(drop=True)
    write_frame(output / 'train.parquet', samples, config)
    positives = samples.loc[samples[label] == 1].copy()
    write_frame(output / 'recall_train.parquet', positives, config)
    item_statistic = next(s for s in prep['statistics'] if s['key'] == item_key)
    item_features = fill_features(items.merge(snapshots[item_key], on=item_key, how='left', validate='one_to_one'), config)
    for name in ('count', 'positive'):
        item_features[item_statistic[name]] = item_features[item_statistic[name]].fillna(0)
    write_frame(output / 'items.parquet', positive_rates(item_features, config), config)
    manifest.update({'format_version': 1, 'dataset_definition': config,
                     'feature_columns': [name for name, spec in config['columns'].items() if 'role' in spec],
                     'preparation_seed': seed, 'splits': {'train': ['train.parquet'], 'valid': [], 'test': []},
                     'files': {}, 'training': {}, 'audit': {'train': describe(samples, config)}, 'provenance': provenance()})
    # Fixed training views belong to the dataset version, not to a test run.
    for view, spec in config['views'].items():
        columns = [*spec['features'], *(config['tasks'][task]['column'] for task in spec['tasks'])]
        frame = positives if view == 'recall' else samples
        filename = f'training/{view}/split=train/part.parquet'
        write_frame(output / filename, frame[columns], config)
        manifest['training'][view] = {'file': filename, 'rows': len(frame)}
    all_ids = set(samples[row_key])
    for split in ('valid', 'test'):
        query_frames, positive_frames = [], []
        for source, frame in splits[split].items():
            if frame[row_key].isna().any() or frame[row_key].duplicated().any() or all_ids.intersection(frame[row_key]):
                raise ValueError('Exposure identities are null, duplicated or overlap between splits')
            all_ids.update(frame[row_key])
            frame = positive_rates(enrich(frame, items, snapshots, config), config)
            name = f'{split}_{source}.parquet'
            write_frame(output / name, frame, config)
            manifest['splits'][split].append(name)
            manifest['audit'][name] = describe(frame, config)
            query_frames.append(frame[[query_key, *prep['query_features']]])
            positive_frames.append(frame.loc[frame[label] == 1])
        query = pd.concat(query_frames, ignore_index=True).drop_duplicates(query_key)
        write_frame(output / f'queries_{split}.parquet', query, config)
        positive = pd.concat(positive_frames, ignore_index=True)
        write_frame(output / f'recall_{split}.parquet', positive.sample(frac=1, random_state=seed), config)
    for path in sorted(output.rglob('*.parquet')):
        manifest['files'][path.relative_to(output).as_posix()] = digest(path)
    manifest['fingerprint'] = stable_id(json.dumps(manifest['files'], sort_keys=True), manifest['protocol'],
                                        json.dumps(config, sort_keys=True), seed)
    write_json(output / 'manifest.json', manifest)
    return manifest


def preparation_version(name):
    """One version for these fixed rules, not an experiment/cache framework."""
    sources = ['prepare.py', 'common.py', f'prepare_{name.replace("100k", "")}.py']
    return stable_id(digest(PACKAGE / 'configs/datasets' / f'{name}.yaml'),
                     [digest(PACKAGE / source) for source in sources])[:16]


def prepare_one(name, data_root):
    from .common import read_json
    from .download_data import download
    from .data import check_prepared
    from .prepare_movielens import prepare as prepare_ml
    from .prepare_kuairand import prepare as prepare_rand
    from .prepare_kuairec import prepare as prepare_rec

    version = preparation_version(name)
    target = Path(data_root) / 'prepared' / name / version
    if target.exists():
        check_prepared(target, name)
        print(f'Reusing {name}: {target}', flush=True)
        return target
    definition = dataset_definition(name)
    raw = Path(data_root) / 'raw' / name
    receipt = read_json(raw / 'download.json') if (raw / 'download.json').exists() else {}
    if receipt.get('spec') != definition['source'] or any(
            not (raw / filename).is_file() or digest(raw / filename) != receipt.get('files', {}).get(filename)
            for filename in definition['source']['files'].values()):
        download(name, raw)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix='.preparing_', dir=target.parent))
    try:
        adapters = {'movielens100k': prepare_ml, 'kuairand': prepare_rand, 'kuairec': prepare_rec}
        manifest = adapters[name](raw, staging)
        manifest['preparation_version'] = version
        write_json(staging / 'manifest.json', manifest)
        check_prepared(staging, name)
        # Never replace an existing version, including a concurrent preparation.
        if target.exists():
            raise FileExistsError(f'Preparation target already exists: {target}')
        staging.rename(target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    print(f'Prepared {name}: {target}', flush=True)
    return target


def main():
    import os
    parser = argparse.ArgumentParser(description='Download and prepare the bundled datasets once.')
    parser.add_argument('--dataset', choices=(*DATASETS, 'all'), default='all')
    parser.add_argument('--data-root', type=Path, default=Path(os.environ.get('BENCH_DATA_ROOT', PACKAGE / 'data')))
    parser.add_argument('--upload', action='store_true', help='Upload fixed training views and register reusable Hive tables')
    args = parser.parse_args()
    names = DATASETS if args.dataset == 'all' else [args.dataset]
    targets = [prepare_one(name, args.data_root) for name in names]
    if args.upload:
        from .client import HS2Client
        from .config import environment
        from .data import upload_training_tables
        env = environment()
        warehouse = HS2Client(env.endpoints['warehouse']).connect()
        try:
            for target in targets:
                upload_training_tables(target, warehouse, env)
        finally:
            warehouse.close()


if __name__ == '__main__':
    main()
