"""KuaiRec small_matrix: deterministic pair holdout, never implicit negatives."""
from __future__ import annotations

import argparse
import ast
from pathlib import Path

import numpy as np
import pandas as pd

from .common import digest, require_columns, stable_id
from .prepare import (load_preparation_config, preparation_seed, save_dataset,
                      select_users, split_percentages, string_ids)


def prepare(raw, output, max_users=None, seed=None, threshold=None, config_path=None):
    raw = Path(raw)
    config, metadata = load_preparation_config(config_path or Path(__file__).parent / 'configs/datasets/kuairec.yaml',
                                              'benchmark.model_quality.prepare_kuairec')
    spec, source_spec = config['preparation']['parameters'], config['source']
    seed = preparation_seed(config, seed)
    threshold = spec['positive_threshold'] if threshold is None else threshold
    train_end, valid_end = split_percentages(spec['split_percent'])
    if not np.isfinite(threshold) or threshold <= 0:
        raise ValueError('Preference threshold must be positive and finite')
    matrix_path, categories_path = raw / source_spec['files']['matrix'], raw / source_spec['files']['categories']
    frame = pd.read_csv(matrix_path, dtype={'user_id': str, 'video_id': str})
    require_columns(frame, ['user_id', 'video_id', 'timestamp', 'watch_ratio', 'video_duration'])
    string_ids(frame, ['user_id', 'video_id'])
    missing_timestamps = int(frame.timestamp.isna().sum())
    # Official small_matrix contains feedback with no time/date. Pair holdout
    # needs no temporal boundary; retain its real labels with an audited sentinel.
    frame['timestamp_missing'] = frame.timestamp.isna()
    frame['timestamp'] = frame.timestamp.fillna(0)
    for col in ('timestamp', 'watch_ratio', 'video_duration'):
        frame[col] = pd.to_numeric(frame[col], errors='raise')
        if not np.isfinite(frame[col]).all() or (frame[col] < 0).any():
            raise ValueError(f'Invalid {col}')
    original_rows = len(frame)
    # Known timestamps precede missing timestamps; label/duration resolve ties
    # independently of input order. Official small_matrix has no repeated pairs.
    frame = frame.sort_values(['timestamp_missing', 'timestamp', 'watch_ratio', 'video_duration'], kind='stable').drop_duplicates(['user_id', 'video_id']).copy()
    frame['row_id'] = [stable_id('kuairec-pair', u, v) for u, v in frame[['user_id', 'video_id']].itertuples(index=False, name=None)]
    buckets = np.array([int(stable_id('pair-split', seed, u, v)[:16], 16) % 100
                        for u, v in frame[['user_id', 'video_id']].itertuples(index=False, name=None)])
    frame['split'] = np.where(buckets < train_end, 'train', np.where(buckets < valid_end, 'valid', 'test'))
    frame['label'] = (frame.watch_ratio > threshold).astype('int32')
    frame['time_ms'] = (frame.timestamp * 1000).astype('int64')
    frame['tab'], frame['source'] = '0', 'observed'
    if max_users is not None:
        cohort = select_users(frame.loc[frame.split == 'train'], max_users, config['dataset']['user_key'])
        frame = frame.loc[frame.user_id.isin(cohort)].copy()
    categories = pd.read_csv(categories_path, dtype={'video_id': str})
    require_columns(categories, ['video_id', 'feat'])
    def tags(value):
        decoded = ast.literal_eval(value) if isinstance(value, str) else value
        if not isinstance(decoded, list):
            raise ValueError('item_categories feat must be a list')
        return sorted(set(map(str, decoded)))
    categories['tag'] = categories.feat.map(tags)
    # Static duration; no aggregate user/item statistics from held-out pairs.
    items = frame.sort_values('time_ms').drop_duplicates('video_id')[['video_id', 'video_duration']].copy()
    items = items.merge(categories[['video_id', 'tag']], on='video_id', how='left', validate='one_to_one')
    items['tag'] = items.tag.map(lambda t: t if isinstance(t, list) else [])
    items['author_id'] = config['preparation']['defaults']['author_id']
    items['duration_s'] = (items.video_duration / 1000).astype('float32')
    items['eligible'], items['eligibility_reason'] = True, 'observed-pair-holdout'
    items = items.drop(columns='video_duration')
    columns = ['row_id', 'user_id', 'video_id', 'tab', 'time_ms', 'timestamp_missing', 'label', 'source']
    train = frame.loc[frame.split == 'train', columns].copy()
    splits = {split: {'observed': frame.loc[frame.split == split, columns].copy()} for split in ('valid', 'test')}
    manifest = {'dataset': config['name'], 'protocol': 'held-out-preference-observed-candidates',
                'candidate_scope': config['protocol']['candidate_scope'], 'source_spec': source_spec, **metadata,
                'split_seed': seed, 'split_percent': spec['split_percent'], 'positive_threshold': threshold, 'max_users': max_users,
                'raw_files': {p.name: digest(p) for p in (matrix_path, categories_path)},
                'missing_timestamps': missing_timestamps, 'missing_timestamp_policy': 'retained; time_ms=0 sentinel; pair holdout only',
                'repeat_pairs_removed': original_rows - len(frame) if max_users is None else None,
                'ignored_features': ['big_matrix', 'full-matrix statistics', 'user snapshot', 'missing pairs']}
    return save_dataset(output, train, splits, items, manifest, config, temporal=False, seed=seed)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--seed', type=int, help='Override preparation.seed from the dataset YAML')
    parser.add_argument('--threshold', type=float, help='Override the configured preference threshold')
    parser.add_argument('--config', help='Dataset YAML; defaults to configs/datasets/kuairec.yaml')
    parser.add_argument('--max-users', type=int)
    args = parser.parse_args()
    if args.max_users is not None and args.max_users <= 0:
        parser.error('--max-users must be positive')
    print(prepare(args.raw, args.output, args.max_users, args.seed, args.threshold, args.config)['fingerprint'])
