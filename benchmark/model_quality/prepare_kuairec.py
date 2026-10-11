"""KuaiRec small_matrix: deterministic pair holdout, never implicit negatives."""
from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pandas as pd

from .common import digest, require_columns, stable_id
from .prepare import (load_preparation_config, preparation_seed, save_dataset,
                      split_percentages, string_ids)


def prepare(raw, output):
    raw = Path(raw)
    config, metadata = load_preparation_config(Path(__file__).parent / 'configs/datasets/kuairec.yaml',
                                              'benchmark.model_quality.prepare_kuairec')
    spec, source_spec = config['preparation']['parameters'], config['source']
    seed = preparation_seed(config, None)
    threshold = spec['positive_threshold']
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
    # Feedback must not choose rows or metadata: held-out labels cannot change
    # training features. Resolve timestamp ties with static fields only.
    frame = frame.sort_values(['timestamp_missing', 'timestamp', 'video_duration', 'user_id', 'video_id'],
                              kind='stable').drop_duplicates(['user_id', 'video_id']).copy()
    frame['row_id'] = [stable_id('kuairec-pair', u, v) for u, v in frame[['user_id', 'video_id']].itertuples(index=False, name=None)]
    buckets = np.array([int(stable_id('pair-split', seed, u, v)[:16], 16) % 100
                        for u, v in frame[['user_id', 'video_id']].itertuples(index=False, name=None)])
    frame['split'] = np.where(buckets < train_end, 'train', np.where(buckets < valid_end, 'valid', 'test'))
    frame['label'] = (frame.watch_ratio > threshold).astype('int32')
    frame['time_ms'] = (frame.timestamp * 1000).astype('int64')
    frame['tab'], frame['source'] = '0', 'observed'
    categories = pd.read_csv(categories_path, dtype={'video_id': str})
    require_columns(categories, ['video_id', 'feat'])
    def tags(value):
        decoded = ast.literal_eval(value) if isinstance(value, str) else value
        if not isinstance(decoded, list):
            raise ValueError('item_categories feat must be a list')
        return sorted(set(map(str, decoded)))
    categories['tag'] = categories.feat.map(tags)
    # Static duration; no aggregate user/item statistics from held-out pairs.
    items = frame.drop_duplicates('video_id')[['video_id', 'video_duration']].copy()
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
                'split_seed': seed, 'split_percent': spec['split_percent'], 'positive_threshold': threshold, 'max_users': None,
                'raw_files': {p.name: digest(p) for p in (matrix_path, categories_path)},
                'missing_timestamps': missing_timestamps, 'missing_timestamp_policy': 'retained; time_ms=0 sentinel; pair holdout only',
                'repeat_pairs_removed': original_rows - len(frame),
                'ignored_features': ['big_matrix', 'full-matrix statistics', 'user snapshot', 'missing pairs']}
    return save_dataset(output, train, splits, items, manifest, config, temporal=False, seed=seed)
