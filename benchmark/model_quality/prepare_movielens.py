"""MovieLens preference labels; pair and per-user chronological holdouts."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from .common import digest, stable_id
from .prepare import load_preparation_config, preparation_seed, save_dataset, split_percentages


def dataset_config(path=None):
    return load_preparation_config(path or Path(__file__).parent / 'configs/datasets/movielens100k.yaml',
                                   'benchmark.model_quality.prepare_movielens')


def split_ratings(frame, protocol, seed=None, split_percent=None):
    if seed is None or split_percent is None:
        config, _ = dataset_config()
        seed = preparation_seed(config, seed)
        split_percent = config['preparation']['parameters']['split_percent'] if split_percent is None else split_percent
    train_end, valid_end = split_percentages(split_percent)
    frame = frame.copy()
    if frame.duplicated(['user_id', 'video_id']).any():
        raise ValueError('Duplicate rated user/item pairs')
    if protocol == 'pair':
        buckets = np.array([int(stable_id('pair-split', seed, u, v)[:16], 16) % 100
                            for u, v in frame[['user_id', 'video_id']].itertuples(index=False, name=None)])
        frame['split'] = np.where(buckets < train_end, 'train', np.where(buckets < valid_end, 'valid', 'test'))
    elif protocol == 'user-time':
        frame = frame.sort_values(['user_id', 'time_ms', 'video_id'], kind='stable')
        # Keep co-timestamp rating groups together, including boundary ties.
        frame['split'] = 'train'
        for _, group in frame.groupby('user_id', sort=False):
            times = np.sort(group.time_ms.to_numpy())
            b1, b2 = times[int(len(times) * train_end / 100)], times[int(len(times) * valid_end / 100)]
            frame.loc[group.index, 'split'] = np.where(group.time_ms < b1, 'train', np.where(group.time_ms < b2, 'valid', 'test'))
    else:
        raise ValueError('Unknown split protocol')
    return frame


def prepare(raw, output, protocol=None, seed=None, config_path=None):
    raw = Path(raw)
    config, metadata = dataset_config(config_path)
    parameters, source = config['preparation']['parameters'], config['source']
    seed = preparation_seed(config, seed)
    protocol = parameters['protocol'] if protocol is None else protocol
    threshold = parameters['positive_threshold']
    if not np.isfinite(threshold) or not 1 < threshold <= 5:
        raise ValueError('Rating preference threshold must be finite and in (1, 5]')
    ratings_path, items_path = raw / source['files']['ratings'], raw / source['files']['items']
    frame = pd.read_csv(ratings_path, sep='\t', names=['user_id', 'video_id', 'rating', 'timestamp'],
                        dtype={'user_id': str, 'video_id': str})
    movies = pd.read_csv(items_path, sep='|', encoding='latin-1', header=None, dtype={0: str})
    if len(frame) != parameters['expected_rows'] or frame.user_id.nunique() != parameters['expected_users'] or movies[0].nunique() != parameters['expected_items']:
        raise ValueError('Unexpected configured MovieLens dimensions')
    if not frame.rating.isin([1, 2, 3, 4, 5]).all():
        raise ValueError('Invalid rating')
    frame['row_id'] = [stable_id('ml100k', u, v) for u, v in frame[['user_id', 'video_id']].itertuples(index=False, name=None)]
    frame['label'] = frame.rating.ge(threshold).astype('int32')
    frame['time_ms'] = frame.timestamp.astype('int64') * 1000
    frame['tab'], frame['source'] = '0', 'rated'
    frame = split_ratings(frame, protocol, seed, parameters['split_percent'])
    items = pd.DataFrame({'video_id': movies[0], 'eligible': True, 'eligibility_reason': 'official-catalog'})
    genres = parameters['genres']
    if movies.shape[1] - 5 != len(genres):
        raise ValueError('Movie metadata genre columns differ from configured genres')
    items['tag'] = [[genres[j] for j in range(len(genres)) if row[j]]
                    for row in movies.iloc[:, 5:].itertuples(index=False, name=None)]
    columns = ['row_id', 'user_id', 'video_id', 'rating', 'tab', 'source', 'time_ms', 'label']
    train = frame.loc[frame.split.eq('train'), columns]
    splits = {s: {'rated': frame.loc[frame.split.eq(s), columns]} for s in ('valid', 'test')}
    manifest = {'dataset': config['name'], 'protocol': 'movielens-' + protocol + '-full-catalog',
                'candidate_scope': config['protocol']['candidate_scope'], 'max_users': None,
                'split_seed': seed, 'split_percent': parameters['split_percent'], 'positive_threshold': threshold,
                'source_spec': source, **metadata,
                'raw_files': {path.name: digest(path) for path in (ratings_path, items_path)},
                'label_semantics': f'rating >= {threshold}; lower ratings are observed negatives, not CTR',
                'split_description': 'pair hash' if protocol == 'pair' else 'per-user chronological; boundary timestamp ties stay together; no global cutoff',
                'exclude_seen_required': True, 'ignored_features': ['demographics', 'full-data aggregates']}
    return save_dataset(output, train, splits, items, manifest, config, temporal=(protocol == 'user-time'), seed=seed)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--protocol', choices=['pair', 'user-time'], help='Override the configured split protocol')
    parser.add_argument('--seed', type=int, help='Override preparation.seed from the dataset YAML')
    parser.add_argument('--config', help='Dataset YAML; defaults to configs/datasets/movielens100k.yaml')
    args = parser.parse_args()
    print(prepare(args.raw, args.output, args.protocol, args.seed, args.config)['fingerprint'])
