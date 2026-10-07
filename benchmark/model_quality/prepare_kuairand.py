"""KuaiRand-Pure: temporal holdout, frozen profiles, real exposure negatives."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from .common import binary, digest, require_columns, stable_id
from .prepare import (load_preparation_config, preparation_seed, save_dataset,
                      select_users, string_ids)


def prepare(raw, output, max_users=None, seed=None, config_path=None):
    raw = Path(raw)
    config, metadata = load_preparation_config(config_path or Path(__file__).parent / 'configs/datasets/kuairand.yaml',
                                              'benchmark.model_quality.prepare_kuairand')
    spec = config['preparation']['parameters']
    source_spec = config['source']
    seed = preparation_seed(config, seed)
    files = [(source_spec['files'][role], source) for role, source in spec['log_sources'].items()]
    if not files or any(source not in ('standard', 'random') for _, source in files):
        raise ValueError('KuaiRand log_sources must declare standard/random input roles')
    logs, duplicate_count, inputs = [], 0, {}
    for name, source in files:
        frame = pd.read_csv(raw / name, dtype={'user_id': str, 'video_id': str, 'tab': str})
        inputs[name] = digest(raw / name)
        require_columns(frame, ['user_id', 'video_id', 'tab', 'date', 'time_ms', 'is_click', 'long_view', 'is_rand'])
        if not (frame.is_rand == (1 if source == 'random' else 0)).all():
            raise ValueError(f'Unexpected is_rand in {name}')
        before = len(frame)
        frame = frame.drop_duplicates().copy()
        duplicate_count += before - len(frame)
        string_ids(frame, ['user_id', 'video_id', 'tab'])
        frame['label'] = binary(frame.is_click)
        frame['long_view'] = binary(frame.long_view)
        frame['date'] = pd.to_numeric(frame.date, errors='raise').astype('int64')
        frame['time_ms'] = pd.to_numeric(frame.time_ms, errors='raise').astype('int64')
        if (frame.time_ms <= 0).any():
            raise ValueError('time_ms must be positive')
        frame['source'] = source
        # Retain distinct repeat events; remove only exact duplicated export rows.
        frame['row_id'] = [stable_id(name, i) for i in frame.index]
        logs.append(frame[['row_id', 'user_id', 'video_id', 'tab', 'time_ms', 'date', 'label', 'long_view', 'source']])
    events = pd.concat(logs, ignore_index=True)
    # Logged dates have delayed-reporting overlap near midnight. Keep date windows,
    # purge the preceding window at each minimum timestamp; never guess a timezone.
    ranges = events.groupby('date').time_ms.agg(['min', 'max']).sort_index()
    valid_start = int(events.loc[events.date.between(*spec['valid_dates']), 'time_ms'].min())
    test_start = int(events.loc[events.date.between(*spec['test_dates']), 'time_ms'].min())
    def in_dates(bounds):
        return events.date.between(*bounds)
    train_mask = in_dates(spec['train_dates']) & (events.source == 'standard')
    purged_train = int((train_mask & (events.time_ms >= valid_start)).sum())
    purged_valid = int((in_dates(spec['valid_dates']) & (events.time_ms >= test_start)).sum())
    train = events.loc[train_mask & (events.time_ms < valid_start)].copy()
    seen_before = set(train.video_id)
    cohort = select_users(train, max_users, config['dataset']['user_key']) if max_users is not None else None
    if cohort is not None:
        events = events.loc[events.user_id.isin(cohort)].copy()
        train = train.loc[train.user_id.isin(cohort)].copy()
    metadata_path = raw / source_spec['files']['items']
    items = pd.read_csv(metadata_path, dtype={'video_id': str, 'author_id': str})
    inputs[metadata_path.name] = digest(metadata_path)
    require_columns(items, ['video_id', 'author_id', 'tag', 'video_duration', 'upload_dt'])
    string_ids(items, ['video_id'])
    items['author_id'] = items.author_id.fillna(config['preparation']['defaults']['author_id']).astype(str)
    items['tag'] = items.tag.fillna('').map(lambda t: sorted(set(str(t).split(','))) if str(t) else [])
    durations = pd.to_numeric(items.video_duration, errors='coerce') / 1000
    if (durations.dropna() < 0).any() or np.isinf(durations).any():
        raise ValueError('Invalid static video duration')
    items['duration_s'] = durations.fillna(0).astype('float32')
    uploaded = pd.to_datetime(items.upload_dt, errors='coerce')
    cutoff = pd.to_datetime(str(spec['candidate_cutoff']))
    items['eligible'] = (uploaded < cutoff) | (uploaded.isna() & items.video_id.isin(seen_before))
    items['eligibility_reason'] = np.where(uploaded.notna(), np.where(uploaded < cutoff, 'uploaded_before_cutoff', 'uploaded_later'),
                                         np.where(items.video_id.isin(seen_before), 'seen_before_cutoff', 'unknown_availability'))
    items = items[['video_id', 'author_id', 'tag', 'duration_s', 'eligible', 'eligibility_reason']]
    splits = {split: {source: events.loc[in_dates_for(events, spec[f'{split}_dates']) & (events.source == source)
                                       & ((events.time_ms < test_start) if split == 'valid' else True)].copy()
                      for source in ('standard', 'random')} for split in ('valid', 'test')}
    manifest = {'dataset': config['name'], 'protocol': 'temporal-frozen-profile-full-pool',
                'candidate_scope': config['protocol']['candidate_scope'], 'source_spec': source_spec, **metadata,
                'raw_files': inputs, 'max_users': max_users, 'exact_duplicate_rows_removed': duplicate_count,
                'candidate_eligible': int(items.eligible.sum()), 'candidate_ineligible': int((~items.eligible).sum()),
                'candidate_reasons': {str(k): int(v) for k, v in items.eligibility_reason.value_counts().items()},
                'temporal_boundary_audit': {'valid_start_time_ms': valid_start, 'test_start_time_ms': test_start,
                    'purged_train_rows_before_cohort': purged_train, 'purged_valid_rows_before_cohort': purged_valid,
                    'overlapping_logged_dates': int((ranges['min'].iloc[1:].to_numpy() <= ranges['max'].iloc[:-1].to_numpy()).sum())},
                'ignored_features': ['user_features snapshot', 'video_features_statistic', 'post-exposure feedback']}
    return save_dataset(output, train, splits, items, manifest, config, temporal=True, seed=seed)


def in_dates_for(events, bounds):
    return events.date.between(*bounds)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--max-users', type=int, help='Development cohort selected independently of test labels')
    parser.add_argument('--seed', type=int, help='Override preparation.seed from the dataset YAML')
    parser.add_argument('--config', help='Dataset YAML; defaults to configs/datasets/kuairand.yaml')
    args = parser.parse_args()
    if args.max_users is not None and args.max_users <= 0:
        parser.error('--max-users must be positive')
    manifest = prepare(args.raw, args.output, args.max_users, args.seed, args.config)
    print(manifest['fingerprint'])
