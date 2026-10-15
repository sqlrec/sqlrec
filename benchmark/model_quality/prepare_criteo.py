"""Criteo CTR holdout; identical feature rows always stay in the same split."""
from pathlib import Path

import numpy as np
import pandas as pd

from .common import binary, digest, stable_id
from .prepare import (finish_dataset, load_preparation_config, preparation_seed,
                      split_percentages, write_frame, write_training_views)


def prepare(raw, output):
    config, metadata = load_preparation_config(
        Path(__file__).parent / 'configs/datasets/criteo1m.yaml',
        'benchmark.model_quality.prepare_criteo')
    source, parameters = config['source'], config['preparation']['parameters']
    seed = preparation_seed(config, None)
    output = Path(output)
    if (output / 'manifest.json').exists():
        raise FileExistsError(f'Prepared dataset already exists: {output}')
    path = Path(raw) / source['files']['observations']
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    numeric = [f'intCol_{i}' for i in range(13)]
    categorical = [f'catCol_{i}' for i in range(26)]
    if list(frame.columns) != ['target', *numeric, *categorical] or len(frame) != parameters['expected_rows']:
        raise ValueError('Unexpected configured Criteo columns or row count')
    frame.columns = ['label', *[f'i{i}' for i in range(1, 14)], *[f'c{i}' for i in range(1, 27)]]
    frame['label'] = binary(pd.to_numeric(frame.label, errors='raise')).astype('int32')
    features = list(frame.columns[1:])
    for name in features:
        if config['columns'][name]['type'] == 'FLOAT':
            frame[name] = pd.to_numeric(frame[name].replace('', '0'), errors='raise').astype('float32')
            if not np.isfinite(frame[name]).all():
                raise ValueError(f'Nonfinite Criteo feature: {name}')
        else:
            frame[name] = frame[name].replace('', '__MISSING__')
    # Hash only model inputs, never labels or row positions, for duplicate grouping.
    frame['feature_id'] = [stable_id('criteo-features', seed, *row)
                           for row in frame[features].itertuples(index=False, name=None)]
    frame['row_id'] = [stable_id('criteo-row', source['version'], i) for i in range(len(frame))]
    buckets = np.array([int(value[:16], 16) % 100 for value in frame.feature_id])
    train_end, valid_end = split_percentages(parameters['split_percent'])
    splits = np.where(buckets < train_end, 'train', np.where(buckets < valid_end, 'valid', 'test'))
    manifest = {'dataset': config['name'], 'protocol': 'feature-hash-holdout',
                'split_seed': seed, 'split_percent': parameters['split_percent'],
                'label_semantics': 'observed click target=1', 'source_spec': source,
                'raw_files': {path.name: digest(path)}, 'splits': {}, 'audit': {}, **metadata}
    for split in ('train', 'valid', 'test'):
        part = frame.loc[splits == split].copy()
        if part.empty or part.label.nunique() != 2:
            raise ValueError(f'Criteo {split} must contain both label classes')
        if split == 'train':
            part = part.sample(frac=1, random_state=seed).reset_index(drop=True)
            write_training_views(output, part, config, manifest)
        filename = f'{split}.parquet'
        write_frame(output / filename, part, config)
        manifest['splits'][split] = [filename]
        manifest['audit'][split] = {'rows': len(part), 'positive_rate': float(part.label.mean()),
                                    'unique_feature_rows': int(part.feature_id.nunique())}
    return finish_dataset(output, manifest, config, seed)
