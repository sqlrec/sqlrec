"""Versioned prepared data, deterministic samples and reusable training tables."""
from dataclasses import dataclass, field
from pathlib import Path
import subprocess

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .common import arrow_type, binary, digest, identifier, literal, load_split, read_json, stable_id
from .config import EVAL_QUERIES, EVAL_ROWS, SEED, dataset_definition, model_view


def check_prepared(root, name):
    from .prepare import preparation_version
    root = Path(root)
    if not (root / 'manifest.json').is_file():
        raise ValueError(f'Missing {name} data; run python -m benchmark.model_quality.prepare --dataset {name}')
    manifest = read_json(root / 'manifest.json')
    if manifest.get('preparation_version') != preparation_version(name) or manifest.get('max_users') is not None:
        raise ValueError(f'{name} preparation rules changed; run the prepare command again')
    if manifest.get('dataset_definition') != dataset_definition(name):
        raise ValueError('Prepared dataset definition differs from the current definition')
    required = ['train.parquet', 'items.parquet', 'queries_valid.parquet']
    required.extend(manifest['splits']['valid'])
    if set(manifest.get('training', {})) != set(manifest['dataset_definition']['views']):
        raise ValueError('Prepared dataset is missing fixed training views; run the prepare command again')
    required.extend(view['file'] for view in manifest['training'].values())
    if not manifest['splits']['valid'] or set(required) - set(manifest['files']):
        raise ValueError('Prepared manifest does not cover the required inputs')
    for filename, checksum in manifest['files'].items():
        path = (root / filename).resolve()
        if not path.is_relative_to(root.resolve()) or digest(path) != checksum:
            raise ValueError(f'Prepared data checksum mismatch: {filename}')
        for column in pq.read_schema(path):
            expected = manifest['dataset_definition']['columns'].get(column.name)
            if not expected or column.type != arrow_type(expected['type']):
                raise ValueError(f'Prepared column type mismatch: {filename}/{column.name}')
    return manifest


def sample(frame, key, limit):
    if limit is None or len(frame) <= limit:
        return frame.copy()
    # No labels enter selection. Hash once per ID, not once per row/column access.
    order = np.argsort([stable_id(SEED, str(value)) for value in frame[key]], kind='stable')
    return frame.iloc[order[:limit]].copy()


def records(frame):
    return pa.Table.from_pandas(frame, preserve_index=False).to_pylist()


@dataclass
class Dataset:
    name: str
    root: Path
    definition: dict
    manifest: dict
    train: pd.DataFrame
    valid: pd.DataFrame
    items: pd.DataFrame
    queries: pd.DataFrame
    full_eval: bool
    training_tables: dict = field(default_factory=dict)

    def ranking_sample(self):
        return sample(self.valid, 'row_id', None if self.full_eval else EVAL_ROWS)

    def retrieval_sample(self):
        queries = sample(self.queries, 'query_id', None if self.full_eval else EVAL_QUERIES)
        observations = self.valid[self.valid.query_id.isin(queries.query_id)]
        return queries, observations


def load_dataset(name, data_root, full_eval=False):
    from .prepare import preparation_version
    root = Path(data_root) / 'prepared' / name / preparation_version(name)
    manifest = check_prepared(root, name)
    definition = dataset_definition(name)
    train, valid = load_split(root, 'train'), load_split(root, 'valid')
    if train.empty or valid.empty or set(train.row_id) & set(valid.row_id):
        raise ValueError('Empty or overlapping train/valid data')
    for frame in (train, valid):
        for key in ('row_id', 'user_id', 'video_id', 'query_id'):
            if frame[key].isna().any():
                raise ValueError(f'Null dataset identity: {key}')
        for task in definition['tasks'].values():
            binary(frame[task['column']])
    items = pd.read_parquet(root / 'items.parquet')
    items = items[items.eligible].sort_values('video_id').reset_index(drop=True)
    queries = pd.read_parquet(root / 'queries_valid.parquet')
    if items.empty or items.video_id.isna().any() or items.video_id.duplicated().any():
        raise ValueError('Empty/null/duplicate catalog identities')
    if queries.query_id.isna().any() or queries.query_id.duplicated().any():
        raise ValueError('Null/duplicate query identities')
    users = queries.set_index('query_id').user_id
    if not valid.user_id.eq(valid.query_id.map(users)).all():
        raise ValueError('Query identities do not cover valid user bindings')
    return Dataset(name, root, definition, manifest, train, valid, items, queries, full_eval)


def fields(features):
    return '\n    ' + ',\n    '.join(f'`{identifier(f.name)}` {f.type}' for f in features) + '\n'


def options(values):
    return '\n    ' + ',\n    '.join(f'{literal(key)} = {literal(value)}' for key, value in values.items()) + '\n'


def training_identity(manifest, view, env):
    name = manifest['dataset_definition']['id']
    uri = f'{env.storage}/datasets/{name}/{manifest["fingerprint"][:16]}/{view}'
    return 'mq_data_' + stable_id(uri)[:20], uri


def check_training_table(manifest, view, warehouse, env):
    """Read-only validation: evaluation never creates or uploads training data."""
    name, uri = training_identity(manifest, view, env)
    rows = warehouse.execute(f'DESCRIBE FORMATTED `{name}`').rows
    definition = manifest['dataset_definition']
    spec = definition['views'][view]
    columns = [*spec['features'], *(definition['tasks'][task]['column'] for task in spec['tasks'])]
    expected = [(key, definition['columns'][key]['type'].lower()) for key in columns] + [('split', 'string')]
    schema = []
    for row in rows:
        if str(row[0]).startswith('#'):
            break
        schema.append((str(row[0]).strip(), str(row[1]).strip().lower()))
    details = {str(row[0]).strip().rstrip(':'): str(row[1]).strip() for row in rows}
    if schema != expected or details.get('Location', '').rstrip('/') != uri or details.get('Type') != 'EXTERNAL':
        raise ValueError(f'Training table schema/location mismatch: {name}')
    actual = warehouse.execute(f"SELECT COUNT(*) FROM `{name}` WHERE `split`='train'").rows
    if not actual or int(actual[0][0]) != manifest['training'][view]['rows']:
        raise ValueError(f'Training table count differs from prepared input: {name}')
    return name


def upload_training_tables(root, warehouse, env):
    """Prepare each version/view once; an existing table is checked and reused."""
    root = Path(root)
    manifest = read_json(root / 'manifest.json')
    existing = {str(row[1]) for row in warehouse.execute('SHOW TABLES').rows}
    for view in manifest['training']:
        name, uri = training_identity(manifest, view, env)
        if name not in existing:
            directory = root / 'training' / view
            subprocess.run([env.hadoop, 'fs', '-mkdir', '-p', uri.rsplit('/', 1)[0]], check=True)
            # No overwrite or blind retry of an upload with an uncertain outcome.
            subprocess.run([env.hadoop, 'fs', '-put', str(directory), uri], check=True)
            definition = manifest['dataset_definition']
            spec = definition['views'][view]
            columns = [*spec['features'], *(definition['tasks'][task]['column'] for task in spec['tasks'])]
            sql = ', '.join(f'`{key}` {definition["columns"][key]["type"]}' for key in columns)
            warehouse.execute(f'CREATE EXTERNAL TABLE `{name}` ({sql}) PARTITIONED BY (`split` STRING) '
                              f'STORED AS PARQUET LOCATION {literal(uri)}')
            warehouse.execute(f"ALTER TABLE `{name}` ADD PARTITION (`split`='train') LOCATION {literal(uri + '/split=train')}")
        check_training_table(manifest, view, warehouse, env)
        print(f'Ready training table: {name} ({view})', flush=True)


def training_table(dataset, model, warehouse, env):
    """Reuse the versioned table, checking each view once per loaded dataset."""
    view = model_view(model)
    if view in dataset.training_tables:
        return dataset.training_tables[view]
    if view == 'recall' and dataset.manifest['training'][view]['rows'] < 2:
        raise ValueError('DSSM requires at least two training positives')
    existing = {str(row[1]) for row in warehouse.execute('SHOW TABLES').rows}
    name, _ = training_identity(dataset.manifest, view, env)
    if name not in existing:
        raise ValueError(f'Missing training table; run python -m benchmark.model_quality.prepare --dataset {dataset.name} --upload')
    check_training_table(dataset.manifest, view, warehouse, env)
    dataset.training_tables[view] = name
    return name
