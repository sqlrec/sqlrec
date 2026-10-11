"""DSSM through public SQL, with a complete catalog and per-query Top-50."""
from dataclasses import asdict
import json
import os
import time

import numpy as np
import pandas as pd
import requests

from .common import literal, write_json
from .config import ITEM_BATCH, K, KUAIRAND_DSSM_PARAMS, KUAIRAND_POPULARITY_WEIGHT, Feature, model_parameters, view_fields
from .data import fields, options, records


def sql_value(value, typ):
    expression_type = {'STRING': 'VARCHAR', 'INT': 'INTEGER'}.get(typ, typ)
    if typ.startswith('ARRAY<'):
        inner = typ[6:-1]
        element = {'STRING': 'VARCHAR', 'INT': 'INTEGER'}.get(inner, inner)
        expression_type = element + ' ARRAY'
    if value is None:
        return f'CAST(NULL AS {expression_type})'
    if typ.startswith('ARRAY<'):
        array = 'ARRAY[' + ', '.join(sql_value(item, inner) for item in value) + ']' if value else (
            f'ARRAY(SELECT CAST(NULL AS {element}) FROM (VALUES(1)) AS empty_table(v) WHERE FALSE)')
        return f'CAST({array} AS {expression_type})'
    if typ == 'STRING':
        return literal(value)
    if typ == 'BOOLEAN':
        return 'TRUE' if value else 'FALSE'
    if not isinstance(value, (int, float)) or not np.isfinite(value):
        raise ValueError('SQL numeric inputs must be finite')
    return f'CAST({value} AS {expression_type})'


def input_sql(name, columns, rows):
    # This Calcite path loses array aliases in multirow VALUES. Keep SELECT aliases.
    query = '\nUNION ALL\n'.join('SELECT ' + ', '.join(
        f'{sql_value(row[column.name], column.type)} AS `{column.name}`' for column in columns) for row in rows)
    if not rows:
        query = 'SELECT ' + ', '.join(f'{sql_value(None, column.type)} AS `{column.name}`' for column in columns)
        query += ' FROM (VALUES(1)) AS empty_table(v) WHERE FALSE'
    return f'CACHE TABLE `{name}` AS {query}'


class IndexAdmin:
    """Milvus schema/count administration only. Embedding writes use public SQL."""
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()

    def request(self, path, body):
        token = os.environ.get(self.config['token_env'], '')
        response = self.session.post(self.config['url'].rstrip('/') + '/v2/vectordb/' + path,
                                     headers={'Authorization': 'Bearer ' + token} if token else {}, json=body, timeout=30)
        response.raise_for_status()
        result = response.json()
        if result.get('code') != 0:
            raise ValueError(f'Milvus rejected {path} (code {result.get("code")})')
        return result.get('data')

    def provision(self, name, dimension):
        if name in self.request('collections/list', {}):
            raise ValueError('Collection name already exists')
        self.request('collections/create', {
            'collectionName': name,
            'schema': {'autoId': False, 'enabledDynamicField': False, 'fields': [
                {'fieldName': 'id', 'dataType': 'Int64', 'isPrimary': True},
                {'fieldName': 'item_id', 'dataType': 'VarChar', 'elementTypeParams': {'max_length': 512}},
                {'fieldName': 'embedding', 'dataType': 'FloatVector', 'elementTypeParams': {'dim': str(dimension)}},
                {'fieldName': 'log_popularity', 'dataType': 'Double'}]},
            'indexParams': [{'fieldName': 'embedding', 'metricType': 'IP', 'indexName': 'embedding', 'indexType': 'FLAT'}]})

    def wait_count(self, name, expected):
        deadline = time.monotonic() + 120
        while True:
            data = self.request('entities/query', {'collectionName': name, 'filter': 'id >= 0',
                                                  'outputFields': ['count(*)'], 'consistencyLevel': 'Strong'})
            count = int(data[0]['count(*)'])
            if count == expected:
                return
            if count > expected or time.monotonic() >= deadline:
                raise ValueError(f'Catalog count is {count}, expected {expected}')
            time.sleep(1)


def query_inputs(dataset):
    queries, observations = dataset.retrieval_sample()
    positive = observations[observations.label == 1].groupby('query_id').video_id.agg(lambda values: sorted(set(values)))
    seen = {}
    if dataset.definition['protocol']['exclude_seen']:
        seen = dataset.train.groupby('user_id').video_id.agg(lambda values: sorted(set(values))).to_dict()
    allowed = None
    if dataset.definition['protocol']['candidate_scope'] == 'observed_only':
        allowed = observations.groupby('query_id').video_id.agg(lambda values: sorted(set(values)))
    inputs = []
    for row in records(queries):
        query = {'query_id': row['query_id'], 'features': row,
                 'seen': seen.get(row['user_id'], []), 'targets': positive.get(row['query_id'], [])}
        if allowed is not None:
            query['allowed'] = allowed.get(row['query_id'], [])
        inputs.append(query)
    return inputs


def run_retrieval(dataset, table, resources, api, save_predictions=False):
    features, tasks = view_fields(dataset.definition, 'dssm')
    params = model_parameters('dssm', features, tasks, dataset.train, dataset.items, resources.env, dataset.name)
    if dataset.name == 'kuairand':
        params.update(KUAIRAND_DSSM_PARAMS)
    popularity_weight = KUAIRAND_POPULARITY_WEIGHT if dataset.name == 'kuairand' else 0.0
    write_json(resources.root / 'config.json', {'parameters': params, 'features': [asdict(f) for f in features],
                                              'tasks': [asdict(t) for t in tasks],
                                              'retrieval': {'popularity_weight': popularity_weight}})
    if len(dataset.items) > 16384:
        raise ValueError('This complete-catalog SQL recipe supports at most 16384 items')
    resources.define(features, tasks, params, dataset.definition)
    checkpoints = resources.train_export(table, dssm=True)
    services = resources.deploy(checkpoints)
    vector = resources.name + '_vectors'
    admin = IndexAdmin(resources.env.endpoints['milvus'])
    try:
        with resources.stage('item_load'):
            admin.provision(vector, params['output_dim'])
            milvus = resources.env.endpoints['milvus']
            connector = dict(connector='milvus', url=milvus['url'], database='default', collection=vector, token='')
            if os.environ.get(milvus['token_env']):
                connector['token'] = '{{env:' + milvus['token_env'] + '}}'
            resources.execute(f'CREATE TABLE `{vector}` (id BIGINT, item_id STRING, embedding ARRAY<DOUBLE>, log_popularity DOUBLE, '
                              f'PRIMARY KEY (id) NOT ENFORCED) WITH ({options(connector)})')
            item_features = [feature for feature in features if feature.role == 'item']
            columns = [Feature('catalog_id', 'BIGINT'), Feature('mq_item_identity', 'STRING'),
                       Feature('mq_log_popularity', 'DOUBLE'), *item_features]
            for start in range(0, len(dataset.items), ITEM_BATCH):
                frame = dataset.items.iloc[start:start + ITEM_BATCH]
                rows = [{'catalog_id': start + index + 1, 'mq_item_identity': row['video_id'],
                         'mq_log_popularity': float(row['item_positive_count']),
                         **{feature.name: row[feature.name] for feature in item_features}}
                        for index, row in enumerate(records(frame))]
                resources.execute(input_sql('catalog_input', columns, rows))
                resources.execute(f'CACHE TABLE catalog_emb AS CALL call_service({literal(services["item"])}, catalog_input)')
                vectors = resources.execute('SELECT catalog_id, item_tower_emb FROM catalog_emb').records()
                if len(vectors) != len(rows) or {row['catalog_id'] for row in vectors} != {row['catalog_id'] for row in rows}:
                    raise ValueError('Missing or duplicate item embeddings')
                for row in vectors:
                    # HS2 represents array columns as JSON strings; SQL API uses arrays.
                    embedding = row['item_tower_emb']
                    value = np.asarray(json.loads(embedding) if isinstance(embedding, str) else embedding, dtype=float)
                    if value.shape != (params['output_dim'],) or not np.isfinite(value).all():
                        raise ValueError('Invalid item embedding dimensions/values')
                resources.execute(f'INSERT INTO `{vector}` SELECT catalog_id, mq_item_identity, item_tower_emb, mq_log_popularity FROM catalog_emb')
            admin.wait_count(vector, len(dataset.items))
    finally:
        admin.session.close()
    function = resources.name + '_recall'
    query_columns = [Feature('query_id', 'STRING')] + [feature for feature in features if feature.role == 'user']
    observed = dataset.definition['protocol']['candidate_scope'] == 'observed_only'
    statements = [f'CREATE OR REPLACE SQL FUNCTION `{function}`',
                  f'DEFINE INPUT TABLE query_input ({fields(query_columns)})',
                  'DEFINE INPUT TABLE seen (item_id STRING)']
    if observed:
        statements.append('DEFINE INPUT TABLE allowed (item_id STRING)')
    # Retain the entire candidate budget, including item_id tie sorting and observed filtering.
    statements.extend([f'CACHE TABLE q AS CALL call_service({literal(services["user"])}, query_input)',
                       f'CACHE TABLE candidates AS SELECT q.query_id, v.item_id, ip(q.user_tower_emb, v.embedding) AS score, v.log_popularity '
                       f'FROM q JOIN `{vector}` v ON 1=1 ORDER BY ip(q.user_tower_emb, v.embedding) DESC LIMIT {max(K, len(dataset.items))}'])
    if observed:
        statements.append('CACHE TABLE eligible AS SELECT c.* FROM candidates c JOIN allowed a ON c.item_id=a.item_id')
    source = 'eligible' if observed else 'candidates'
    # Prepared item_positive_count is log1p(clicks) from train only. Re-rank the
    # complete catalog, so low-similarity items with a strong prior remain eligible.
    score = f'unseen.score + {popularity_weight} * unseen.log_popularity' if popularity_weight else 'unseen.score'
    statements.extend([f"CACHE TABLE unseen AS CALL dedup({source}, seen, 'item_id', 'item_id')",
                       f'CACHE TABLE recommendations AS SELECT query_id, item_id, {score} AS score '
                       f'FROM unseen ORDER BY {score} DESC, unseen.item_id LIMIT {K}',
                       'RETURN recommendations'])
    for sql in statements:
        resources.execute(sql)
    if any(row[0] == function for row in resources.execute('SHOW APIS').rows):
        raise ValueError('API name already exists')
    resources.execute(f'CREATE OR REPLACE API `{function}` WITH `{function}`')
    queries = query_inputs(dataset)
    responses = {}
    with resources.stage('predict'):
        for query in queries:
            data = {'query_input': [{feature.name: query['features'][feature.name] for feature in query_columns}],
                    'seen': [{'item_id': item} for item in query['seen']]}
            if observed:
                data['allowed'] = [{'item_id': item} for item in query['allowed']]
            rows = api.call(function, {'data': data})
            responses[query['query_id']] = rows
    if save_predictions:
        # A null item/score row preserves the identity of an empty response.
        rows = [row for query_id, result in responses.items()
                for row in (result or [{'query_id': query_id, 'item_id': None, 'score': None}])]
        pd.DataFrame(rows, columns=['query_id', 'item_id', 'score']).to_parquet(resources.root / 'predictions.parquet', index=False)
    return queries, responses
