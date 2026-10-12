"""DSSM through public SQL, with a complete catalog and per-query Top-50."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
import os
import threading
import time

import numpy as np
import pandas as pd
import requests

from .client import APIClient
from .common import literal, write_json
from .config import ITEM_BATCH, RECALL_WORKERS, K, Feature, model_parameters, view_fields
from .data import fields, options, records


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
                {'fieldName': 'embedding', 'dataType': 'FloatVector', 'elementTypeParams': {'dim': str(dimension)}}]},
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
    write_json(resources.root / 'config.json', {'parameters': params, 'features': [asdict(f) for f in features],
                                              'tasks': [asdict(t) for t in tasks],
                                              'retrieval': {'score': 'inner_product'}})
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
            resources.execute(f'CREATE TABLE `{vector}` (\n    id BIGINT,\n    item_id STRING,\n'
                              f'    embedding ARRAY<DOUBLE>,\n'
                              f'    PRIMARY KEY (id) NOT ENFORCED\n)\nWITH ({options(connector)})')
            item_features = [feature for feature in features if feature.role == 'item']
            columns = [Feature('catalog_id', 'BIGINT'), Feature('mq_item_identity', 'STRING'), *item_features]
            loader = resources.name + '_load_items'
            for sql in [f'CREATE OR REPLACE SQL FUNCTION `{loader}`',
                        f'DEFINE INPUT TABLE catalog_input ({fields(columns)})',
                        f'CACHE TABLE catalog_emb AS\nCALL call_service({literal(services["item"])}, catalog_input)',
                        f'INSERT INTO `{vector}`\nSELECT\n    catalog_id,\n    mq_item_identity,\n'
                        '    item_tower_emb\nFROM catalog_emb',
                        'CACHE TABLE loaded AS\nSELECT\n    catalog_id,\n    item_tower_emb\nFROM catalog_emb',
                        'RETURN loaded',
                        f'CREATE OR REPLACE API `{loader}`\nWITH `{loader}`']:
                resources.execute(sql)
            for start in range(0, len(dataset.items), ITEM_BATCH):
                frame = dataset.items.iloc[start:start + ITEM_BATCH]
                rows = [{'catalog_id': start + index + 1, 'mq_item_identity': row['video_id'],
                         **{feature.name: row[feature.name] for feature in item_features}}
                        for index, row in enumerate(records(frame))]
                vectors = api.call(loader, {'data': {'catalog_input': rows}}, retry=False)
                if len(vectors) != len(rows) or {row['catalog_id'] for row in vectors} != {row['catalog_id'] for row in rows}:
                    raise ValueError('Missing or duplicate item embeddings')
                for row in vectors:
                    # HS2 represents array columns as JSON strings; SQL API uses arrays.
                    embedding = row['item_tower_emb']
                    value = np.asarray(json.loads(embedding) if isinstance(embedding, str) else embedding, dtype=float)
                    if value.shape != (params['output_dim'],) or not np.isfinite(value).all():
                        raise ValueError('Invalid item embedding dimensions/values')
            admin.wait_count(vector, len(dataset.items))
    finally:
        admin.session.close()
    function = resources.name + '_recall'
    query_columns = [Feature('query_id', 'STRING')] + [feature for feature in features if feature.role == 'user']
    observed = dataset.definition['protocol']['candidate_scope'] == 'observed_only'
    statements = [f'CREATE OR REPLACE SQL FUNCTION `{function}`',
                  f'DEFINE INPUT TABLE query_input ({fields(query_columns)})',
                  'DEFINE INPUT TABLE seen (\n    item_id STRING\n)']
    if observed:
        statements.append('DEFINE INPUT TABLE allowed (\n    item_id STRING\n)')
    # Retain the entire candidate budget, including item_id tie sorting and observed filtering.
    statements.extend([f'CACHE TABLE q AS\nCALL call_service({literal(services["user"])}, query_input)',
                       'CACHE TABLE candidates AS\nSELECT\n    q.query_id,\n    v.item_id,\n'
                       '    ip(q.user_tower_emb, v.embedding) AS score\n'
                       f'FROM q\nJOIN `{vector}` v ON 1 = 1\n'
                       f'ORDER BY ip(q.user_tower_emb, v.embedding) DESC\nLIMIT {max(K, len(dataset.items))}'])
    if observed:
        statements.append('CACHE TABLE eligible AS\nSELECT\n    c.*\nFROM candidates c\nJOIN allowed a ON c.item_id = a.item_id')
    source = 'eligible' if observed else 'candidates'
    statements.extend([f"CACHE TABLE unseen AS\nCALL dedup({source}, seen, 'item_id', 'item_id')",
                       f'CACHE TABLE recommendations AS\nSELECT\n    query_id,\n    item_id,\n    unseen.score AS score\n'
                       f'FROM unseen\nORDER BY\n    unseen.score DESC,\n    unseen.item_id\nLIMIT {K}',
                       'RETURN recommendations'])
    for sql in statements:
        resources.execute(sql)
    if any(row[0] == function for row in resources.execute('SHOW APIS').rows):
        raise ValueError('API name already exists')
    resources.execute(f'CREATE OR REPLACE API `{function}`\nWITH `{function}`')
    queries = query_inputs(dataset)
    with resources.stage('predict'):
        responses = predict_queries(queries, query_columns, observed, function, api.config)
    if save_predictions:
        # A null item/score row preserves the identity of an empty response.
        rows = [row for query_id, result in responses.items()
                for row in (result or [{'query_id': query_id, 'item_id': None, 'score': None}])]
        pd.DataFrame(rows, columns=['query_id', 'item_id', 'score']).to_parquet(resources.root / 'predictions.parquet', index=False)
    return queries, responses


def predict_queries(queries, columns, observed, function, api_config):
    """Independent per-query Top-K requests, with one HTTP session per worker."""
    local = threading.local()
    clients = []

    def initialize():
        local.api = APIClient(api_config)
        clients.append(local.api)

    def predict(query):
        data = {'query_input': [{f.name: query['features'][f.name] for f in columns}],
                'seen': [{'item_id': item} for item in query['seen']]}
        if observed:
            data['allowed'] = [{'item_id': item} for item in query['allowed']]
        return query['query_id'], local.api.call(function, {'data': data})

    try:
        with ThreadPoolExecutor(max_workers=RECALL_WORKERS, initializer=initialize) as pool:
            # map preserves input order, including queries with empty responses.
            return dict(pool.map(predict, queries))
    finally:
        for client in clients:
            client.close()
