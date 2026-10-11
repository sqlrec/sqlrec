"""One batch probability path for ranking, trees and real MMoE tasks."""
from dataclasses import asdict
import pandas as pd

from .common import literal, write_json
from .config import API_BATCH, Feature, model_parameters, view_fields
from .data import fields, records


def output_columns(model, tasks):
    if model == 'mmoe':
        return {task.id: 'probs_' + task.column for task in tasks}
    return {task.id: 'probs_light' if model == 'rocket_launching' else 'probs' for task in tasks}


def run_ranking(dataset, model, table, resources, api, save_predictions=False):
    features, tasks = view_fields(dataset.definition, model)
    params = model_parameters(model, features, tasks, dataset.train, dataset.items, resources.env, dataset.name)
    write_json(resources.root / 'config.json', {'parameters': params, 'features': [asdict(f) for f in features],
                                              'tasks': [asdict(t) for t in tasks]})
    resources.define(features, tasks, params, dataset.definition)
    checkpoints = resources.train_export(table)
    services = resources.deploy(checkpoints)
    function = resources.name + '_score'
    columns = [Feature('row_id', 'STRING'), *features]
    outputs = output_columns(model, tasks)
    result_columns = ['row_id', *outputs.values()]
    # Row IDs pass through call_service, so responses can be aligned by identity.
    for sql in [f'CREATE OR REPLACE SQL FUNCTION `{function}`',
                f'DEFINE INPUT TABLE observations ({fields(columns)})',
                f'CACHE TABLE scores AS CALL call_service({literal(services["scalar"])}, observations)',
                f'CACHE TABLE predictions AS SELECT {", ".join(result_columns)} FROM scores',
                'RETURN predictions']:
        resources.execute(sql)
    if any(row[0] == function for row in resources.execute('SHOW APIS').rows):
        raise ValueError('API name already exists')
    resources.execute(f'CREATE OR REPLACE API `{function}` WITH `{function}`')
    observations = dataset.ranking_sample()
    predictions = []
    with resources.stage('predict'):
        for start in range(0, len(observations), API_BATCH):
            batch = observations.iloc[start:start + API_BATCH]
            rows = api.call(function, {'data': {'observations': records(batch[[f.name for f in columns]])}})
            expected = set(batch.row_id)
            if len(rows) != len(batch) or {row.get('row_id') for row in rows} != expected:
                raise ValueError('Batch prediction IDs are missing or duplicated')
            predictions.extend({key: row[key] for key in result_columns} for row in rows)
    if save_predictions:
        pd.DataFrame(predictions, columns=result_columns).to_parquet(resources.root / 'predictions.parquet', index=False)
    return observations, predictions, tasks, outputs
