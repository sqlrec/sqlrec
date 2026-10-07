"""Generate the exact public SQL statements executed by the lifecycle runner."""
from dataclasses import asdict
from pathlib import Path

from ..common import digest, write_json
from .datasets import prepare
from .specs import Feature, ModelRecipe, SQLStep, Task, WorkflowPlan, identifier, kubernetes_name, literal, model_name, validate_config


def fields(features):
    return ', '.join(f'`{f.name}` {f.type}' for f in features)


def options(params):
    return ', '.join(f'{literal(k)}={literal(v)}' for k, v in sorted(params.items()))


def sql_value(value, typ):
    expression_type = {'STRING': 'VARCHAR', 'INT': 'INTEGER'}.get(typ, typ)
    if typ.startswith('ARRAY<'):
        inner = typ[6:-1]
        expression_type = {'STRING': 'VARCHAR', 'INT': 'INTEGER'}.get(inner, inner) + ' ARRAY'
    if value is None:
        return f'CAST(NULL AS {expression_type})'
    if typ.startswith('ARRAY<'):
        inner = typ[6:-1]
        constructor = ('ARRAY[' + ', '.join(sql_value(v, inner) for v in value) + ']') if value else (
            f'ARRAY(SELECT CAST(NULL AS {expression_type[:-6]}) FROM (VALUES(1)) AS empty_table(v) WHERE FALSE)')
        return f'CAST({constructor} AS {expression_type})'
    if typ == 'STRING':
        return literal(value)
    if typ == 'BOOLEAN':
        if not isinstance(value, bool):
            raise ValueError('BOOLEAN input requires bool')
        return 'TRUE' if value else 'FALSE'
    import math
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('Numeric SQL inputs must be finite numbers')
    return f'CAST({value} AS {expression_type})'


def input_sql(name, columns, rows):
    """Typed VALUES also handles empty tables, without phantom observations."""
    identifier(name)
    if not rows:
        select = ', '.join(f'{sql_value(None, f.type)} AS `{f.name}`' for f in columns)
        query = f'SELECT {select} FROM (VALUES(1)) AS empty_table(v) WHERE FALSE'
    else:
        # Multi-row VALUES with array expressions loses aliases in the deployed
        # Calcite path. Explicit SELECT/UNION ALL retains public function schema.
        query = '\nUNION ALL\n'.join('SELECT ' + ', '.join(
            sql_value(r[f.name], f.type) + f' AS `{f.name}`' for f in columns) for r in rows)
    return f'CACHE TABLE `{name}` AS {query};'


def build(config, output):
    output = Path(output)
    if output.exists() and any(output.iterdir()):
        raise FileExistsError('Build needs a new empty run directory')
    config = validate_config(config)
    prefix = config['name']
    table_prefix = prefix if len(prefix) <= 100 else model_name(prefix, 'data', 0)
    seed = config['execution']['seed']
    features = [Feature(**f) for f in config['features']]
    tasks = [Task(**t) for t in config['tasks']]
    serving = config['serving']; batch = serving['batch_size']
    ks = config['evaluation']['ks']
    bundle, hashes = prepare(config, output)
    # prepare may resolve categorical bucket sizes from the frozen data cohort.
    config = validate_config(config)
    models = [ModelRecipe(**m) for m in config['models']]
    write_json(output / 'resolved_config.json', config)
    hashes['resolved_config.json'] = digest(output / 'resolved_config.json')
    steps, evaluations = [], []
    identity = {}

    def step(name, statements, endpoint='sqlrec', replay='safe', checks=None, capture=None,
             kind='sql', result_roles=None):
        identifier(name); paths = []
        for i, statement in enumerate(statements):
            path = f'sql/{len(steps):03d}_{name}_{i:02d}.sql'
            target = output / path; target.parent.mkdir(exist_ok=True)
            target.write_text(statement.rstrip() + '\n')
            hashes[path] = digest(target); paths.append(path)
        roles = result_roles or ['sql'] * len(paths)
        if capture and capture['kind'] in ('scalar', 'recall', 'vectors', 'table_count'):
            capture['result_role'] = 'vectors' if capture['kind'] == 'vectors' else (
                'table_count' if capture['kind'] == 'table_count' else 'prediction')
            if paths and result_roles is None:
                roles[-1] = capture['result_role']
        if len(roles) != len(paths):
            raise ValueError('Every SQL statement must have a declared result role')
        steps.append(SQLStep(name, endpoint, paths, replay, checks or [], capture,
                             kind=kind, result_roles=roles, **identity))

    # Hive data access and model definitions share the same frozen Parquet schema.
    label_features = [Feature(t.column, t.data_type) for t in tasks]
    for table in ('samples', 'positive') if bundle.get('items') else ('samples',):
        name = identifier(table_prefix + '_' + table)
        uri = config['dataset']['storage_uri'].rstrip('/') + f'/{table}'
        table_contract = {'kind': 'table_schema', 'name': name,
                          'columns': {f.name: f.type for f in features + label_features},
                          'partitions': {'split': 'STRING'}, 'location': uri}
        step('table_' + table, [f'CREATE EXTERNAL TABLE `{name}` ({fields(features + label_features)}) '
                               f'PARTITIONED BY (`split` STRING) STORED AS PARQUET LOCATION {literal(uri)};'],
             'warehouse', 'never', capture=table_contract, kind='table_create')
        step('partition_' + table, [f'ALTER TABLE `{name}` ADD IF NOT EXISTS PARTITION (`split`=\'train\') '
                                   f'LOCATION {literal(uri + "/split=train")};'], 'warehouse', 'safe',
             capture={**table_contract, 'kind': 'table_partition', 'partition': {'split': 'train'},
                      'partition_location': uri + '/split=train'}, kind='table_partition')
        count = bundle['positive_train_rows'] if table == 'positive' else bundle['train_rows']
        step('verify_' + table, [f'SELECT COUNT(*) AS row_count FROM `{name}` WHERE `split`=\'train\';'],
             'warehouse', capture={'kind': 'table_count', 'table': table, 'expected_rows': count}, kind='verify_table')
    resource = config['execution']['resources']
    service_resource = config['execution'].get('service_resources', resource)
    cleanup = []; resources = {'models': {}}
    for model in models:
        name = model_name(prefix, model.id, seed)
        resources['models'][name] = {'services': [], 'jobs': [
            {'name': kubernetes_name(name + '-v1') + '-job', 'namespace': resource['NAMESPACE'], 'kind': 'train'},
            {'name': kubernetes_name(name + '-v1-export') + '-job', 'namespace': resource['NAMESPACE'], 'kind': 'export'}]}
        identity = {'model_id': name, 'recipe_id': model.id, 'seed': seed}
        params = model.parameters(features, tasks, seed)
        step(name + '_define', [f'CREATE MODEL `{name}` ({fields(features + label_features)}) WITH ({options(params)});'],
             replay='verify', checks=[{'kind': 'model', 'name': name, 'params': params}], kind='define')
        table = table_prefix + ('_positive' if model.family == 'tzrec.dssm' else '_samples')
        # These isolated tables expose train partitions only. No held-out
        # partition is mounted, so ON the table is the complete train view.
        step(name + '_train', [f'TRAIN MODEL `{name}` CHECKPOINT=\'v1\' ON `{table}` '
                              f'WITH ({options(resource)});'], replay='verify',
             checks=[{'kind': 'checkpoint', 'name': name, 'checkpoint': 'v1', 'type': 'origin'}], kind='train')
        exports = ['v1_export/user', 'v1_export/item'] if model.family == 'tzrec.dssm' else ['v1_export']
        step(name + '_export', [f'EXPORT MODEL `{name}` CHECKPOINT=\'v1\' ON `{table}` '
                               f'WITH ({options(resource)});'], replay='verify',
             checks=[{'kind': 'checkpoint', 'name': name, 'checkpoint': ck, 'type': 'export'} for ck in exports], kind='export')
        services = {}
        for ck in exports:
            tower = ck.split('/')[-1] if '/' in ck else 'scalar'
            service = identifier(name + '_' + tower); services[tower] = service
            step(service + '_service', [f'CREATE SERVICE `{service}` ON MODEL `{name}` '
                                       f'CHECKPOINT={literal(ck)} WITH ({options(service_resource)});'], replay='verify',
                 checks=[{'kind': 'service', 'name': service, 'model': name, 'checkpoint': ck}], kind='service')
            resources['models'][name]['services'].append({
                'name': service, 'model': name, 'checkpoint': ck, 'step_id': service + '_service',
                'namespace': service_resource['NAMESPACE'], 'deployment': kubernetes_name(service)})
            cleanup.append(f'DROP SERVICE `{service}`;')
        fn = identifier(name + '_score')
        cols = [Feature('row_id', 'STRING')] + features
        statements = [f'CREATE OR REPLACE SQL FUNCTION `{fn}`;', f'DEFINE INPUT TABLE observations ({fields(cols)});']
        if model.family == 'tzrec.dssm':
            statements += [f'CACHE TABLE u AS CALL call_service({literal(services["user"])}, observations);',
                           f'CACHE TABLE i AS CALL call_service({literal(services["item"])}, observations);',
                           'CACHE TABLE scores AS SELECT u.row_id, ip(u.user_tower_emb, i.item_tower_emb) AS score '
                           'FROM u JOIN i ON u.row_id=i.row_id;', 'RETURN scores;']
            outputs = {tasks[0].id: 'score'}
        else:
            outputs = model.outputs(tasks)
            statements += [f'CACHE TABLE scores AS CALL call_service({literal(services["scalar"])}, observations);',
                           'RETURN scores;']
        step(fn + '_function', statements, kind='function')
        if serving['entrypoint'] == 'sql_api':
            step(fn + '_api', [f'CREATE OR REPLACE API `{fn}` WITH `{fn}`;'], kind='api')
            cleanup.append(f'DROP API `{fn}`;')
        captures = []
        for b in range(0, len(bundle['scalar_inputs']), batch):
            rows = bundle['scalar_inputs'][b:b+batch]
            req = {'data': {'observations': rows}}
            path = f'requests/{name}_score_{b:07d}.json'
            write_json(output / path, req); hashes[path] = digest(output / path)
            sid = identifier(name + f'_score_{b}')
            statements = [input_sql('observations', cols, rows), f'CALL `{fn}`(observations);'] if serving['entrypoint'] == 'sql' else []
            step(sid, statements, capture={'kind': 'scalar', 'request': path, 'api': fn, 'outputs': outputs,
                                          'probability_columns':[outputs[t.id] for t in tasks if t.type=='binary']
                                          if model.family!='tzrec.dssm' else [],
                                          'row_ids': [r['row_id'] for r in rows]}, kind='scalar')
            captures.append(sid)
        evaluation = {'id': name, 'model': model.family, 'seed': seed, 'outputs': outputs,
                      'probability': model.family != 'tzrec.dssm', 'scalar_steps': captures, 'recipe_id': model.id}
        if model.family == 'tzrec.dssm':
            vector = identifier(name + '_vectors'); dim = int(params.get('output_dim', 64))
            index = {'collection': vector, 'dimension': dim, 'metric': 'IP', 'index_type': 'FLAT',
                     'expected_rows': len(bundle['items'])}
            path = f'infra/{vector}.json'; write_json(output / path, index); hashes[path] = digest(output / path)
            step(vector + '_ready', [], 'infra', capture={'kind': 'index_empty', 'spec': path}, kind='index_empty')
            milvus = config['sql']['endpoints']['milvus']
            connector = {'connector': 'milvus', 'url': milvus['url'], 'database': 'default', 'collection': vector,
                         'token': '{{env:' + milvus['token_env'] + '}}'}
            step(vector + '_table', [f'CREATE TABLE `{vector}` (`id` BIGINT, `item_id` STRING, '
                                    f'`embedding` ARRAY<DOUBLE>, PRIMARY KEY (`id`) NOT ENFORCED) WITH ({options(connector)});'],
                 replay='never', kind='table_create', capture={'kind': 'table_schema', 'name': vector,
                     'columns': {'id': 'BIGINT', 'item_id': 'STRING', 'embedding': 'ARRAY<DOUBLE>'},
                     'partitions': {}, 'properties': connector})
            item_cols = [Feature('catalog_id', 'BIGINT'), Feature('mq_item_identity', 'STRING')] + [f for f in features if f.role == 'item']
            for b in range(0, len(bundle['items']), batch):
                rows = [{'catalog_id': r['catalog_id'], 'mq_item_identity': r['item_id'], **r['features']} for r in bundle['items'][b:b+batch]]
                step(vector + f'_load_{b}', [input_sql('catalog_input', item_cols, rows),
                     f'CACHE TABLE catalog_emb AS CALL call_service({literal(services["item"])}, catalog_input);',
                     'SELECT catalog_id, item_tower_emb FROM catalog_emb;',
                     f'INSERT INTO `{vector}` SELECT catalog_id, mq_item_identity, item_tower_emb FROM catalog_emb;'],
                     capture={'kind': 'vectors', 'dimension': dim, 'rows': len(rows), 'catalog_ids': [r['catalog_id'] for r in rows]}, replay='never',
                     kind='vector_load', result_roles=['input', 'embedding_call', 'vectors', 'mutation'])
            step(vector + '_visible', [], 'infra', capture={'kind': 'index_visible', 'spec': path}, kind='index_visible')
            recall = identifier(name + '_recall')
            query_cols = [Feature('query_id', 'STRING')] + [f for f in features if f.role == 'user']
            statements = [f'CREATE OR REPLACE SQL FUNCTION `{recall}`;',
                          f'DEFINE INPUT TABLE query_input ({fields(query_cols)});',
                          'DEFINE INPUT TABLE seen (item_id STRING);']
            observed_only = config['protocol']['candidate_scope'] == 'observed_only'
            if observed_only:
                statements.append('DEFINE INPUT TABLE allowed (item_id STRING);')
            statements += [
                          f'CACHE TABLE q AS CALL call_service({literal(services["user"])}, query_input);',
                          f'CACHE TABLE candidates AS SELECT q.query_id, v.item_id, ip(q.user_tower_emb, v.embedding) AS score '
                          f'FROM q JOIN `{vector}` v ON 1=1 ORDER BY ip(q.user_tower_emb, v.embedding) DESC LIMIT {serving["candidate_limit"]};']
            if observed_only:
                statements.append('CACHE TABLE eligible_candidates AS SELECT c.* FROM candidates c JOIN allowed a ON c.item_id=a.item_id;')
            source = 'eligible_candidates' if observed_only else 'candidates'
            statements += [f"CACHE TABLE unseen AS CALL dedup({source}, seen, 'item_id', 'item_id');",
                          f'CACHE TABLE recommendations AS SELECT * FROM unseen ORDER BY score DESC, item_id LIMIT {max(ks)};',
                          'RETURN recommendations;']
            step(recall + '_function', statements, kind='function')
            if serving['entrypoint'] == 'sql_api':
                step(recall + '_api', [f'CREATE OR REPLACE API `{recall}` WITH `{recall}`;'], kind='api')
                cleanup.append(f'DROP API `{recall}`;')
            recall_steps = []
            for n, query in enumerate(bundle['queries']):
                qrows = [{'query_id': query['query_id'], **query['features']}]
                seen = [{'item_id': item} for item in query['seen']]
                request = {'data': {'query_input': qrows, 'seen': seen}}
                allowed = [{'item_id': item} for item in query.get('candidates', [])]
                if observed_only:
                    request['data']['allowed'] = allowed
                path = f'requests/{name}_recall_{n:07d}.json'; write_json(output / path, request); hashes[path] = digest(output / path)
                sid = identifier(name + f'_recall_{n}')
                statements = []
                if serving['entrypoint'] == 'sql':
                    statements = [input_sql('query_input', query_cols, qrows), input_sql('seen', [Feature('item_id', 'STRING')], seen)]
                    if observed_only:
                        statements.append(input_sql('allowed', [Feature('item_id', 'STRING')], allowed))
                    statements.append(f'CALL `{recall}`(query_input, seen' + (', allowed' if observed_only else '') + ');')
                step(sid, statements, capture={'kind': 'recall', 'request': path, 'api': recall,
                                              'query_id': query['query_id']}, kind='recall')
                recall_steps.append(sid)
            evaluation['recall_steps'] = recall_steps
        evaluations.append(evaluation); cleanup.append(f'DROP MODEL `{name}`;')
    (output / 'cleanup.sql').write_text(
        '-- Manual cleanup only, after reviewing retained evidence.\n'
        '-- Services are released per model; omit recorded successful DROP SERVICE entries.\n'
        + '\n'.join(cleanup) + '\n')
    plan = WorkflowPlan(prefix, bundle['fingerprint'], config, [asdict(f) for f in features], [asdict(t) for t in tasks],
                        steps, hashes, evaluations, format_version=5, resources=resources).to_dict()
    write_json(output / 'resolved_plan.json', plan)
    return plan
