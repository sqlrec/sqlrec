"""Journaled execution of immutable public SQL/API recipes, with no model fallback."""
from pathlib import Path
from datetime import datetime, timezone
import re
import os

from ..common import captured_result, digest, finite, read_json, stable_id, write_json
from .infrastructure import IndexAdmin, WorkloadAdmin
from .sql_client import APIClient, HS2Client, Indeterminate, SQLFailed, resolve_environment
from .specs import kubernetes_name, literal
from .runtime_audit import RuntimeAudit


def verify_plan(root):
    root = Path(root).resolve(); plan = read_json(root / 'resolved_plan.json')
    value = dict(plan); plan_id = value.pop('plan_id')
    if stable_id(value) != plan_id or plan.get('execution_profile') != 'user_workflow':
        raise ValueError('Resolved plan changed or profile is not formal user_workflow')
    for name, checksum in plan['files'].items():
        path = (root / name).resolve()
        if not path.is_relative_to(root) or digest(path) != checksum:
            raise ValueError(f'Immutable recipe/input changed: {name}')
    if plan.get('format_version') not in (3, 4, 5):
        raise ValueError('Unsupported frozen plan version')
    if plan['format_version'] == 3:
        plan = legacy_plan(plan, root)
    else:
        for step in plan['steps']:
            if len(step.get('result_roles', [])) != len(step['files']):
                raise ValueError('Invalid statement/result-role contract')
            if step['kind'] not in {'sql', 'table_create', 'table_partition', 'verify_table', 'define', 'train', 'export',
                                    'service', 'function', 'api', 'scalar', 'index_empty', 'index_visible',
                                    'vector_load', 'recall'}:
                raise ValueError('Unsupported workflow stage kind')
            if step['replay'] not in ('safe', 'verify', 'never'):
                raise ValueError('Invalid stage replay policy')
            if step['kind'] in ('train', 'export') and not step.get('model_id'):
                raise ValueError('Lifecycle stages need an explicit model identity')
    if plan['format_version'] == 5:
        groups = plan.get('resources', {}).get('models')
        if not isinstance(groups, dict):
            raise ValueError('Missing frozen model resource bindings')
        declared = {s['model_id'] for s in plan['steps'] if s['model_id']}
        if declared != set(groups):
            raise ValueError('Model groups differ from frozen resource bindings')
        physical = set()
        bindings = []
        for model, group in groups.items():
            lifecycle = {s['kind'] for s in plan['steps'] if s['model_id'] == model and s['kind'] in ('train', 'export')}
            if len(group['jobs']) != len(lifecycle) or {b['kind'] for b in group['jobs']} != lifecycle:
                raise ValueError('Incomplete training/export resource contract')
            for binding in group['services']:
                stage = next((s for s in plan['steps'] if s['id'] == binding['step_id']), None)
                if (stage is None or stage['kind'] != 'service' or stage['model_id'] != model or
                    binding['model'] != model or not any(all(c.get(k) == binding[k] for k in
                    ('name', 'model', 'checkpoint')) for c in stage['checks']) or
                    binding['deployment'] != kubernetes_name(binding['name'])):
                    raise ValueError('Service resource binding differs from frozen SQL stage')
                key = ('serving', binding['namespace'], binding['deployment'])
                if key in physical:
                    raise ValueError('Physical serving resource name collision')
                physical.add(key); bindings.append(binding['step_id'])
            for binding in group['jobs']:
                suffix = '-v1' if binding['kind'] == 'train' else '-v1-export'
                if binding['kind'] not in ('train', 'export') or binding['name'] != kubernetes_name(model + suffix) + '-job':
                    raise ValueError('Invalid native Job resource binding')
                key = ('job', binding['namespace'], binding['name'])
                if key in physical:
                    raise ValueError('Physical Job resource name collision')
                physical.add(key)
        if set(bindings) != {s['id'] for s in plan['steps'] if s['kind'] == 'service'}:
            raise ValueError('Incomplete service release contract')
    return plan


def describe_map(result):
    rows = result.rows if hasattr(result, 'rows') else [list(row.values()) for row in result]
    return {str(row[0]).strip(): str(row[1]).strip() for row in rows if len(row) >= 2}


def legacy_plan(plan, root):
    """Adapt v3 metadata in memory after checking the original fingerprint."""
    import copy
    plan = copy.deepcopy(plan)
    recipes = {f'{plan["name"]}_{recipe["id"]}_s{seed}': recipe['id']
               for recipe in plan['config'].get('models', [])
               for seed in plan['config']['execution'].get('seeds', [])}
    evaluations = {e['id']: e for e in plan['evaluations']}
    for evaluation in evaluations.values():
        evaluation['recipe_id'] = recipes[evaluation['id']]
    for step in plan['steps']:
        statements = [(root / path).read_text().lstrip() for path in step['files']]
        first = statements[0] if statements else ''
        kind = ('function' if first.startswith('CREATE OR REPLACE SQL FUNCTION') else
                'train' if first.startswith('TRAIN MODEL') else (step.get('capture') or {}).get('kind', 'sql'))
        step['kind'] = kind
        model = next((c['name'] for c in step['checks'] if c['kind'] == 'checkpoint'), None)
        step['model_id'] = model
        evaluation = evaluations.get(model, {})
        step['recipe_id'] = evaluation.get('recipe_id')
        step['seed'] = evaluation.get('seed')
        step['result_roles'] = ['sql'] * len(statements)
        capture = step.get('capture') or {}
        if capture.get('kind') in ('scalar', 'recall', 'table_count') and statements:
            step['result_roles'][-1] = 'prediction' if capture['kind'] != 'table_count' else 'table_count'
        if capture.get('kind') == 'vectors':
            step['result_roles'][-2] = 'vectors'
    return plan


def validate_capture(capture, results):
    kind = capture['kind']
    if kind == 'table_count':
        rows = captured_result(capture, results)['rows']
        if len(rows) != 1 or rows[0].get('row_count') != capture['expected_rows']:
            raise ValueError('Public training table row count differs from frozen fixture')
    elif kind == 'vectors':
        # Select validation evidence by role; INSERT's summary is not an embedding.
        rows = captured_result(capture, results)['rows']
        if len(rows) != capture['rows'] or len({r['catalog_id'] for r in rows}) != len(rows):
            raise ValueError('Incomplete/duplicate item embeddings')
        if 'catalog_ids' in capture and {r['catalog_id'] for r in rows} != set(capture['catalog_ids']):
            raise ValueError('Item embedding identities differ from the frozen catalog batch')
        for row in rows:
            vec = row.get('item_tower_emb')
            if isinstance(vec, str):
                import json
                vec = json.loads(vec)
            if not isinstance(vec, list) or len(vec) != capture['dimension'] or not all(finite(v) for v in vec):
                raise ValueError('Missing/nonfinite/wrong-dimension tower output')
    elif kind == 'scalar':
        rows = captured_result(capture, results)['rows']
        ids = [r.get('row_id') for r in rows]
        if len(set(ids)) != len(ids) or set(ids) != set(capture['row_ids']):
            raise ValueError('Scalar prediction identities do not exactly cover request')
        for row in rows:
            for column in capture['outputs'].values():
                if not finite(row.get(column)):
                    raise ValueError(f'Missing/null/nonfinite scalar output: {column}')
                if column in capture.get('probability_columns',[]) and not 0 <= row[column] <= 1:
                    raise ValueError(f'Probability output outside [0,1]: {column}')
    elif kind == 'recall':
        rows = captured_result(capture, results)['rows']
        if any(r.get('query_id') != capture['query_id'] or not isinstance(r.get('item_id'), str)
               or not finite(r.get('score')) for r in rows):
            raise ValueError('Malformed recall identity/score')
        if len({r['item_id'] for r in rows}) != len(rows):
            raise ValueError('Duplicate recommendations')


class Runner:
    def __init__(self, root, client_factory=HS2Client, api_factory=APIClient, index_factory=IndexAdmin, workload_factory=WorkloadAdmin):
        self.root = Path(root); self.plan = verify_plan(root)
        milvus = self.plan['config']['sql']['endpoints'].get('milvus', {})
        if milvus.get('token_env'):
            # Empty authentication is valid for a deployment with auth disabled.
            os.environ.setdefault(milvus['token_env'], '')
        self.client_factory, self.api_factory, self.index_factory = client_factory, api_factory, index_factory
        self.formal = client_factory is HS2Client and api_factory is APIClient and index_factory is IndexAdmin and workload_factory is WorkloadAdmin
        self.path = self.root / 'execution/journal.json'
        self.journal = read_json(self.path) if self.path.exists() else {
            'plan_id': self.plan['plan_id'], 'formal_transport': self.formal, 'steps': {}, 'status': 'planned'}
        if self.journal['plan_id'] != self.plan['plan_id'] or self.journal['formal_transport'] != self.formal:
            raise ValueError('Resume plan/transport mismatch')
        package = Path(__file__).parents[1]
        sources = sorted([*package.glob('*.py'), *(package/'user_workflow').glob('*.py')])
        self.journal.setdefault('driver_versions',[]).append({
            'started_at':datetime.now(timezone.utc).isoformat(),
            'source_sha256':{str(p.relative_to(package)):digest(p) for p in sources}})
        self.clients = {}; self.api = None
        self.runtime_audit = RuntimeAudit(self.root,self.plan)
        self.workloads = workload_factory(self.plan['config']['execution'])
        for model in self.plan.get('resources', {}).get('models', {}):
            self.journal.setdefault('models', {}).setdefault(model, {'status': 'not_executed', 'cleanup_status': 'not_required'})

    def audit_train(self, step):
        if step['kind'] != 'train' or not self.runtime_audit.config:
            return
        recipe = next(r for r in self.plan['config']['models'] if r['id'] == step['recipe_id'])
        return self.runtime_audit.checkpoint(step['model_id'], recipe, step['seed'])

    def save(self):
        self.journal['updated_at'] = datetime.now(timezone.utc).isoformat()
        write_json(self.path, self.journal)

    def client(self, endpoint):
        if endpoint in self.clients and getattr(self.clients[endpoint], 'transport_broken', False):
            self.clients.pop(endpoint)
        if endpoint not in self.clients:
            self.clients[endpoint] = self.client_factory(self.plan['config']['sql']['endpoints'][endpoint]).connect()
            if endpoint == 'sqlrec':
                # Normal public connector DDL requires the Flink/default dialect.
                sql = 'SET table.sql-dialect = default'
                result = self.clients[endpoint].execute(sql)
                write_json(self.root / 'execution/session_sqlrec.json',
                           {'sql': sql, 'source': result.source, 'rows': result.records()})
        return self.clients[endpoint]

    def reconcile_table(self, step):
        """Verify frozen schema, connector options and warehouse locations."""
        statement = (self.root / step['files'][0]).read_text()
        contract = step.get('capture') or {}
        match = re.match(r'CREATE (?:EXTERNAL )?TABLE `([a-z0-9_]+)`', statement)
        name = contract.get('name') or (match[1] if match else None)
        if not name:
            raise ValueError('No frozen table identity available for reconciliation')
        client = self.client(step['endpoint'])
        def normalize(typ):
            typ = re.sub(r'NOT NULL|\s+', '', typ.upper())
            typ = re.sub(r'VARCHAR(?:\(\d+\))?', 'STRING', typ)
            typ = typ.replace('INTEGER', 'INT')
            return 'ARRAY<' + typ[:-5] + '>' if typ.endswith('ARRAY') else typ
        schema = client.execute(f'DESCRIBE `{name}`').records()
        actual = {}
        for row in schema:
            column = str(row.get('name', row.get('col_name', ''))).strip()
            typ = row.get('type', row.get('data_type', ''))
            if column and not column.startswith('#') and typ:
                value = normalize(str(typ))
                if column in actual and actual[column] != value:
                    raise ValueError('Ambiguous existing table schema')
                actual[column] = value
        wanted = contract.get('columns')
        if wanted is None:
            wanted = dict(re.findall(r'`([a-z0-9_]+)`\s+(ARRAY<[^>]+>|BIGINT|INT|STRING|FLOAT|DOUBLE|BOOLEAN)', statement))
        wanted = {k:normalize(v) for k,v in {**wanted, **contract.get('partitions', {})}.items()}
        if actual != wanted:
            raise ValueError('Existing table schema differs from immutable contract')
        def params(text):
            return {k.replace("''", "'"):v.replace("''", "'") for k,v in
                    re.findall(r"'((?:''|[^'])*)'\s*=\s*'((?:''|[^'])*)'", text)}
        properties = contract.get('properties')
        if properties is not None or step['endpoint'] != 'warehouse':
            ddl = client.execute(f'SHOW CREATE TABLE `{name}`').rows[0][0]
            expected = params(resolve_environment(statement)); values = params(ddl)
            if any(values.get(k) != v for k,v in expected.items()):
                raise ValueError('Existing Connector properties differ from immutable CREATE SQL')
        if contract.get('location'):
            suffix = ''
            expected_location = contract['location']
            if contract['kind'] == 'table_partition':
                suffix = ' PARTITION (' + ', '.join(f'`{k}`={literal(v)}' for k,v in contract['partition'].items()) + ')'
                expected_location = contract['partition_location']
            metadata = client.execute(f'DESCRIBE FORMATTED `{name}`' + suffix).rows
            # Spark returns the partition section followed by table storage;
            # both contain a Location row. Never flatten these into one map.
            partition = contract['kind'] == 'table_partition'
            section = False; partition_section = False; locations = []; all_locations = []
            for row in metadata:
                key = str(row[0]).strip() if row else ''
                if key.startswith('#'):
                    section = key == '# Detailed Partition Information'
                    partition_section |= section
                elif key.rstrip(':') == 'Location' and len(row) > 1 and row[1] is not None:
                    value = str(row[1]).strip().rstrip('/')
                    all_locations.append(value)
                    if section:
                        locations.append(value)
            locations = locations if partition and partition_section else all_locations
            if set(locations) != {resolve_environment(expected_location).rstrip('/')}:
                raise ValueError('Existing warehouse location differs from immutable contract')
        return {'source':'public_sql', 'reconciled':True, 'table':name, 'schema':schema}

    def checks(self, step, timeout=None, completion=False, allow_failed_checkpoint=False):
        proofs = []
        client = self.client(step['endpoint'])
        for check in step['checks']:
            if check['kind'] == 'service':
                sql = f'DESCRIBE FORMATTED SERVICE `{check["name"]}`;'
            else:
                sql = f'DESCRIBE FORMATTED MODEL `{check["name"]}`'
                if check['kind'] == 'checkpoint':
                    sql += ' CHECKPOINT=' + literal(check['checkpoint'])
                sql += ';'
            result = client.execute(sql, timeout=timeout or self.plan['config']['execution'].get('operation_timeout', 3600))
            meta = describe_map(result)
            if check['kind'] == 'checkpoint':
                terminal = ('succeeded', 'failed') if allow_failed_checkpoint else ('succeeded',)
                if meta.get('Status:') not in terminal or meta.get('Checkpoint Type:') != check['type']:
                    raise Indeterminate('Checkpoint has no public succeeded/type completion proof')
            elif check['kind'] == 'service':
                if meta.get('Model Name:') != check['model'] or meta.get('Checkpoint Name:') != check['checkpoint'] or not meta.get('URL:'):
                    raise ValueError('Public service binding mismatch')
                if completion and self.plan['format_version'] == 5:
                    binding = next(b for b in self._service_bindings(step['model_id']) if b['name'] == check['name'])
                    deployment = self.workloads.get('deployment', binding['namespace'], binding['deployment'])
                    if not deployment or deployment.get('status', {}).get('availableReplicas', 0) < deployment.get('spec', {}).get('replicas', 1):
                        raise Indeterminate('Service metadata alone cannot prove deployment completion')
            else:
                # Exact parameter comparison prevents claiming ignored configuration as verified.
                if any(meta.get(k) != v for k, v in check['params'].items()):
                    raise ValueError('Public model parameters differ from submitted recipe')
            proofs.append({'sql': sql, 'rows': result.records(), 'source': result.source})
        return proofs

    def preflight(self):
        requested = {'SHOW MODELS': set(), 'SHOW SERVICES': set(), 'SHOW SQL FUNCTIONS': set(), 'SHOW APIS': set()}
        for step in self.plan['steps']:
            for check in step['checks']:
                if check['kind'] in ('model', 'service'):
                    requested['SHOW MODELS' if check['kind'] == 'model' else 'SHOW SERVICES'].add(check['name'])
            for path in step['files']:
                text = (self.root / path).read_text()
                match = re.match(r'CREATE OR REPLACE (SQL FUNCTION|API) `([a-z0-9_]+)`', text)
                if match:
                    requested['SHOW SQL FUNCTIONS' if match[1] == 'SQL FUNCTION' else 'SHOW APIS'].add(match[2])
        evidence = []
        for sql, names in requested.items():
            if not names:
                continue
            result = self.client('sqlrec').execute(sql)
            existing = {str(row[0]) for row in result.rows if row}
            if names & existing:
                raise ValueError('Run resource names already exist; use a new isolated prefix')
            evidence.append({'sql': sql, 'rows': result.records(), 'source': result.source})
        if self.plan.get('format_version') == 5:
            for group in self.plan['resources']['models'].values():
                for binding in group['services']:
                    if self.workloads.snapshot(binding):
                        raise ValueError('Serving Kubernetes resource names already exist')
                for binding in group['jobs']:
                    if self.workloads.get('job', binding['namespace'], binding['name']) or (
                        self.workloads.get('pods', binding['namespace'], selector='job-name=' + binding['name']) or {}).get('items'):
                        raise ValueError('Training/export Kubernetes resource names already exist')
        write_json(self.root / 'execution/preflight.json', evidence)
        self.journal['preflight_completed'] = True; self.save()

    def close(self):
        for client in self.clients.values():
            try:
                client.close()
            except Exception:
                pass
        self.clients.clear()
        if self.api:
            self.api.close(); self.api = None

    def run(self, through=None, retry_failed=()):
        self.journal['status'] = 'running'; self.save()
        try:
            if not self.journal['steps'] and not self.journal.get('preflight_completed'):
                self.preflight()
            if self.plan['format_version'] < 5:
                # Old frozen plans retain their execution contract.
                cleanup = self.root / 'execution/services_cleanup.json'
                if cleanup.exists() and read_json(cleanup).get('services'):
                    raise ValueError('Legacy services were released; use a new run')
                self._run_steps(self.plan['steps'], through, retry_failed)
            else:
                shared = [s for s in self.plan['steps'] if not s['model_id']]
                self._run_steps(shared, through, retry_failed)
                if not any(s['id'] == through for s in shared):
                    for model, resources in self.plan['resources']['models'].items():
                        steps = [s for s in self.plan['steps'] if s['model_id'] == model]
                        state = self.journal.setdefault('models', {}).setdefault(model, {})
                        complete = all(self.journal['steps'].get(s['id'], {}).get('status') == 'succeeded' for s in steps)
                        if complete:
                            for step in steps:
                                record = self.journal['steps'][step['id']]
                                if not record.get('artifact') or digest(self.root / record['artifact']) != record['sha256']:
                                    raise ValueError('Saved public result/evidence changed')
                            self._release_services(model)
                            state['status'] = 'succeeded'; self.save()
                        else:
                            released = self._cleanup_evidence()['services']
                            if any(b['name'] in released and released[b['name']]['status'] != 'awaiting_creation'
                                   for b in resources['services']):
                                raise Indeterminate('Incomplete model has released services; use a new isolated run')
                            state.update(status='running', cleanup_status='pending'); self.save()
                            try:
                                self._run_steps(steps, through, retry_failed)
                                state['status'] = 'succeeded' if all(
                                    self.journal['steps'].get(s['id'], {}).get('status') == 'succeeded' for s in steps) else 'partial'
                            except BaseException:
                                state['status'] = 'failed'; raise
                            finally:
                                # Mark interruptions before the resource barrier observes Jobs.
                                for record in self.journal['steps'].values():
                                    if record['status'] in ('submitted', 'running'):
                                        record['status'] = 'indeterminate'
                                self.save()
                                try:
                                    self._release_services(model)
                                except BaseException:
                                    state['cleanup_status'] = 'blocked'
                                    self.journal['status'] = 'cleanup_failed'; self.save(); raise
                        if any(s['id'] == through for s in steps):
                            break
            if self.plan['format_version'] == 5 and self.journal.get('resource_status') in ('blocked', 'pending'):
                raise Indeterminate('A model resource barrier remains unproven; reconcile cleanup before continuing')
            self.journal['status'] = 'succeeded' if all(
                self.journal['steps'].get(s['id'], {}).get('status') == 'succeeded' for s in self.plan['steps']) else 'partial'
            self.save(); return self.journal
        except BaseException:
            if self.journal['status'] == 'running':
                for record in self.journal['steps'].values():
                    if record['status'] in ('running', 'submitted'):
                        record['status'] = 'indeterminate'
                self.journal['status'] = 'indeterminate'; self.save()
            raise
        finally:
            self.close()

    def _run_steps(self, steps, through, retry_failed):
        config = self.plan['config']
        for step in steps:
            name = step['id']; previous = self.journal['steps'].get(name)
            completed = previous and previous['status'] == 'succeeded'
            # Safe SQL function groups must be replayed from CREATE on a fresh session.
            rebuild = step['kind'] == 'function'
            if completed and not rebuild:
                if previous.get('artifact') and digest(self.root / previous['artifact']) != previous['sha256']:
                    raise ValueError('Saved public result/evidence changed')
                if step['checks']:
                    self.checks(step)
                self.audit_train(step)
                if (step.get('capture') or {}).get('kind') in ('table_schema', 'table_partition'):
                    self.reconcile_table(step)
                if name == through:
                    break
                continue
            if previous and not completed and step['replay'] != 'safe':
                explicit_create_retry = name in retry_failed and previous['status'] == 'failed' and all(
                    re.match(r'^CREATE (?:EXTERNAL )?TABLE `', (self.root / path).read_text())
                    for path in step['files']) and bool(step['files'])
                if explicit_create_retry:
                    # CREATE without REPLACE/IF NOT EXISTS cannot overwrite a
                    # table. Only an operator-selected definitive failure is
                    # retried, never an uncertain write/train/export.
                    previous = {**previous, 'explicit_create_retry': True}
                    try:
                        proof = self.reconcile_table(step)
                    except SQLFailed:
                        pass  # Not present: retry collision-safe CREATE below.
                    else:
                        attempt = len(previous.get('history',[]))+1
                        artifact = f'execution/{name}_attempt_{attempt:03d}.json'
                        write_json(self.root/artifact, {'step':name, 'results':[proof]})
                        record = {'status':'succeeded','endpoint':step['endpoint'],'artifact':artifact,
                                  'sha256':digest(self.root/artifact),'reconciled':True,
                                  'history':previous.get('history',[])+[{k:v for k,v in previous.items() if k!='history'}]}
                        self.journal['steps'][name] = record; self.save()
                        if name == through:
                            break
                        continue
                elif previous['status'] == 'failed' and not previous.get('mutation_completed'):
                    raise Indeterminate(f'{name}: definitive mutation failure; cannot adopt an existing resource')
                elif step['replay'] == 'verify' and step['checks']:
                    try:
                        proof = self.checks(step, completion=True)
                    except Exception as error:
                        raise Indeterminate(f'{name}: cannot prove completion; mutation will not be resubmitted') from error
                    artifact = f'execution/{name}_reconciled_{len(previous.get("history", [])):03d}.json'
                    write_json(self.root / artifact, {'step': name, 'results': proof})
                    previous.update(status='succeeded', reconciled=True, proofs=proof,
                                    artifact=artifact, sha256=digest(self.root / artifact)); self.save()
                    self.audit_train(step)
                    if name == through:
                        break
                    continue
                else:
                    raise Indeterminate(f'{name}: prior outcome uncertain; no automatic mutation retry')
            record = {'status': 'submitted', 'operations': [], 'endpoint': step['endpoint']}
            if previous:
                record['history'] = previous.get('history', []) + [{k:v for k,v in previous.items() if k != 'history'}]
            self.journal['steps'][name] = record; self.save()
            outputs = []
            def submitted(operation):
                record['status'] = 'running'; record['operations'].append(operation); self.save()
                if step['kind'] == 'train':
                    self.runtime_audit.capture_job(step['model_id'])
            try:
                capture = step.get('capture')
                if step['endpoint'] == 'infra':
                    spec = read_json(self.root / capture['spec'])
                    proof = self.index_factory(config['sql']['endpoints']['milvus']).verify(
                        spec, empty=capture['kind'] == 'index_empty', timeout=config['execution'].get('index_timeout', 120))
                    outputs.append({**proof, 'role': 'index_proof'})
                else:
                    for position, path in enumerate(step['files']):
                        result = self.client(step['endpoint']).execute((self.root / path).read_text(),
                            timeout=config['execution'].get('operation_timeout', 3600), on_submitted=submitted)
                        outputs.append({'source': result.source, 'sql_file': path,
                                        'operation_id': result.operation_id, 'rows': result.records(),
                                        'role': step['result_roles'][position]})
                        # Validate embeddings before SQL INSERT, not after a broken index write.
                        if capture and capture['kind'] == 'vectors' and outputs[-1]['role'] == 'vectors':
                            # v3's fixed layout reserved a final INSERT receipt.
                            evidence = outputs if capture.get('result_role') else outputs + [{'rows': []}]
                            validate_capture(capture, evidence)
                    record['mutation_completed'] = bool(step['files']); self.save()
                    if capture and capture.get('request') and config['serving']['entrypoint'] == 'sql_api':
                        if self.api is None:
                            self.api = self.api_factory(config['sql']['endpoints']['api'])
                        rows = self.api.call(capture['api'], read_json(self.root / capture['request']))
                        outputs.append({'source': 'public_sql_api', 'request': capture['request'], 'rows': rows,
                                        'receipt': getattr(self.api, 'last_call', {}), 'role': 'prediction'})
                    if capture:
                        validate_capture(capture, outputs)
                        if capture['kind'] in ('table_schema', 'table_partition'):
                            outputs.append(self.reconcile_table(step))
                    if step['checks']:
                        outputs.extend({**proof, 'role': 'completion_proof'} for proof in self.checks(step))
                    self.audit_train(step)
                suffix = f'_attempt_{len(record["history"]):03d}' if record.get('history') else ''
                artifact = f'execution/{name}{suffix}.json'
                write_json(self.root / artifact, {'step': name, 'results': outputs})
                record.update(status='succeeded', artifact=artifact, sha256=digest(self.root / artifact)); self.save()
            except Exception as error:
                record.update(status='indeterminate' if isinstance(error, Indeterminate) else 'failed',
                              error_type=type(error).__name__, error=str(error).splitlines()[0][:1000]
                              if isinstance(error, (SQLFailed, Indeterminate, ValueError)) else 'Public request failed; see endpoint logs')
                # Server/HTTP exception text may contain secrets; raw SQL templates are retained instead.
                self.journal['status'] = record['status']; self.save(); raise
            if name == through:
                break

    def _cleanup_evidence(self):
        path = self.root / 'execution/services_cleanup.json'
        evidence = read_json(path) if path.exists() else {'plan_id': self.plan['plan_id'], 'services': {}}
        if evidence['plan_id'] != self.plan['plan_id']:
            raise ValueError('Service release belongs to another plan')
        return evidence

    def _service_bindings(self, model=None):
        if self.plan['format_version'] == 5:
            return [binding for name, group in self.plan['resources']['models'].items()
                    if model is None or name == model for binding in group['services']]
        # Older frozen plans have no physical-resource manifest.
        execution = self.plan['config']['execution']
        namespace = execution.get('service_resources', execution.get('resources', {})).get('NAMESPACE')
        return [{**step['checks'][0], 'step_id': step['id'], 'namespace': namespace,
                 'deployment': kubernetes_name(step['checks'][0]['name'])}
                for step in self.plan['steps'] if step['kind'] == 'service' and
                (model is None or step['checks'][0]['model'] == model)]

    def _release_services(self, model=None):
        """Idempotent public DROP followed by a read-only physical resource barrier."""
        from urllib.parse import urlsplit
        import time
        path = self.root / 'execution/services_cleanup.json'
        evidence = self._cleanup_evidence()
        errors = []
        client = None
        timeout = self.plan['config']['execution'].get('cleanup_timeout', 120)
        for binding in self._service_bindings(model):
            name = binding['name']
            attempted = self.journal['steps'].get(binding['step_id'])
            previous = evidence['services'].get(name)
            if not attempted and not previous:
                continue
            try:
                if not self.journal.get('preflight_completed') or not binding.get('namespace'):
                    raise Indeterminate('No isolation/namespace proof; service deletion is blocked')
                client = client or self.client('sqlrec')
                def present():
                    return name in {str(r[0]) for r in client.execute('SHOW SERVICES', timeout=timeout).rows if r}
                exists = present()
                if previous and previous.get('status') == 'succeeded':
                    if exists:
                        raise Indeterminate('Released service name has been reused; no further deletion')
                    self.workloads.wait_absent(binding, previous['owned_uids'])
                    continue
                if exists:
                    # Reconcile uncertain DROP by absence, never by resubmitting it.
                    if previous and previous.get('status') in ('submitted', 'indeterminate', 'drop_completed'):
                        raise Indeterminate('Prior DROP outcome remains uncertain; no automatic resubmission')
                    step = next(s for s in self.plan['steps'] if s['id'] == binding['step_id'])
                    proofs = self.checks(step, timeout)
                    url = describe_map(proofs[0]['rows']).get('URL:', '')
                    if urlsplit(url).hostname != binding['deployment'] + '.' + binding['namespace'] + '.svc.cluster.local':
                        raise Indeterminate('Public service URL differs from frozen Kubernetes binding')
                    owned = self.workloads.snapshot(binding)
                    if attempted['status'] in ('submitted', 'running', 'indeterminate') and not any(
                        key.startswith('Deployment/') for key in owned):
                        raise Indeterminate('Service operation may still create its Deployment; release is blocked')
                    # Prove Pod/ReplicaSet ancestry before recording ownership.
                    roots = {k:v for k,v in owned.items() if k.startswith(('Deployment/', 'Service/'))}
                    self.workloads.verify_owners(owned, roots)
                    previous = {'status': 'submitted', 'owned_uids': owned, 'binding_proofs': proofs,
                                'sql': f'DROP SERVICE `{name}`;'}
                    evidence['services'][name] = previous; write_json(path, evidence)
                    try:
                        result = client.execute(previous['sql'], timeout=timeout)
                        previous.update(status='drop_completed', source=result.source,
                                        rows=result.records(), operation_id=result.operation_id)
                        write_json(path, evidence)
                    except BaseException:
                        previous['status'] = 'indeterminate'; write_json(path, evidence); raise
                elif previous is None:
                    if attempted['status'] in ('submitted', 'running', 'indeterminate'):
                        evidence['services'][name] = {'status': 'awaiting_creation', 'owned_uids': {}}
                        write_json(path, evidence)
                        raise Indeterminate('Service creation may still be running; absence alone is not a release proof')
                    owned = self.workloads.snapshot(binding)
                    if owned:
                        raise Indeterminate('SQL service absent but unverified physical resources remain')
                    previous = {'status': 'absent', 'owned_uids': {}}
                    evidence['services'][name] = previous; write_json(path, evidence)
                elif previous['status'] == 'awaiting_creation':
                    raise Indeterminate('Service creation outcome is still uncertain; reconcile the public operation first')
                deadline = time.monotonic() + timeout
                while present():
                    if time.monotonic() >= deadline:
                        raise Indeterminate('Public service metadata is still present; scheduling is blocked')
                    time.sleep(1)
                previous['resource_proof'] = self.workloads.wait_absent(binding, previous['owned_uids'])
                previous['status'] = 'succeeded'; write_json(path, evidence)
                evidence.get('errors', {}).pop(name, None)
            except Exception as error:
                errors.append(error)
                # Try other owned services even when one DSSM tower cannot be released.
                evidence.setdefault('errors', {})[name] = type(error).__name__
                write_json(path, evidence)
        groups = self.plan.get('resources', {}).get('models', {})
        for name, resources in groups.items():
            if model is not None and name != model:
                continue
            state = self.journal.setdefault('models', {}).setdefault(name, {})
            try:
                active = []; terminal_proofs = []
                for binding in resources['jobs']:
                    step = next(s for s in self.plan['steps'] if s['model_id'] == name and s['kind'] == binding['kind'])
                    record = self.journal['steps'].get(step['id'])
                    if not record:
                        continue
                    active.append(binding)
                    if record['status'] in ('submitted', 'running', 'indeterminate'):
                        # Absence alone cannot exclude a job appearing after a lost SQL response.
                        try:
                            self.checks(step, timeout, allow_failed_checkpoint=True)
                        except Indeterminate:
                            # Cancelling the SQL operation can leave metadata at
                            # "created". A terminal Job with the captured UID is
                            # a release proof, even without checkpoint success.
                            captured = self.root / f'execution/runtime/{name}_job.json'
                            observed = read_json(captured) if binding['kind'] == 'train' and captured.exists() else {}
                            job = self.workloads.get('job', binding['namespace'], binding['name'])
                            if (not job or observed.get('job') != binding['name'] or
                                observed.get('uid') != job['metadata']['uid'] or not any(
                                    c.get('status') == 'True' and c.get('type') in ('Complete', 'Failed')
                                    for c in job.get('status', {}).get('conditions', []))):
                                raise
                            terminal_proofs.append({'source': 'public_kubernetes_readonly',
                                                    'job': binding['name'], 'uid': observed['uid'], 'terminal': True})
                state['job_proofs'] = terminal_proofs + self.workloads.jobs_quiet(active)
                if not errors:
                    state['cleanup_status'] = 'not_required' if state.get('status') == 'not_executed' else 'released'
                else:
                    state['cleanup_status'] = 'blocked'
            except Exception as error:
                errors.append(error); state['cleanup_status'] = 'blocked'
        states = self.journal.get('models', {}).values()
        evidence['status'] = 'blocked' if errors or any(s.get('cleanup_status') == 'blocked' for s in states) else (
            'pending' if any(s.get('cleanup_status') == 'pending' for s in states) else 'succeeded')
        write_json(path, evidence)
        self.journal['resource_status'] = evidence['status']; self.save()
        if errors:
            self.journal['status'] = 'cleanup_failed'; self.save()
            raise Indeterminate('Resource cleanup/quiescence is unproven; no next model may start') from errors[0]
        return evidence

    def cleanup_services(self):
        """Failure/signal fallback, independent of report generation."""
        try:
            result = self._release_services()
            if result['status'] != 'succeeded':
                raise Indeterminate('Another model resource barrier remains unproven')
            return result
        finally:
            self.close()
