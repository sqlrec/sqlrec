"""Read-only inspection of normal public training artifacts and Kubernetes jobs.

This never generates configuration, loads weights, trains, or patches resources.
Stored SQL parameters alone are not evidence that a deployed version applies them.
"""
import json
import os
import re
import subprocess
import time
from pathlib import Path

from ..common import digest, read_json, write_json


def parse_proto(text):
    tokens = re.findall(r'"(?:[^"\\]|\\.)*"|[A-Za-z_][A-Za-z_0-9.]*|[-+]?\d+(?:\.\d*)?(?:[eE][-+]?\d+)?|[{}:\[\],]', text)
    position = 0
    def scalar(token):
        if token.startswith('"'):
            return json.loads(token)
        if token in ('true','false'):
            return token == 'true'
        try:
            return float(token) if any(c in token for c in '.eE') else int(token)
        except ValueError:
            return token
    def message():
        nonlocal position
        value = {}
        while position < len(tokens) and tokens[position] != '}':
            key = tokens[position]; position += 1
            if tokens[position] == ':':
                position += 1
            if tokens[position] == '{':
                position += 1; item = message(); position += 1
            elif tokens[position] == '[':
                position += 1; item = []
                while tokens[position] != ']':
                    if tokens[position] != ',':
                        item.append(scalar(tokens[position]))
                    position += 1
                position += 1
            else:
                item = scalar(tokens[position]); position += 1
            value.setdefault(key, []).extend(item if isinstance(item, list) else [item])
        return value
    return message()


def one(value, key, default=None):
    result = value.get(key, [])
    return result[0] if len(result) == 1 else default


def compare_dssm(text, params, features, seed, job=None):
    data = parse_proto(text); actual = {}; unsupported = []
    dc = one(data,'data_config',{}); tc = one(data,'train_config',{})
    dssm = one(one(data,'model_config',{}),'dssm',{})
    for key in ('batch_size','num_workers'):
        actual[key] = one(dc,key)
    actual['num_epochs'] = one(tc,'num_epochs')
    actual['sparse_lr'] = one(one(one(tc, 'sparse_optimizer', {}), 'adagrad_optimizer', {}), 'lr')
    actual['dense_lr'] = one(one(one(tc, 'dense_optimizer', {}), 'adam_optimizer', {}), 'lr')
    actual['mixed_precision'] = one(tc, 'mixed_precision')
    actual['output_dim'] = one(dssm,'output_dim')
    for tower in ('user','item'):
        actual[tower+'_hidden_units'] = ','.join(map(str, one(one(dssm,tower+'_tower',{}),'mlp',{}).get('hidden_units',[])))
    errors = []
    groups = {one(g,'group_name'):g.get('feature_names',[]) for g in one(data,'model_config',{}).get('feature_groups',[])}
    for tower in ('user','item'):
        group = one(one(dssm,tower+'_tower',{}),'input',tower)
        declared = [f['name'] for f in features if f.get('role')==tower]
        if groups.get(group) != declared:
            errors.append({'parameter':tower+'_features','expected':declared,'actual':groups.get(group)})
    if one(dssm,'in_batch_negative') is not True:
        errors.append({'parameter':'in_batch_negative','expected':True,'actual':one(dssm,'in_batch_negative')})
    feature_config = {}
    for entry in data.get('feature_configs',[]):
        for kind, entries in entry.items():
            for conf in entries:
                feature_config[one(conf,'feature_name')] = (kind, conf)
    for feature in features:
        kind, conf = feature_config.get(feature['name'], ('missing',{}))
        if kind == 'missing':
            errors.append({'parameter':feature['name'], 'expected':'configured feature', 'actual':None})
        name = feature['name']; bucket = 'num_buckets' if feature['type'] in ('INT','BIGINT') else 'hash_bucket_size'
        actual[f'column.{name}.bucket_size'] = one(conf,bucket)
        actual[f'column.{name}.embedding_dim'] = one(conf,'embedding_dim')
        for option in ('normalizer', 'default_value', 'separator', 'value_dim'):
            actual[f'column.{name}.{option}'] = one(conf, option)
        embedding = params.get(f'column.{name}.embedding_dim', params.get('embedding_dim'))
        if embedding is not None and kind == 'id_feature' and one(conf,'embedding_dim') != int(embedding):
            errors.append({'parameter':f'column.{name}.embedding_dim', 'expected':int(embedding), 'actual':one(conf,'embedding_dim')})
        if kind == 'id_feature' and 'num_buckets' in params and f'column.{name}.bucket_size' not in params and one(conf, bucket) != int(params['num_buckets']):
            errors.append({'parameter': f'column.{name}.bucket_size', 'expected': int(params['num_buckets']), 'actual': one(conf, bucket)})
    for key,value in params.items():
        if key in ('version','image','embedding_dim','num_buckets'):
            continue
        if key not in actual:
            unsupported.append(key); continue
        expected = str(value).upper() if key == 'mixed_precision' else value
        if not equivalent(actual[key], expected):
            errors.append({'parameter':key,'expected':value,'actual':actual[key]})
    env = (job or {}).get('seed_env', {})
    seed_verified = env.get('TORCH_MANUAL_SEED') == str(seed) and env.get('NUMPY_MANUAL_SEED') == str(seed)
    if not seed_verified:
        errors.append({'parameter':'random_seed','expected':seed,'actual':env or (
            'training seed environment variables missing' if job else 'training job evidence unavailable')})
    expected_image = params.get('image','sqlrec/tzrec')+':'+params['version'] if 'version' in params else None
    actual['image'] = (job or {}).get('image')
    if job and expected_image and job.get('image') != expected_image:
        errors.append({'parameter':'image','expected':expected_image,'actual':job.get('image')})
    return {'status':'verified' if not errors and not unsupported else 'mismatch' if errors else 'unsupported',
            'actual':actual, 'mismatches':errors, 'unverified_parameters':unsupported, 'seed_verified':seed_verified}


def equivalent(actual, expected):
    if actual is None:
        return False
    try:
        return float(actual) == float(expected)
    except (ValueError, TypeError):
        return str(actual) == str(expected)


def compare_pipeline(text, recipe, features, tasks, seed, job=None):
    """Check submitted native configs without importing any model runtime."""
    family, params = recipe['family'], recipe['params']
    if family == 'tzrec.dssm':
        result = compare_dssm(text, params, features, seed, job)
        labels = one(parse_proto(text), 'data_config', {}).get('label_fields', [])
        if tasks and labels != [t['column'] for t in tasks]:
            result['mismatches'].append({'parameter': 'label_columns', 'expected': [t['column'] for t in tasks], 'actual': labels})
            result['status'] = 'mismatch'
        if job is None or not params.get('version'):
            result['unverified_parameters'].append('training_image')
            if result['status'] == 'verified':
                result['status'] = 'unsupported'
        return result
    actual, errors, unsupported = {}, [], []
    def check(key, expected, observed):
        actual[key] = observed
        if isinstance(expected, list):
            matches = observed == expected
        else:
            matches = equivalent(observed, expected)
        if not matches:
            errors.append({'parameter': key, 'expected': expected, 'actual': observed})
    overridden = []
    if family.startswith('gbdt.'):
        pipeline = json.loads(text)
        native = pipeline.get('params', {})
        check('model_type', family.split('.')[1], pipeline.get('model_type'))
        check('features', [f['name'] for f in features], pipeline.get('feature_columns'))
        check('label_columns', ','.join(t['column'] for t in tasks), pipeline.get('label_columns'))
        categorical = [f['name'] for f in features if f['type'] in ('STRING', 'INT', 'BIGINT')] if family == 'gbdt.catboost' else []
        check('categorical_features', categorical, pipeline.get('categorical_features'))
        check('random_seed', seed, native.get('random_seed'))
        aliases = {'num_iterations': 'iterations', 'max_depth': 'depth', 'l2_regularization': 'l2_leaf_reg',
                   'cb_iterations': 'iterations', 'cb_depth': 'depth', 'cb_l2_leaf_reg': 'l2_leaf_reg'} if family == 'gbdt.catboost' else {}
        shadows = {'num_iterations': 'cb_iterations', 'max_depth': 'cb_depth', 'l2_regularization': 'cb_l2_leaf_reg'} if aliases else {}
        for key, value in params.items():
            if key in ('version', 'image'):
                continue
            if shadows.get(key) in params:
                overridden.append(key)
                continue
            native_key = aliases.get(key, key)
            if native_key not in native:
                unsupported.append(key)
            else:
                check(key, value, native[native_key])
        seed_verified = equivalent(native.get('random_seed'), seed)
    else:
        pipeline = parse_proto(text)
        dc = one(pipeline, 'data_config', {}); tc = one(pipeline, 'train_config', {})
        mc = one(pipeline, 'model_config', {}); model_key = family.split('.')[1]
        model = one(mc, model_key)
        if not isinstance(model, dict):
            errors.append({'parameter': 'model', 'expected': model_key, 'actual': None})
            model = {}
        native = {'batch_size': one(dc, 'batch_size'), 'num_workers': one(dc, 'num_workers'),
                  'num_epochs': one(tc, 'num_epochs'), 'mixed_precision': one(tc, 'mixed_precision'),
                  'sparse_lr': one(one(one(tc, 'sparse_optimizer', {}), 'adagrad_optimizer', {}), 'lr'),
                  'dense_lr': one(one(one(tc, 'dense_optimizer', {}), 'adam_optimizer', {}), 'lr')}
        mlp = lambda conf: ','.join(map(str, conf.get('hidden_units', [])))
        if model_key in ('wide_and_deep', 'deepfm'):
            native['hidden_units'] = mlp(one(model, 'deep', {}))
        elif model_key == 'rocket_launching':
            for tower in ('booster', 'light', 'share'):
                native[tower + '_hidden_units'] = mlp(one(model, tower + '_mlp', {}))
        elif model_key == 'mmoe':
            native['num_expert'] = one(model, 'num_expert')
            native['expert_hidden_units'] = mlp(one(model, 'expert_mlp', {}))
            towers = {one(t, 'label_name'): t for t in model.get('task_towers', [])}
            check('task_labels', [t['column'] for t in tasks], list(towers))
            widths = []
            for task in tasks:
                tower = towers.get(task['column'], {})
                loss = 'binary_cross_entropy' if task['type'] == 'binary' else 'l2_loss'
                if not any(loss in entry for entry in tower.get('losses', [])):
                    errors.append({'parameter': task['column'] + '.type', 'expected': loss, 'actual': tower.get('losses')})
                widths.append(mlp(one(tower, 'mlp', {})))
            if widths and len(set(widths)) == 1:
                native['task_hidden_units'] = widths[0]
        check('label_columns', [t['column'] for t in tasks], dc.get('label_fields', []))
        groups = {one(g, 'group_name'): g.get('feature_names', []) for g in mc.get('feature_groups', [])}
        check('deep_features', [f['name'] for f in features], groups.get('deep'))
        sparse = [f['name'] for f in features if f['type'] not in ('FLOAT', 'DOUBLE', 'ARRAY<FLOAT>', 'ARRAY<DOUBLE>')
                  or params.get(f'column.{f["name"]}.boundaries')]
        if model_key in ('wide_and_deep', 'deepfm'):
            check('wide_features', sparse, groups.get('wide'))
        if model_key == 'deepfm':
            check('fm_features', sparse, groups.get('fm'))
        feature_configs = {one(conf, 'feature_name'): (kind, conf)
                           for entry in pipeline.get('feature_configs', []) for kind, configs in entry.items() for conf in configs}
        check('configured_features', sorted(f['name'] for f in features), sorted(feature_configs))
        for feature in features:
            name = feature['name']; kind, conf = feature_configs.get(name, ('missing', {}))
            bucket = 'num_buckets' if feature['type'] in ('INT', 'BIGINT') else 'hash_bucket_size'
            native[f'column.{name}.bucket_size'] = one(conf, bucket)
            for option in ('embedding_dim', 'normalizer', 'default_value', 'separator', 'value_dim'):
                native[f'column.{name}.{option}'] = one(conf, option)
            embedding = params.get(f'column.{name}.embedding_dim', params.get('embedding_dim'))
            if kind == 'id_feature' and embedding is not None:
                check(f'column.{name}.embedding_dim', embedding, one(conf, 'embedding_dim'))
            if kind == 'id_feature' and 'num_buckets' in params and f'column.{name}.bucket_size' not in params:
                check(f'column.{name}.bucket_size', params['num_buckets'], one(conf, bucket))
        for key, value in params.items():
            if key in ('version', 'image', 'embedding_dim', 'num_buckets'):
                continue
            if key not in native:
                unsupported.append(key)
            else:
                check(key, str(value).upper() if key == 'mixed_precision' else value, native[key])
        seed_env = (job or {}).get('seed_env', {})
        seed_verified = seed_env.get('TORCH_MANUAL_SEED') == str(seed) and seed_env.get('NUMPY_MANUAL_SEED') == str(seed)
        if not seed_verified:
            errors.append({'parameter': 'random_seed', 'expected': seed, 'actual': seed_env or 'job seed evidence unavailable'})
    image = params.get('image', 'sqlrec/tzrec' if family.startswith('tzrec.') else 'sqlrec/gbdt')
    if job and params.get('version'):
        check('image', image + ':' + params['version'], job.get('image'))
    else:
        unsupported.append('training_image')
    return {'status': 'mismatch' if errors else 'unsupported' if unsupported else 'verified',
            'actual': actual, 'mismatches': errors, 'unverified_parameters': unsupported,
            'overridden_parameters': overridden, 'seed_verified': seed_verified}


from .specs import kubernetes_name


class RuntimeAudit:
    def __init__(self, root, plan):
        self.root, self.plan = Path(root), plan
        self.config = plan['config']['execution'].get('runtime_audit')

    def capture_job(self, model):
        if not self.config:
            return
        deadline = time.monotonic() + self.config.get('job_timeout', 10)
        reason = 'job_not_observed'
        while True:
            try:
                reason = self._capture_job_once(model, max(.1, min(30, deadline - time.monotonic())))
            except (OSError, subprocess.TimeoutExpired):
                reason = 'kubernetes_read_failed'
            except (ValueError, KeyError, TypeError):
                reason = 'invalid_kubernetes_evidence'
            if reason is None:
                return
            if reason != 'job_not_observed' or time.monotonic() >= deadline:
                write_json(self.root/f'execution/runtime/{model}_job_capture.json',
                           {'plan_id': self.plan['plan_id'], 'status': 'missing', 'reason': reason,
                            'source': 'public_kubernetes_readonly'})
                return
            time.sleep(min(self.config.get('poll_interval', 1), max(0, deadline - time.monotonic())))

    def _capture_job_once(self, model, timeout):
        deadline = time.monotonic() + timeout
        namespace = self.plan['config']['execution']['resources']['NAMESPACE']
        command = self.config.get('kubectl', ['kubectl']) + ['get','job',kubernetes_name(model + '-v1') + '-job', '-n',namespace,'-o','json']
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            return 'job_not_observed' if 'NotFound' in result.stderr else 'kubernetes_read_failed'
        value = json.loads(result.stdout); container = value['spec']['template']['spec']['containers'][0]
        proof = {'source':'public_kubernetes_readonly', 'job':value['metadata']['name'],
                 'uid':value['metadata']['uid'], 'image':container['image'],
                 'seed_env':{e['name']:e.get('value') for e in container.get('env',[]) if e['name'] in ('TORCH_MANUAL_SEED','NUMPY_MANUAL_SEED')}}
        for volume in value['spec']['template']['spec'].get('volumes',[]):
            cm_name = volume.get('configMap',{}).get('name')
            if not cm_name:
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            cm_result = subprocess.run(self.config.get('kubectl',['kubectl'])+
                                       ['get','configmap',cm_name,'-n',namespace,'-o','json'],
                                       capture_output=True,text=True,timeout=remaining)
            if cm_result.returncode:
                continue
            pipeline = json.loads(cm_result.stdout).get('data',{}).get('pipeline.config')
            if pipeline:
                path = self.root/f'execution/runtime/{model}_submitted_pipeline.config'
                path.parent.mkdir(parents=True,exist_ok=True); path.write_text(pipeline)
                proof.update(configmap=cm_name, pipeline_artifact=str(path.resolve()), pipeline_sha256=digest(path))
        write_json(self.root/f'execution/runtime/{model}_job.json',proof)
        return None

    def checkpoint(self, model, recipe, seed, artifact=None):
        if not recipe['family'].startswith(('tzrec.', 'gbdt.')):
            value = {'status':'unsupported','reason':'Runtime artifact audit for this family is not implemented'}
        else:
            job_path = self.root/f'execution/runtime/{model}_job.json'
            job = read_json(job_path) if job_path.exists() else None
            if artifact is None and job and job.get('pipeline_artifact'):
                artifact = Path(job['pipeline_artifact'])
                if digest(artifact) != job['pipeline_sha256']:
                    raise ValueError('Submitted public training configuration changed after capture')
            if artifact is None:
                if not self.config:
                    return
                if not (self.config.get('hadoop') or os.environ.get('HADOOP_BIN')) or not self.config.get('model_base_uri'):
                    value = {'status':'missing', 'reason':'Submitted pipeline was not captured and no public filesystem reader is configured',
                             'plan_id':self.plan['plan_id'], 'model':model}
                    write_json(self.root/f'execution/runtime/{model}_audit.json',value)
                    return value
                uri = self.config['model_base_uri'].rstrip('/')+f'/{model}/v1/pipeline.config'
                env = dict(os.environ)
                if self.config.get('java_home') and not env.get('JAVA_HOME'):
                    env['JAVA_HOME'] = self.config['java_home']
                command = [os.environ['HADOOP_BIN']] if os.environ.get('HADOOP_BIN') else self.config['hadoop']
                result = subprocess.run(command+['fs','-cat',uri], env=env,
                                        capture_output=True, text=True, timeout=60)
                if result.returncode:
                    raise ValueError('Cannot read normal training artifact through public filesystem client')
                artifact = self.root/f'execution/runtime/{model}_pipeline.config'
                artifact.parent.mkdir(parents=True, exist_ok=True); artifact.write_text(result.stdout)
            value = compare_pipeline(Path(artifact).read_text(), recipe, self.plan['features'],
                                     self.plan.get('tasks', self.plan['config'].get('tasks', [])), seed, job)
            value.update(artifact_sha256=digest(artifact), artifact=str(Path(artifact).resolve()),
                         source='public_training_artifact_readonly')
        value.update(plan_id=self.plan['plan_id'],model=model)
        write_json(self.root/f'execution/runtime/{model}_audit.json',value)
        return value
