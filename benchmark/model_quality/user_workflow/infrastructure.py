"""Normal Milvus administration, separate from SQL inference/vector loading.

Provisioning creates schema/index only. Verification uses read-only public REST;
this module cannot insert embeddings or execute vector search.
"""
import os
import time
import json
import subprocess

import requests

from .sql_client import Indeterminate


class WorkloadAdmin:
    """Read-only barrier after public DROP; never deletes Kubernetes resources."""
    def __init__(self, execution):
        self.command = execution.get('runtime_audit', {}).get('kubectl', ['kubectl'])
        self.timeout = execution.get('cleanup_timeout', 120)
        self.interval = execution.get('runtime_audit', {}).get('poll_interval', 1)

    def get(self, kind, namespace, name=None, selector=None):
        command = self.command + ['get', kind, '-n', namespace, '-o', 'json', '--ignore-not-found']
        if name:
            command.append(name)
        if selector:
            command += ['-l', selector]
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=min(10, self.timeout))
            if result.returncode:
                raise Indeterminate('Kubernetes resource read failed; scheduling is blocked')
            return json.loads(result.stdout) if result.stdout.strip() else None
        except (OSError, subprocess.TimeoutExpired, ValueError) as error:
            raise Indeterminate('Kubernetes absence cannot be verified; scheduling is blocked') from error

    def snapshot(self, binding):
        ns, name = binding['namespace'], binding['deployment']
        objects = []
        for kind in ('deployment', 'service'):
            value = self.get(kind, ns, name)
            if value:
                objects.append(value)
        for kind in ('replicasets', 'pods'):
            objects.extend((self.get(kind, ns, selector='app=' + name) or {}).get('items', []))
        return {f'{obj["kind"]}/{obj["metadata"]["name"]}': {
                    'uid': obj['metadata']['uid'],
                    'owners': [o['uid'] for o in obj['metadata'].get('ownerReferences', [])]}
                for obj in objects}

    @staticmethod
    def verify_owners(snapshot, owned):
        deployment = next((v['uid'] for k, v in owned.items() if k.startswith('Deployment/')), None)
        if snapshot and deployment is None:
            raise Indeterminate('Serving workload has no verified deployment UID')
        # A controller may create another replica or Pod while deletion starts.
        roots = {deployment}
        remaining = dict(snapshot)
        for key in list(remaining):
            if key in owned:
                if remaining[key]['uid'] != owned[key]['uid']:
                    raise Indeterminate('Resource UID changed; refusing to adopt another workload')
                roots.add(remaining.pop(key)['uid'])
        while remaining:
            children = [k for k, v in remaining.items() if set(v['owners']) & roots]
            if not children:
                raise Indeterminate('Unexpected serving resources; ownership cannot be verified')
            for key in children:
                roots.add(remaining.pop(key)['uid'])

    def wait_absent(self, binding, owned):
        deadline = time.monotonic() + self.timeout
        while True:
            snapshot = self.snapshot(binding)
            self.verify_owners(snapshot, owned)
            if not snapshot:
                return {'source': 'public_kubernetes_readonly', 'absent': True, 'owned_uids': owned}
            if time.monotonic() >= deadline:
                raise Indeterminate('Serving Deployment/Service/ReplicaSet/Pods still exist; scheduling is blocked')
            time.sleep(self.interval)

    def jobs_quiet(self, jobs):
        proofs = []
        for binding in jobs:
            job = self.get('job', binding['namespace'], binding['name'])
            pods = (self.get('pods', binding['namespace'], selector='job-name=' + binding['name']) or {}).get('items', [])
            terminal = job is None or any(c.get('status') == 'True' and c.get('type') in ('Complete', 'Failed')
                                         for c in job.get('status', {}).get('conditions', []))
            if not terminal or any(p.get('status', {}).get('phase') not in ('Succeeded', 'Failed') for p in pods):
                raise Indeterminate('Training/export workload is still active; scheduling is blocked')
            proofs.append({'source': 'public_kubernetes_readonly', **binding, 'quiet': True})
        return proofs


class IndexAdmin:
    def __init__(self, config):
        self.config = config

    def request(self, path, body):
        token = os.environ.get(self.config['token_env'], '')
        response = requests.post(self.config['url'].rstrip('/') + '/v2/vectordb/' + path,
                                 headers={'Authorization': 'Bearer ' + token} if token else {},
                                 json=body, timeout=30)
        if response.status_code in (401, 403):
            raise ValueError(f'Milvus authentication failed; configure {self.config["token_env"]}')
        response.raise_for_status(); result = response.json()
        if result.get('code') != 0:
            raise ValueError(f'Milvus request rejected (code {result.get("code")}); check authentication and permissions for {self.config["token_env"]}')
        return result.get('data')

    def describe(self, spec):
        return self.request('collections/describe', {'collectionName': spec['collection']})

    def provision(self, spec):
        # CREATE is deliberate and never replaces/drops an existing collection.
        if spec['collection'] in self.request('collections/list', {}):
            raise ValueError('Collection already exists; provision needs a new isolated prefix')
        return self.request('collections/create', {
            'collectionName': spec['collection'],
            'schema': {'autoId': False, 'enabledDynamicField': False, 'fields': [
                {'fieldName': 'id', 'dataType': 'Int64', 'isPrimary': True},
                {'fieldName': 'item_id', 'dataType': 'VarChar', 'elementTypeParams': {'max_length': 512}},
                {'fieldName': 'embedding', 'dataType': 'FloatVector', 'elementTypeParams': {'dim': str(spec['dimension'])}}]},
            'indexParams': [{'fieldName': 'embedding', 'metricType': spec['metric'],
                             'indexName': 'embedding', 'indexType': spec['index_type']}]})

    def verify(self, spec, empty=False, timeout=120):
        description = self.describe(spec)
        fields = {f.get('name', f.get('fieldName')): f for f in description.get('fields', [])}
        embedding = fields.get('embedding', {})
        params = embedding.get('params', embedding.get('elementTypeParams', {}))
        if isinstance(params, list):
            params = {p['key']: p['value'] for p in params}
        if str(params.get('dim')) != str(spec['dimension']):
            raise ValueError('Index vector dimension mismatch')
        indexes = self.request('indexes/describe', {'collectionName': spec['collection'], 'indexName': 'embedding'})
        if not any(i.get('metricType') == spec['metric'] and i.get('indexType') == spec['index_type'] for i in indexes):
            raise ValueError('Index metric/type mismatch')
        deadline = time.monotonic() + timeout
        while True:
            count = self.request('entities/query', {'collectionName': spec['collection'],
                                                   'filter': 'id >= 0', 'outputFields': ['count(*)'],
                                                   'consistencyLevel': 'Strong'})
            actual = int(count[0]['count(*)'])
            expected = 0 if empty else spec['expected_rows']
            if actual == expected:
                return {'description': description, 'indexes': indexes, 'visible_rows': actual,
                        'source': 'public_milvus_admin_readonly'}
            if empty or time.monotonic() >= deadline:
                raise ValueError(f'Index visible rows {actual}, expected {expected}')
            time.sleep(1)
