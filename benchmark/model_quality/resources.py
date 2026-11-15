"""Model lifecycle and owned service release. No resume or mutation replay."""
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit
import json
import logging
import re
import subprocess
import time

from .client import HS2Client, Indeterminate
from .common import identifier, literal, write_json
from .config import Feature
from .data import fields, options


def metadata(result):
    return {str(row[0]).strip(): str(row[1]).strip() for row in result.rows if len(row) >= 2}


class Workloads:
    """Read Kubernetes only; service removal uses public DROP SERVICE."""
    def __init__(self, command, namespace):
        self.command, self.namespace = command, namespace

    def get(self, kind, name=None, selector=None):
        command = [self.command, 'get', kind, '-n', self.namespace, '-o', 'json', '--ignore-not-found']
        if name:
            command.append(name)
        if selector:
            command.extend(['-l', selector])
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
        if result.returncode:
            raise Indeterminate('Kubernetes read failed; resource release cannot be confirmed')
        return json.loads(result.stdout) if result.stdout.strip() else None

    def snapshot(self, name):
        objects = []
        for kind in ('deployment', 'service'):
            value = self.get(kind, name)
            if value:
                objects.append(value)
        for kind in ('replicasets', 'pods'):
            objects.extend((self.get(kind, selector='app=' + name) or {}).get('items', []))
        return {f'{obj["kind"]}/{obj["metadata"]["name"]}': {
            'uid': obj['metadata']['uid'], 'owners': [ref['uid'] for ref in obj['metadata'].get('ownerReferences', [])]}
            for obj in objects}

    @staticmethod
    def check_owners(current, owned):
        roots = {value['uid'] for value in owned.values()}
        remaining = dict(current)
        for key in list(remaining):
            if key in owned:
                if remaining[key]['uid'] != owned[key]['uid']:
                    raise Indeterminate('Resource UID changed; refusing to adopt another workload')
                roots.add(remaining.pop(key)['uid'])
        while remaining:
            children = [key for key, value in remaining.items() if roots & set(value['owners'])]
            if not children:
                raise Indeterminate('Unexpected serving workload ownership')
            for key in children:
                roots.add(remaining.pop(key)['uid'])


class RunResources:
    def __init__(self, name, env, root, client):
        self.name = identifier(name)
        self.env, self.root, self.client = env, Path(root), client
        self.workloads = Workloads(env.kubectl, env.train_resources['NAMESPACE'])
        self.services, self.operations = [], []
        self.times = {}
        self.root.mkdir(parents=True, exist_ok=True)
        self.sql_log = self.root / 'statements.sql'
        self.status = 'running'

    def save(self):
        write_json(self.root / 'resources.json', {'model': self.name, 'namespace': self.workloads.namespace,
                                                'services': self.services, 'operations': self.operations, 'status': self.status})

    @contextmanager
    def stage(self, name):
        logging.info('%s: %s', self.name, name)
        start = time.monotonic()
        try:
            yield
        finally:
            self.times[name] = self.times.get(name, 0) + time.monotonic() - start

    def execute(self, sql, long=False, on_submitted=None, timeout=3600):
        with self.sql_log.open('a') as stream:
            stream.write(sql.rstrip(';') + ';\n\n')
        return self.client.execute(sql, timeout=timeout, poll_interval=1 if long else .1, on_submitted=on_submitted)

    def define(self, features, tasks, params, definition):
        if any(row[0] == self.name for row in self.execute('SHOW MODELS').rows):
            raise ValueError('Model name already exists')
        labels = [Feature(t.column, definition['columns'][t.column]['type']) for t in tasks]
        self.execute(f'CREATE MODEL `{self.name}` ({fields([*features, *labels])})\nWITH ({options(params)})')
        self.save()

    def checkpoint(self, checkpoint, timeout=3600):
        return metadata(self.execute(f'DESCRIBE FORMATTED MODEL `{self.name}` CHECKPOINT={literal(checkpoint)}', timeout=timeout))

    def checkpoint_operation(self, kind, checkpoints, checkpoint_type, sql):
        # HS2 waits for the server's checkpoint completion watcher. Its operation
        # handle identifies the submission; physical Job names are server internals.
        operation = {'kind': kind, 'checkpoints': checkpoints,
                     'checkpoint_type': checkpoint_type, 'status': 'submitted'}
        self.operations.append(operation)
        self.save()

        def submitted(operation_id):
            operation['operation_id'] = operation_id
            self.save()

        with self.stage(kind):
            self.execute(sql, long=True, on_submitted=submitted)
            self.verify_checkpoints(operation)
            operation['status'] = 'completed'
            self.save()

    def verify_checkpoints(self, operation, timeout=3600):
        info = {name: self.checkpoint(name, timeout=timeout) for name in operation['checkpoints']}
        operation['checkpoint_statuses'] = {name: item.get('Status:') for name, item in info.items()}
        self.save()
        if not all(item.get('Status:') == 'succeeded'
                   and item.get('Checkpoint Type:') == operation['checkpoint_type'] for item in info.values()):
            raise Indeterminate(f'{operation["kind"]} has no public succeeded '
                                f'{operation["checkpoint_type"]} checkpoint proof')

    def train_export(self, table, dssm=False):
        exports = ['v1_export/user', 'v1_export/item'] if dssm else ['v1_export']
        for kind, checkpoints in [('train', ['v1']), ('export', exports)]:
            self.checkpoint_operation(
                kind, checkpoints, 'origin' if kind == 'train' else 'export',
                f"{kind.upper()} MODEL `{self.name}`\nCHECKPOINT = 'v1'\nON `{table}`\n"
                f'WITH ({options(self.env.train_resources)})')
        return exports

    def deploy(self, checkpoints):
        services = {}
        with self.stage('deploy'):
            for checkpoint in checkpoints:
                tower = checkpoint.split('/')[-1] if '/' in checkpoint else 'scalar'
                name = self.name.replace('_', '-') + '-' + tower
                if len(name) > 63 or not re.fullmatch(r'[a-z]([-a-z0-9]*[a-z0-9])?', name):
                    raise ValueError(f'Invalid service name: {name!r}')
                existing = (self.workloads.get('deployment,service') or {}).get('items', [])
                if (any(row[0] == name for row in self.execute('SHOW SERVICES').rows)
                        or any(obj['metadata']['name'] == name for obj in existing)):
                    raise ValueError('Service name already exists')
                service = {'name': name, 'checkpoint': checkpoint, 'status': 'submitted'}
                self.services.append(service)
                self.save()
                self.execute(f'CREATE SERVICE `{name}`\nON MODEL `{self.name}`\nCHECKPOINT = {literal(checkpoint)}\n'
                             f'WITH ({options(self.env.service_resources)})', long=True)
                self.verify_service(service)
                physical = service['deployment']
                owned = self.workloads.snapshot(physical)
                if not any(key.startswith('Deployment/') for key in owned):
                    raise Indeterminate('No service Deployment identity')
                self.workloads.check_owners(owned, {key: value for key, value in owned.items()
                                                 if key.startswith(('Deployment/', 'Service/'))})
                service.update(status='created', owned=owned)
                self.save()
                deadline = time.monotonic() + 120
                while True:
                    deployment = self.workloads.get('deployment', physical)
                    if deployment and deployment['metadata']['uid'] == owned['Deployment/' + physical]['uid']:
                        if deployment.get('status', {}).get('availableReplicas', 0) >= deployment.get('spec', {}).get('replicas', 1):
                            break
                    if time.monotonic() >= deadline:
                        raise Indeterminate('Service readiness timed out')
                    time.sleep(1)
                services[tower] = name
        return services

    def verify_service(self, service, timeout=3600):
        info = metadata(self.execute(f'DESCRIBE FORMATTED SERVICE `{service["name"]}`', timeout=timeout))
        host = urlsplit(info.get('URL:', '')).hostname or ''
        suffix = '.' + self.workloads.namespace + '.svc.cluster.local'
        physical = host.removesuffix(suffix)
        if (info.get('Model Name:') != self.name or info.get('Checkpoint Name:') != service['checkpoint']
                or not host.endswith(suffix) or not re.fullmatch(r'[a-z0-9-]{1,63}', physical)
                or physical != service['name']
                or service.get('deployment', physical) != physical):
            raise Indeterminate('Service binding differs from this run')
        # The public URL must use the exact, validated Service name.
        service['deployment'] = physical
        self.save()

    def cleanup(self):
        """Release services and require public checkpoint completion before another model."""
        errors = []
        self.status = 'cleanup_pending'
        self.save()
        with self.stage('cleanup'):
            # An interrupted Thrift stream cannot be reused for cleanup.
            if getattr(self.client, 'transport_broken', False):
                self.client = HS2Client(self.env.endpoints['sqlrec']).connect()
                self.client.execute('SET table.sql-dialect = default', timeout=120)
            for service in self.services:
                try:
                    present = any(row[0] == service['name'] for row in self.execute('SHOW SERVICES', timeout=120).rows)
                    owned = service.get('owned', {})
                    if present:
                        self.verify_service(service, timeout=120)
                        current = self.workloads.snapshot(service['deployment'])
                        if not owned:
                            if not any(key.startswith('Deployment/') for key in current):
                                raise Indeterminate('Service creation is uncertain; cannot prove release')
                            owned = {key: value for key, value in current.items() if key.startswith(('Deployment/', 'Service/'))}
                        self.workloads.check_owners(current, owned)
                        owned.update(current)
                        service.update(owned=owned, status='dropping')
                        self.save()
                        self.execute(f'DROP SERVICE `{service["name"]}`', long=True, timeout=120)
                    elif service['status'] == 'submitted':
                        raise Indeterminate('Service submission outcome is unknown; absence is insufficient')
                    deadline = time.monotonic() + 120
                    while True:
                        current = self.workloads.snapshot(service['deployment'])
                        self.workloads.check_owners(current, owned)
                        owned.update(current)
                        present = any(row[0] == service['name'] for row in self.execute('SHOW SERVICES', timeout=120).rows)
                        if not current and not present:
                            break
                        if time.monotonic() >= deadline:
                            raise Indeterminate('Service release timed out')
                        time.sleep(1)
                    service['status'] = 'released'
                    self.save()
                except Exception as error:
                    errors.append(error)
                    service.update(status='cleanup_failed', error=str(error))
                    self.save()
            for operation in self.operations:
                try:
                    if operation['status'] != 'completed':
                        # A failed checkpoint can also mean partial submission;
                        # without completion proof, do not assume its Jobs stopped.
                        self.verify_checkpoints(operation, timeout=120)
                        operation['status'] = 'completed'
                except Exception as error:
                    errors.append(error)
                    operation['error'] = str(error)
            self.status = 'cleanup_failed' if errors else 'released'
            self.save()
        if errors:
            raise Indeterminate('Resource release is unproven; stopping this run') from errors[0]
