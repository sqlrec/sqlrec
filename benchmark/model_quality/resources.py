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
from .data import fields, options


def kubernetes_name(name):
    return re.sub(r'[.-]+', '-', re.sub(r'[^a-z0-9.-]', '-', name.lower()).strip('.-'))[:63].rstrip('.-')


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
        self.services, self.jobs = [], []
        self.times = {}
        self.root.mkdir(parents=True, exist_ok=True)
        self.sql_log = self.root / 'statements.sql'
        self.status = 'running'

    def save(self):
        write_json(self.root / 'resources.json', {'model': self.name, 'namespace': self.workloads.namespace,
                                                'services': self.services, 'jobs': self.jobs, 'status': self.status})

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
        labels = ', '.join(f'`{t.column}` {definition["columns"][t.column]["type"]}' for t in tasks)
        self.execute(f'CREATE MODEL `{self.name}` ({fields(features)}, {labels}) WITH ({options(params)})')
        self.save()

    def checkpoint(self, checkpoint, timeout=3600):
        return metadata(self.execute(f'DESCRIBE FORMATTED MODEL `{self.name}` CHECKPOINT={literal(checkpoint)}', timeout=timeout))

    def capture_job(self, job):
        deadline = time.monotonic() + 10
        while True:
            value = self.workloads.get('job', job['name'])
            if value:
                uid = value['metadata']['uid']
                if job.get('uid') and job['uid'] != uid:
                    raise Indeterminate('Training/export Job UID changed')
                job['uid'] = uid
                self.save()
                return
            if time.monotonic() >= deadline:
                raise Indeterminate('Submitted Job identity could not be captured')
            time.sleep(1)

    def train_export(self, table, dssm=False):
        exports = ['v1_export/user', 'v1_export/item'] if dssm else ['v1_export']
        for kind, checkpoints in [('train', ['v1']), ('export', exports)]:
            suffix = '-v1' if kind == 'train' else '-v1-export'
            job = {'name': kubernetes_name(self.name + suffix) + '-job', 'kind': kind,
                   'checkpoints': checkpoints, 'status': 'submitted'}
            if self.workloads.get('job', job['name']) or (self.workloads.get('pods', selector='job-name=' + job['name']) or {}).get('items'):
                raise ValueError('Training/export workload name already exists')
            self.jobs.append(job)
            self.save()
            with self.stage(kind):
                self.execute(f"{kind.upper()} MODEL `{self.name}` CHECKPOINT='v1' ON `{table}` "
                             f'WITH ({options(self.env.train_resources)})', long=True,
                             on_submitted=lambda operation: self.capture_job(job))
                expected = 'origin' if kind == 'train' else 'export'
                for checkpoint in checkpoints:
                    info = self.checkpoint(checkpoint)
                    if info.get('Status:') != 'succeeded' or info.get('Checkpoint Type:') != expected:
                        raise Indeterminate('Training/export has no public succeeded checkpoint proof')
                job['status'] = 'completed'
                self.save()
        return exports

    def deploy(self, checkpoints):
        services = {}
        with self.stage('deploy'):
            for checkpoint in checkpoints:
                tower = checkpoint.split('/')[-1] if '/' in checkpoint else 'scalar'
                name = identifier(self.name + '_' + tower)
                prefix = kubernetes_name(name)[:42].rstrip('-')
                existing = (self.workloads.get('deployment,service') or {}).get('items', [])
                if (any(row[0] == name for row in self.execute('SHOW SERVICES').rows)
                        or any(obj['metadata']['name'] == prefix or obj['metadata']['name'].startswith(prefix + '-') for obj in existing)):
                    raise ValueError('Service name already exists')
                service = {'name': name, 'checkpoint': checkpoint, 'status': 'submitted'}
                self.services.append(service)
                self.save()
                self.execute(f'CREATE SERVICE `{name}` ON MODEL `{self.name}` CHECKPOINT={literal(checkpoint)} '
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
        prefix = kubernetes_name(service['name'])[:42].rstrip('-')
        if (info.get('Model Name:') != self.name or info.get('Checkpoint Name:') != service['checkpoint']
                or not host.endswith(suffix) or not re.fullmatch(r'[a-z0-9-]{1,63}', physical)
                or not (physical == prefix or physical.startswith(prefix + '-'))
                or service.get('deployment', physical) != physical):
            raise Indeterminate('Service binding differs from this run')
        # Public metadata is authoritative; do not copy the server's naming algorithm.
        service['deployment'] = physical
        self.save()

    def cleanup(self):
        """Try both DSSM services, then verify Jobs are quiet before another model."""
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
            for job in self.jobs:
                try:
                    value = self.workloads.get('job', job['name'])
                    pods = (self.workloads.get('pods', selector='job-name=' + job['name']) or {}).get('items', [])
                    if value and job.get('uid') != value['metadata']['uid']:
                        raise Indeterminate('Job ownership is unverified')
                    terminal = value is None or any(c.get('status') == 'True' and c.get('type') in ('Complete', 'Failed')
                                                   for c in value.get('status', {}).get('conditions', []))
                    if job['status'] != 'completed' and not (value and terminal and job.get('uid')):
                        info = [self.checkpoint(checkpoint, timeout=120) for checkpoint in job['checkpoints']]
                        if not all(item.get('Status:') in ('succeeded', 'failed') for item in info):
                            raise Indeterminate('Submitted Job outcome is uncertain')
                    if not terminal or any(p.get('status', {}).get('phase') not in ('Succeeded', 'Failed') for p in pods):
                        raise Indeterminate('Training/export workload remains active')
                    job['quiet'] = True
                except Exception as error:
                    errors.append(error)
                    job['error'] = str(error)
            self.status = 'cleanup_failed' if errors else 'released'
            self.save()
        if errors:
            raise Indeterminate('Resource release is unproven; stopping this run') from errors[0]
