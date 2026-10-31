"""GrowthBook 4.4.0 HTTP APIs, including local-installation bootstrap."""
from datetime import datetime, timezone
import json
import logging
import os
import time

import requests

from benchmark.model_quality.common import write_json


class GrowthBook:
    def __init__(self, host, root, prefix, timeout=300):
        if timeout <= 0:
            raise ValueError('GrowthBook readiness timeout must be positive')
        self.host, self.root, self.prefix, self.timeout = host.rstrip('/'), root, prefix, timeout
        self.session = requests.Session()
        self.jwt = self.token = None
        self.owner = ''
        self.secrets = []
        self.owned = []
        self.resources = {'organization': None, 'project': None, 'experiments': [], 'features': [],
                          'sdk_connection': None, 'cleanup': 'pending'}

    def save(self):
        write_json(self.root / 'resources.json', self.resources)

    def redact(self, value):
        text = str(value)
        for secret in self.secrets:
            if secret:
                text = text.replace(secret, '[redacted]')
        return text

    def record_uncertain_write(self, method, path):
        if method not in ('GET', 'HEAD'):
            self.resources.setdefault('uncertain_writes', []).append({'method': method, 'path': path})
            self.save()

    def request(self, method, path, payload=None, *, auth='api'):
        headers = {}
        if auth == 'session':
            headers['Authorization'] = 'Bearer ' + self.jwt
            if self.resources['organization']:
                headers['X-Organization'] = self.resources['organization']
        elif auth == 'api':
            headers['Authorization'] = 'Bearer ' + self.token
        try:
            response = self.session.request(method, self.host + path, json=payload, headers=headers, timeout=30)
        except requests.RequestException as error:
            self.record_uncertain_write(method, path)
            raise RuntimeError(f'{method} {path}: transport failed; writes are not retried') from error
        logging.info('GrowthBook %s %s: HTTP %s', method, path, response.status_code)
        if response.status_code >= 500:
            self.record_uncertain_write(method, path)
        try:
            data = response.json()
        except ValueError:
            raise RuntimeError(f'{method} {path}: non-JSON response (HTTP {response.status_code})') from None
        if (response.status_code not in (200, 201) or not isinstance(data, dict)
                or isinstance(data.get('status'), int) and data['status'] >= 400):
            message = data.get('message', 'request failed') if isinstance(data, dict) else 'invalid response'
            raise RuntimeError(self.redact(f'{method} {path}: HTTP {response.status_code}: {message}'))
        return data

    def wait_ready(self):
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self.request('GET', '/healthcheck', auth=None)
                return
            except (RuntimeError, requests.RequestException):
                if time.monotonic() >= deadline:
                    raise RuntimeError('GrowthBook API did not become ready before the timeout') from None
                time.sleep(2)

    def authenticate(self):
        email = os.environ.get('GROWTHBOOK_BENCH_EMAIL', 'sqlrec-benchmark@example.invalid')
        password = os.environ.get('GROWTHBOOK_BENCH_PASSWORD', 'SqlrecBenchmark123!')
        self.secrets.append(password)
        has_orgs = self.request('GET', '/auth/hasorgs', auth=None)['hasOrganizations']
        payload = {'email': email, 'password': password}
        if has_orgs:
            result = self.request('POST', '/auth/login', payload, auth=None)
        else:
            result = self.request('POST', '/auth/firsttime',
                                  {**payload, 'name': 'SQLRec benchmark', 'companyname': 'SQLRec Benchmark'}, auth=None)
        self.jwt = result['token']
        self.secrets.append(self.jwt)
        account = self.request('GET', '/user', auth='session')
        self.owner = account['userId']
        organizations = account['organizations']
        requested = os.environ.get('GROWTHBOOK_BENCH_ORGANIZATION')
        if requested:
            organizations = [org for org in organizations if org['id'] == requested]
        if len(organizations) != 1:
            raise ValueError('Select one organization with GROWTHBOOK_BENCH_ORGANIZATION')
        self.resources['organization'] = organizations[0]['id']
        self.save()
        result = self.request('POST', '/keys', {'type': 'admin', 'description': self.prefix,
                                               'limitAccessByEnvironment': False, 'environments': []}, auth='session')
        self.token = result['key']['key']
        self.secrets.append(self.token)

    def remember(self, method, path, auth='api'):
        self.owned.append((method, path, auth))

    def provision(self, experiments):
        project = self.request('POST', '/api/v1/projects', {'name': 'SQLRec UDF ' + self.prefix})['project']
        self.resources['project'] = project['id']
        self.remember('DELETE', '/api/v1/projects/' + project['id'])
        self.save()
        environment = os.environ.get('GROWTHBOOK_BENCH_ENVIRONMENT', 'production')
        for exp in experiments:
            payload = {'name': 'SQLRec ' + exp.name, 'trackingKey': exp.key, 'project': project['id'],
                       'assignmentQueryId': '', 'status': 'running', 'autoRefresh': False,
                       'hashAttribute': 'id', 'hashVersion': 2, 'disableStickyBucketing': True,
                       'variations': [{'key': 'control', 'name': 'Control'}, {'key': 'treatment', 'name': 'Treatment'}],
                       'phases': [{'name': 'Benchmark', 'dateStarted': datetime.now(timezone.utc).isoformat(),
                                   'coverage': exp.coverage, 'variationWeights': exp.weights, 'seed': exp.seed,
                                   'condition': json.dumps(exp.condition)}]}
            created = self.request('POST', '/api/v1/experiments', payload)['experiment']
            self.resources['experiments'].append({'id': created['id'], 'key': exp.key, 'feature': exp.feature})
            self.remember('DELETE', '/experiment/' + created['id'], 'session')
            self.save()
            feature = {'id': exp.feature, 'owner': self.owner, 'project': project['id'],
                       'valueType': 'string', 'defaultValue': 'fallback',
                       'environments': {environment: {'enabled': True, 'rules': [
                           {'type': 'experiment-ref', 'experimentId': created['id'],
                            'variations': [{'variationId': variation['variationId'], 'value': exp.values[index]}
                                           for index, variation in enumerate(created['variations'])]}]}}}
            self.request('POST', '/api/v1/features', feature)
            self.resources['features'].append(exp.feature)
            self.remember('DELETE', '/api/v1/features/' + exp.feature)
            self.save()
        default = self.prefix + '_default'
        self.request('POST', '/api/v1/features', {'id': default, 'owner': self.owner,
                                                 'project': project['id'], 'valueType': 'string',
                                                 'defaultValue': 'plain_default',
                                                 'environments': {environment: {'enabled': True, 'rules': []}}})
        self.resources['features'].append(default)
        self.remember('DELETE', '/api/v1/features/' + default)
        self.save()
        connection = self.request('POST', '/api/v1/sdk-connections', {
            'name': 'SQLRec UDF ' + self.prefix, 'language': 'java',
            'environment': environment, 'projects': [project['id']], 'encryptPayload': False,
        })['sdkConnection']
        self.resources['sdk_connection'] = connection['id']
        self.remember('DELETE', '/api/v1/sdk-connections/' + connection['id'])
        self.save()
        return connection['key']

    def wait_payload(self, key, experiments):
        deadline = time.monotonic() + self.timeout
        while True:
            payload = self.request('GET', '/api/features/' + key, auth=None)
            features = payload.get('features', {})
            if all(any(rule.get('key') == exp.key for rule in features.get(exp.feature, {}).get('rules', []))
                   for exp in experiments) and self.prefix + '_default' in features:
                for exp in experiments:
                    rule = next(rule for rule in features[exp.feature]['rules'] if rule.get('key') == exp.key)
                    if (rule.get('variations') != list(exp.values) or rule.get('hashAttribute') != 'id'
                            or rule.get('hashVersion') != 2 or rule.get('seed') != exp.seed):
                        raise ValueError('Published SDK experiment differs from the API-created configuration')
                write_json(self.root / 'sdk-payload.json', payload)
                return
            if time.monotonic() >= deadline:
                raise RuntimeError('Created experiments did not appear in the SDK feature payload')
            time.sleep(1)

    def cleanup(self):
        errors = []
        for method, path, auth in reversed(self.owned):
            try:
                if method == 'DELETE' and path.startswith('/api/v1/features/'):
                    self.request('POST', path, {'archived': True}, auth=auth)
                self.request(method, path, auth=auth)
            except Exception as error:
                errors.append(self.redact(error))
        if self.token:
            try:
                self.request('DELETE', '/keys', {'key': self.token}, auth='session')
            except Exception as error:
                errors.append(self.redact(error))
        if self.resources.get('uncertain_writes'):
            errors.append('A write outcome is unknown; inspect uncertain_writes and the unique run prefix')
        self.resources['cleanup'] = 'failed' if errors else 'completed'
        self.resources['cleanup_errors'] = errors
        self.save()
        if errors:
            raise RuntimeError('; '.join(errors))

    def close(self):
        self.session.close()
