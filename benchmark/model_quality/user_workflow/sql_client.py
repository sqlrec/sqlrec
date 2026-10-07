"""Standard HS2/Thrift client. No SQLRec internals and no model/connector SDKs."""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass

from .specs import identifier


class SQLFailed(RuntimeError):
    """A definitive server rejection or terminal failed operation."""


class Indeterminate(RuntimeError):
    """Submission/completion is uncertain: do not retry a mutation automatically."""


@dataclass
class SQLResult:
    columns: list[str]
    rows: list[list]
    operation_id: str = ''
    source: str = 'public_sql'

    def records(self):
        if len(set(self.columns)) != len(self.columns):
            raise SQLFailed('Duplicate result column names')
        if any(len(row) != len(self.columns) for row in self.rows):
            raise SQLFailed('Result schema/row width mismatch')
        return [dict(zip(self.columns, row)) for row in self.rows]


def resolve_environment(sql):
    """Secret placeholders stay in saved recipes; escape values only at execution."""
    def replace(match):
        key = match.group(1)
        if key not in os.environ:
            raise ValueError(f'Missing environment variable: {key}')
        return os.environ[key].replace("'", "''")
    return re.sub(r'\{\{env:([A-Z][A-Z0-9_]*)\}\}', replace, sql)


def decode_column(column):
    values = [v for k, v in vars(column).items() if k.endswith('Val') and v is not None]
    if len(values) != 1:
        raise SQLFailed('Expected one typed Thrift column')
    value = values[0]
    nulls = value.nulls or b''
    return [None if i // 8 < len(nulls) and nulls[i // 8] & (1 << (i % 8)) else v
            for i, v in enumerate(value.values)]


class HS2Client:
    """SQLRec negotiates V10; generated standard TCLI types support V6..V10."""
    def __init__(self, config):
        self.config, self.transport, self.client, self.session = config, None, None, None
        self.transport_broken = False

    def connect(self):
        from TCLIService import TCLIService, ttypes
        from thrift.protocol import TBinaryProtocol
        from thrift.transport import TSocket, TTransport
        socket = TSocket.TSocket(self.config['host'], int(self.config['port']))
        socket.setTimeout(int(self.config.get('rpc_timeout', 120) * 1000))
        auth = self.config.get('auth', 'NOSASL').upper()
        if auth == 'NOSASL':
            transport = TTransport.TBufferedTransport(socket)
        elif auth in ('NONE', 'LDAP'):
            from thrift_sasl import TSaslClientTransport
            from pyhive.hive import get_pure_sasl_client
            password = os.environ[self.config['password_env']] if auth == 'LDAP' else 'x'
            transport = TSaslClientTransport(lambda: get_pure_sasl_client(
                self.config['host'], 'PLAIN', username=self.config.get('username', 'quality_benchmark'),
                password=password), 'PLAIN', socket)
        else:
            raise ValueError('Supported HS2 auth: NOSASL, NONE, LDAP (password_env)')
        self.transport = transport
        try:
            transport.open()
            self.client = TCLIService.Client(TBinaryProtocol.TBinaryProtocol(transport))
            result = self.client.OpenSession(ttypes.TOpenSessionReq(
                client_protocol=ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
                username=self.config.get('username', 'quality_benchmark'),
                configuration=self.config.get('configuration', {})))
            self._check(result.status)
            if result.sessionHandle is None or not 5 <= result.serverProtocolVersion <= 9:
                raise SQLFailed('Unsupported HS2 session/protocol')
            self.session = result.sessionHandle
            self.execute('USE `' + identifier(self.config.get('database', 'default')) + '`')
            return self
        except Exception:
            transport.close()
            raise

    @staticmethod
    def _check(status):
        if status is None or status.statusCode not in (0, 1):
            raise SQLFailed(getattr(status, 'errorMessage', None) or 'HS2 request failed')

    def execute(self, sql, timeout=3600, on_submitted=None):
        from TCLIService import ttypes
        handle = None
        try:
            # A file terminator is a client delimiter (as in Beeline), not a
            # parser token. Each recipe file contains exactly one statement.
            statement = sql.strip().removesuffix(';').rstrip()
            response = self.client.ExecuteStatement(ttypes.TExecuteStatementReq(
                sessionHandle=self.session, statement=resolve_environment(statement), runAsync=True))
            self._check(response.status)
            handle = response.operationHandle
            if handle is None:
                raise Indeterminate('SQL submission returned no operation handle')
            operation_id = handle.operationId.guid.hex()
            if on_submitted:
                on_submitted(operation_id)
            deadline = time.monotonic() + timeout
            while True:
                state = self.client.GetOperationStatus(ttypes.TGetOperationStatusReq(operationHandle=handle))
                self._check(state.status)
                if state.operationState == ttypes.TOperationState.FINISHED_STATE:
                    break
                if state.operationState not in (ttypes.TOperationState.INITIALIZED_STATE,
                                               ttypes.TOperationState.PENDING_STATE,
                                               ttypes.TOperationState.RUNNING_STATE):
                    raise SQLFailed(state.errorMessage or f'Terminal SQL state: {state.operationState}')
                if time.monotonic() >= deadline:
                    raise Indeterminate(f'Operation {operation_id} still running after {timeout}s')
                time.sleep(min(1, max(0, deadline - time.monotonic())))
            if not handle.hasResultSet:
                return SQLResult([], [], operation_id)
            meta = self.client.GetResultSetMetadata(ttypes.TGetResultSetMetadataReq(operationHandle=handle))
            self._check(meta.status)
            names = [c.columnName for c in meta.schema.columns]
            rows = []
            while True:
                page = self.client.FetchResults(ttypes.TFetchResultsReq(
                    operationHandle=handle, orientation=ttypes.TFetchOrientation.FETCH_NEXT, maxRows=10000))
                self._check(page.status)
                if page.results.rows:
                    raise SQLFailed('Pre-V6 row-wise Thrift results are not supported')
                columns = [decode_column(c) for c in page.results.columns or []]
                if len(columns) != len(names) or len({len(c) for c in columns}) > 1:
                    raise SQLFailed('Malformed Thrift result columns')
                rows.extend([list(row) for row in zip(*columns)])
                # SQLRec consumes local ordinary SQL rows once, unlike Hive paging.
                if not page.hasMoreRows:
                    break
            return SQLResult(names, rows, operation_id)
        except SQLFailed as error:
            message = str(error).splitlines()[0][:1000]
            for key in re.findall(r'\{\{env:([A-Z][A-Z0-9_]*)\}\}', sql):
                secret = os.environ.get(key)
                if secret:
                    message = message.replace(secret, '[redacted]').replace(secret.replace("'", "''"), '[redacted]')
            raise SQLFailed(message) from None
        except (Indeterminate, ValueError):
            raise
        except BaseException as error:
            self.discard_transport()
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            raise Indeterminate(f'Public SQL transport interrupted: {type(error).__name__}') from error
        finally:
            if handle is not None and self.client is not None:
                try:
                    self.client.CloseOperation(ttypes.TCloseOperationReq(operationHandle=handle))
                except Exception:
                    self.discard_transport()  # Never reuse a stream with an unread RPC response.

    def discard_transport(self):
        """An interrupted RPC cannot safely carry cleanup SQL or CloseSession."""
        self.transport_broken = True
        try:
            if self.transport:
                self.transport.close()
        finally:
            self.transport = self.client = self.session = None

    def close(self):
        if self.transport:
            try:
                if self.session:
                    from TCLIService import ttypes
                    self.client.CloseSession(ttypes.TCloseSessionReq(sessionHandle=self.session))
            finally:
                self.transport.close()


class APIClient:
    def __init__(self, config):
        import requests
        self.config, self.session = config, requests.Session()

    def call(self, name, payload):
        identifier(name)
        headers = {}
        if self.config.get('authorization_env'):
            headers['Authorization'] = os.environ[self.config['authorization_env']]
        # Generated scoring/recall functions are read-only. Newly deployed
        # metadata/service caches can briefly lag readiness; retry the same
        # public request, retaining every status, with no prediction fallback.
        deadline = time.monotonic() + self.config.get('readiness_timeout', 120)
        self.last_call = {'api': name, 'attempt_statuses': []}
        while True:
            response = self.session.post(self.config['base_url'].rstrip('/') + '/api/v1/' + name,
                                         json=payload, headers=headers,
                                         timeout=min(self.config.get('timeout', 120), max(1, deadline-time.monotonic())))
            self.last_call['attempt_statuses'].append(response.status_code)
            if response.status_code in (500, 502, 503, 504) and time.monotonic() < deadline:
                time.sleep(1)
                continue
            if response.status_code != 200:
                raise SQLFailed(f'Public SQL API failed with HTTP {response.status_code}')
            result = response.json()
            if not isinstance(result.get('data'), list) or not all(isinstance(r, dict) for r in result['data']):
                raise SQLFailed('Public SQL API must return a data row array')
            return result['data']

    def close(self):
        self.session.close()
