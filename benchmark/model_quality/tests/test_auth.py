"""Entry authentication, credential isolation and public client wire formats."""
import base64
from dataclasses import asdict
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from benchmark.model_quality import config
from benchmark.model_quality.client import APIClient, HS2Client, SQLFailed


class EntryAuthenticationTest(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, dict(NODE_IP='localhost', BENCH_STORAGE_ROOT='jfs://test/quality',
                                         AUTH_ENABLED='true', AUTH_USERNAME='用户',
                                         AUTH_PASSWORD=' 密码: secret $ \' " '), clear=True)
        env.start()
        self.addCleanup(env.stop)

    def test_shared_configuration_serializes_references_only(self):
        environment = config.environment()
        self.assertEqual(environment.endpoints['sqlrec']['auth'], 'CUSTOM')
        for name in ('sqlrec', 'api'):
            self.assertEqual(environment.endpoints[name]['username_env'], 'AUTH_USERNAME')
            self.assertEqual(environment.endpoints[name]['password_env'], 'AUTH_PASSWORD')
        serialized = json.dumps(asdict(environment), ensure_ascii=False)
        self.assertNotIn(os.environ['AUTH_USERNAME'], serialized)
        self.assertNotIn(os.environ['AUTH_PASSWORD'], serialized)
        self.assertNotIn('Authorization', serialized)
        self.assertEqual(environment.endpoints['warehouse']['auth'], 'NONE')
        self.assertNotIn('password_env', environment.endpoints['warehouse'])
        self.assertNotIn('password_env', environment.endpoints['milvus'])

    def test_anonymous_mode_never_includes_credential_references(self):
        with patch.dict(os.environ, AUTH_ENABLED='FaLsE', AUTH_USERNAME='', AUTH_PASSWORD=''):
            environment = config.environment()
        self.assertEqual(environment.endpoints['sqlrec']['auth'], 'NOSASL')
        self.assertNotIn('username_env', environment.endpoints['sqlrec'])
        self.assertNotIn('username_env', environment.endpoints['api'])

    def test_http_basic_is_utf8_and_does_not_retry_unauthorized(self):
        api = APIClient(config.environment().endpoints['api'])
        self.addCleanup(api.close)
        success = Mock(status_code=200)
        success.json.return_value = {'data': []}
        with patch.object(api.session, 'post', return_value=success) as post:
            self.assertEqual(api.call('score', {'data': {}}), [])
            header = post.call_args.kwargs['headers']['Authorization']
            decoded = base64.b64decode(header.removeprefix('Basic ')).decode('utf-8')
            self.assertEqual(decoded, os.environ['AUTH_USERNAME'] + ':' + os.environ['AUTH_PASSWORD'])
            self.assertIn('json', post.call_args.kwargs)
        with patch.object(api.session, 'post', return_value=Mock(status_code=401)) as post:
            with self.assertRaisesRegex(SQLFailed, 'HTTP 401'):
                api.call('score', {'data': {}})
            self.assertEqual(post.call_count, 1)

    def test_anonymous_http_and_explicit_authorization_compatibility(self):
        for endpoint, expected in (({'base_url': 'http://unused'}, {}),
                                   ({'base_url': 'http://unused', 'authorization_env': 'EXPLICIT_HEADER'},
                                    {'Authorization': 'Bearer custom'})):
            with self.subTest(endpoint=endpoint), patch.dict(os.environ, EXPLICIT_HEADER='Bearer custom'):
                api = APIClient(endpoint)
                self.addCleanup(api.close)
                success = Mock(status_code=200)
                success.json.return_value = {'data': []}
                with patch.object(api.session, 'post', return_value=success) as post:
                    api.call('score', {})
                    self.assertEqual(post.call_args.kwargs['headers'], expected)

    def test_thrift_custom_uses_plain_wire_credentials_and_session_username(self):
        endpoint = config.environment().endpoints['sqlrec']
        transport, rpc = Mock(), Mock()
        rpc.OpenSession.return_value = SimpleNamespace(status=SimpleNamespace(statusCode=0),
                                                       sessionHandle=object(), serverProtocolVersion=9)
        sasl_clients = []
        def make_transport(factory, mechanism, socket):
            sasl_clients.append(factory())
            self.assertEqual(mechanism, 'PLAIN')
            return transport
        with patch('thrift.transport.TSocket.TSocket'), \
             patch('thrift_sasl.TSaslClientTransport', side_effect=make_transport), \
             patch('TCLIService.TCLIService.Client', return_value=rpc), \
             patch.object(HS2Client, 'execute'):
            client = HS2Client(endpoint).connect()
            client.close()
        success, mechanism, response = sasl_clients[0].start('PLAIN')
        self.assertTrue(success)
        self.assertEqual(mechanism, 'PLAIN')
        # SASL PLAIN: authzid NUL authcid NUL password, encoded as UTF-8.
        self.assertEqual(response, ('\0' + os.environ['AUTH_USERNAME'] + '\0' + os.environ['AUTH_PASSWORD']).encode())
        self.assertEqual(rpc.OpenSession.call_args.args[0].username, os.environ['AUTH_USERNAME'])
        self.assertIsNone(rpc.OpenSession.call_args.args[0].password)
        transport.open.assert_called_once()
        transport.close.assert_called_once()

    def test_thrift_auth_failure_closes_transport_without_opening_session(self):
        transport, rpc = Mock(), Mock()
        transport.open.side_effect = RuntimeError('SASL authentication failed')
        with patch('thrift.transport.TSocket.TSocket'), \
             patch('thrift_sasl.TSaslClientTransport', return_value=transport), \
             patch('TCLIService.TCLIService.Client', return_value=rpc):
            with self.assertRaisesRegex(RuntimeError, 'SASL authentication failed'):
                HS2Client(config.environment().endpoints['sqlrec']).connect()
        transport.close.assert_called_once()
        rpc.OpenSession.assert_not_called()

    def test_pyhive_custom_authentication_and_v6_session(self):
        from pyhive import hive
        from TCLIService import ttypes

        transport, rpc = Mock(), Mock()
        rpc.OpenSession.return_value = SimpleNamespace(
            status=SimpleNamespace(statusCode=0), sessionHandle=object(),
            serverProtocolVersion=ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6)
        rpc.CloseSession.return_value = SimpleNamespace(status=SimpleNamespace(statusCode=0))
        with patch('thrift.transport.TSocket.TSocket'), \
             patch('thrift_sasl.TSaslClientTransport', return_value=transport) as sasl_transport, \
             patch('pyhive.hive.get_installed_sasl', side_effect=hive.get_pure_sasl_client), \
             patch('pyhive.hive.TCLIService.Client', return_value=rpc), \
             patch('pyhive.hive.Cursor.execute') as execute:
            connection = hive.Connection(
                host=os.environ['NODE_IP'], port=30000, auth='CUSTOM',
                username=os.environ['AUTH_USERNAME'], password=os.environ['AUTH_PASSWORD'])
            connection.close()
            factory, mechanism, _ = sasl_transport.call_args.args
            success, _, response = factory().start('PLAIN')
            self.assertTrue(success)
            self.assertEqual(mechanism, 'PLAIN')
            self.assertEqual(response, ('\0' + os.environ['AUTH_USERNAME'] + '\0' +
                                        os.environ['AUTH_PASSWORD']).encode('utf-8'))
            request = rpc.OpenSession.call_args.args[0]
            self.assertEqual(request.client_protocol, ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6)
            self.assertEqual(request.username, os.environ['AUTH_USERNAME'])
            execute.assert_called_once_with('USE `default`')
        transport.open.assert_called_once()
        transport.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
