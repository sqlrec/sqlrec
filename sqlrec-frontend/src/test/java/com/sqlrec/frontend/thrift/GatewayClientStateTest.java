package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Exercises remote state and handle ownership without opening a network connection. */
class GatewayClientStateTest {
    @Test
    void lostConnectionDuringStateCleanupRequiresReplayBeforeTheNextSql() throws Exception {
        try (Remote remote = new Remote()) {
            when(remote.client.CloseOperation(any())).thenThrow(new TTransportException("cleanup connection lost"));
            TException failure = assertThrows(TException.class, () -> remote.proxy.executeStatement(
                    new TExecuteStatementReq(remote.session, "SELECT 1")));
            assertTrue(failure.getMessage().startsWith("FLINK_SESSION_STATE_SYNC_FAILED"));
            ArgumentCaptor<TExecuteStatementReq> first = ArgumentCaptor.forClass(TExecuteStatementReq.class);
            verify(remote.client).ExecuteStatement(first.capture());
            assertEquals("USE `default`", first.getValue().getStatement());

            remote.proxy.executeStatement(new TExecuteStatementReq(remote.session, "SELECT 1"));
            assertEquals(2, remote.sockets.constructed().size());
            TCLIService.Client reconnected = remote.clients.constructed().get(1);
            ArgumentCaptor<TExecuteStatementReq> replay = ArgumentCaptor.forClass(TExecuteStatementReq.class);
            verify(reconnected, times(2)).ExecuteStatement(replay.capture());
            assertEquals(java.util.List.of("USE `default`", "SELECT 1"),
                    replay.getAllValues().stream().map(TExecuteStatementReq::getStatement).toList());
        }
    }

    @Test
    void stateIsEscapedAndAppliedBeforeSqlOnlyWhenItChanges() throws Exception {
        try (Remote remote = new Remote()) {
            java.util.Map<String, String> settings = new java.util.LinkedHashMap<>();
            settings.put("custom.key", "it's quoted");
            settings.put("table.sql-dialect", "default");
            remote.proxy.setSessionState("db`name", settings);
            remote.proxy.executeStatement(new TExecuteStatementReq(remote.session, "SELECT 1"));
            remote.proxy.executeStatement(new TExecuteStatementReq(remote.session, "SELECT 2"));
            ArgumentCaptor<TExecuteStatementReq> requests = ArgumentCaptor.forClass(TExecuteStatementReq.class);
            verify(remote.client, times(5)).ExecuteStatement(requests.capture());
            assertEquals(java.util.List.of("SET 'table.sql-dialect' = 'default'",
                    "SET 'custom.key' = 'it''s quoted'", "USE `db``name`", "SELECT 1", "SELECT 2"),
                    requests.getAllValues().stream().map(TExecuteStatementReq::getStatement).toList());
        }
    }

    @Test
    void resetBypassesPendingOverridesAndCleanupFailureDoesNotUndoSuccess() throws Exception {
        try (Remote remote = new Remote()) {
            remote.proxy.setSessionState("default", java.util.Map.of("invalid.setting", "bad"));
            when(remote.client.CloseOperation(any())).thenReturn(new TCloseOperationResp(error("close failed")));

            assertDoesNotThrow(() -> remote.proxy.executeSessionCommand("RESET 'invalid.setting'"));

            ArgumentCaptor<TExecuteStatementReq> statements = ArgumentCaptor.forClass(TExecuteStatementReq.class);
            verify(remote.client).ExecuteStatement(statements.capture());
            assertEquals("RESET 'invalid.setting'", statements.getValue().getStatement());
        }
    }

    @Test
    void cleanupFailurePreservesTheOriginalCommandError() throws Exception {
        try (Remote remote = new Remote()) {
            TGetOperationStatusResp failed = new TGetOperationStatusResp(success());
            failed.setOperationState(TOperationState.ERROR_STATE);
            failed.setErrorMessage("invalid setting value");
            when(remote.client.GetOperationStatus(any())).thenReturn(failed);
            when(remote.client.CloseOperation(any())).thenReturn(new TCloseOperationResp(error("cleanup error")));

            TException failure = assertThrows(TException.class,
                    () -> remote.proxy.executeSessionCommand("RESET"));
            assertTrue(failure.getMessage().contains("invalid setting value"));
            assertFalse(failure.getMessage().contains("cleanup error"));
        }
    }

    @Test
    void failedStateSynchronizationKeepsExistingOperationsUsable() throws Exception {
        try (Remote remote = new Remote()) {
            TOperationHandle previous = remote.proxy.executeStatement(
                    new TExecuteStatementReq(remote.session, "SELECT 1")).getOperationHandle();
            remote.proxy.setSessionState("default", java.util.Map.of("invalid.setting", "bad"));
            when(remote.client.ExecuteStatement(any())).thenReturn(new TExecuteStatementResp(error("invalid value")));

            TException failure = assertThrows(TException.class, () -> remote.proxy.executeStatement(
                    new TExecuteStatementReq(remote.session, "SELECT 2")));
            assertTrue(failure.getMessage().startsWith("FLINK_SESSION_STATE_SYNC_FAILED"));
            assertEquals(TOperationState.FINISHED_STATE,
                    remote.proxy.getOperationStatus(new TGetOperationStatusReq(previous)).getOperationState());
            verify(remote.sockets.constructed().get(0), never()).close();
        }
    }

    @Test
    void crossReferenceHandlesSupportFetchAndBecomeInvalidAfterClose() throws Exception {
        try (Remote remote = new Remote()) {
            TGetCrossReferenceReq request = new TGetCrossReferenceReq();
            request.setSessionHandle(remote.session);
            TSessionHandle original = remote.session.deepCopy();
            TOperationHandle handle = remote.proxy.getCrossReference(request).getOperationHandle();
            assertEquals(original, request.getSessionHandle());
            assertDoesNotThrow(() -> remote.proxy.fetchResults(
                    new TFetchResultsReq(handle, TFetchOrientation.FETCH_NEXT, 10)));
            remote.proxy.closeOperation(new TCloseOperationReq(handle));
            assertThrows(TException.class, () -> remote.proxy.getOperationStatus(new TGetOperationStatusReq(handle)));
            assertEquals(1, remote.sockets.constructed().size());
        }
    }

    @Test
    void transportFailureInvalidatesOldHandlesWithoutReconnectingForFetch() throws Exception {
        try (Remote remote = new Remote()) {
            TOperationHandle handle = remote.proxy.executeStatement(
                    new TExecuteStatementReq(remote.session, "SELECT 1")).getOperationHandle();
            when(remote.client.GetOperationStatus(any())).thenThrow(new TTransportException("connection lost"));
            assertThrows(TTransportException.class,
                    () -> remote.proxy.getOperationStatus(new TGetOperationStatusReq(handle)));
            assertThrows(TException.class, () -> remote.proxy.fetchResults(
                    new TFetchResultsReq(handle, TFetchOrientation.FETCH_NEXT, 10)));
            assertEquals(1, remote.sockets.constructed().size());
            verify(remote.sockets.constructed().get(0)).close();
        }
    }

    @Test
    void unsuccessfulExecuteResponseDoesNotRegisterItsHandle() throws Exception {
        try (Remote remote = new Remote()) {
            remote.proxy.executeStatement(new TExecuteStatementReq(remote.session, "SELECT 1"));
            TOperationHandle handle = operation();
            TExecuteStatementResp failed = new TExecuteStatementResp(error("execution rejected"));
            failed.setOperationHandle(handle);
            when(remote.client.ExecuteStatement(any())).thenReturn(failed);
            remote.proxy.executeStatement(new TExecuteStatementReq(remote.session, "SELECT 2"));
            assertThrows(TException.class,
                    () -> remote.proxy.getOperationStatus(new TGetOperationStatusReq(handle)));
        }
    }

    @Test
    void forwardsACopyOfTheRequestWithTheRemoteHandle() throws Exception {
        try (Remote remote = new Remote()) {
            TExecuteStatementReq request = new TExecuteStatementReq(remote.session, "SELECT 1");
            request.setConfOverlay(java.util.Map.of("key", "value"));
            TExecuteStatementReq original = request.deepCopy();
            remote.proxy.executeStatement(request);
            ArgumentCaptor<TExecuteStatementReq> forwarded = ArgumentCaptor.forClass(TExecuteStatementReq.class);
            verify(remote.client, times(2)).ExecuteStatement(forwarded.capture());
            TExecuteStatementReq sql = forwarded.getAllValues().get(1);
            assertNotSame(request, sql);
            assertNotEquals(original.getSessionHandle(), sql.getSessionHandle());
            assertEquals(original.getConfOverlay(), sql.getConfOverlay());
            assertEquals(original, request);
        }
    }

    @Test
    void transportFailureDoesNotRetrySqlAndTheNextSqlReplaysState() throws Exception {
        try (Remote remote = new Remote()) {
            remote.proxy.executeStatement(new TExecuteStatementReq(remote.session, "SELECT 1"));
            clearInvocations(remote.client);
            when(remote.client.ExecuteStatement(any())).thenThrow(new TTransportException("response lost"));
            TExecuteStatementReq request = new TExecuteStatementReq(remote.session, "INSERT INTO items VALUES (1)");
            TExecuteStatementReq original = request.deepCopy();
            assertThrows(TTransportException.class, () -> remote.proxy.executeStatement(request));
            verify(remote.client).ExecuteStatement(any());
            assertEquals(1, remote.sockets.constructed().size());
            assertEquals(original, request);

            remote.proxy.executeStatement(new TExecuteStatementReq(remote.session, "SELECT 2"));
            assertEquals(2, remote.sockets.constructed().size());
            ArgumentCaptor<TExecuteStatementReq> replay = ArgumentCaptor.forClass(TExecuteStatementReq.class);
            verify(remote.clients.constructed().get(1), times(2)).ExecuteStatement(replay.capture());
            assertEquals(java.util.List.of("USE `default`", "SELECT 2"),
                    replay.getAllValues().stream().map(TExecuteStatementReq::getStatement).toList());
        }
    }

    @Test
    void waitsThroughTransientStatesAndClosesTheSameOperationAfterCompletion() throws Exception {
        try (Remote remote = new Remote()) {
            TOperationHandle handle = operation();
            TExecuteStatementResp response = new TExecuteStatementResp(success());
            response.setOperationHandle(handle);
            when(remote.client.ExecuteStatement(any())).thenReturn(response);
            when(remote.client.GetOperationStatus(any())).thenReturn(
                    operationStatus(TOperationState.INITIALIZED_STATE),
                    operationStatus(TOperationState.PENDING_STATE),
                    operationStatus(TOperationState.RUNNING_STATE),
                    operationStatus(TOperationState.FINISHED_STATE));

            remote.proxy.executeSessionCommand("RESET");

            ArgumentCaptor<TExecuteStatementReq> submitted = ArgumentCaptor.forClass(TExecuteStatementReq.class);
            var order = inOrder(remote.client);
            order.verify(remote.client).ExecuteStatement(submitted.capture());
            assertFalse(submitted.getValue().isRunAsync());
            order.verify(remote.client, times(4)).GetOperationStatus(
                    argThat(request -> handle.equals(request.getOperationHandle())));
            order.verify(remote.client).CloseOperation(
                    argThat(request -> handle.equals(request.getOperationHandle())));
        }
    }

    @Test
    void timeoutStillPollsOnceAndClosesTheOperation() throws Exception {
        try (Remote remote = new Remote()) {
            SqlRecConfigs.FLINK_SQL_GATEWAY_CONNECT_TIMEOUT.setDefaultValue(0);
            when(remote.client.GetOperationStatus(any()))
                    .thenReturn(operationStatus(TOperationState.RUNNING_STATE));

            TException failure = assertThrows(TException.class,
                    () -> remote.proxy.executeSessionCommand("RESET"));

            assertEquals("Gateway session state synchronization timed out", failure.getMessage());
            verify(remote.client).GetOperationStatus(any());
            verify(remote.client).CloseOperation(any());
        }
    }

    @Test
    void finishedStateSucceedsEvenWhenTheDeadlineHasPassed() throws Exception {
        try (Remote remote = new Remote()) {
            SqlRecConfigs.FLINK_SQL_GATEWAY_CONNECT_TIMEOUT.setDefaultValue(0);

            assertDoesNotThrow(() -> remote.proxy.executeSessionCommand("RESET"));

            verify(remote.client).GetOperationStatus(any());
            verify(remote.client).CloseOperation(any());
        }
    }

    @Test
    void pollingInterruptionRestoresTheInterruptFlagAndStillClosesTheOperation() throws Exception {
        try (Remote remote = new Remote()) {
            when(remote.client.GetOperationStatus(any())).thenAnswer(call -> {
                Thread.currentThread().interrupt();
                return operationStatus(TOperationState.RUNNING_STATE);
            });

            TException failure = assertThrows(TException.class,
                    () -> remote.proxy.executeSessionCommand("RESET"));

            assertEquals("Gateway state synchronization interrupted", failure.getMessage());
            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
            verify(remote.client).CloseOperation(any());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void rejectedStateSubmissionDoesNotPollOrCloseItsReturnedHandle() throws Exception {
        try (Remote remote = new Remote()) {
            TExecuteStatementResp rejected = new TExecuteStatementResp(error("rejected"));
            rejected.setOperationHandle(operation());
            when(remote.client.ExecuteStatement(any())).thenReturn(rejected);

            assertEquals("rejected", assertThrows(TException.class,
                    () -> remote.proxy.executeSessionCommand("RESET")).getMessage());
            verify(remote.client, never()).GetOperationStatus(any());
            verify(remote.client, never()).CloseOperation(any());
        }
    }

    @Test
    void missingStateOperationHandleFailsWithoutPollingOrCleanup() throws Exception {
        try (Remote remote = new Remote()) {
            when(remote.client.ExecuteStatement(any())).thenReturn(new TExecuteStatementResp(success()));

            assertEquals("Gateway state statement returned no operation handle", assertThrows(TException.class,
                    () -> remote.proxy.executeSessionCommand("RESET")).getMessage());
            verify(remote.client, never()).GetOperationStatus(any());
            verify(remote.client, never()).CloseOperation(any());
        }
    }

    @Test
    void transportFailureWhilePollingDisconnectsWithoutClosingOrRetryingTheOperation() throws Exception {
        try (Remote remote = new Remote()) {
            TTransportException failure = new TTransportException("poll connection lost");
            when(remote.client.GetOperationStatus(any())).thenThrow(failure);

            assertSame(failure, assertThrows(TTransportException.class,
                    () -> remote.proxy.executeSessionCommand("RESET")));
            verify(remote.client).ExecuteStatement(any());
            verify(remote.client).GetOperationStatus(any());
            verify(remote.client, never()).CloseOperation(any());
            verify(remote.sockets.constructed().get(0)).close();
            assertEquals(1, remote.sockets.constructed().size());
        }
    }

    private static TGetOperationStatusResp operationStatus(TOperationState state) {
        TGetOperationStatusResp response = new TGetOperationStatusResp(success());
        response.setOperationState(state);
        return response;
    }

    private static TStatus success() {
        return new TStatus(TStatusCode.SUCCESS_STATUS);
    }

    private static TStatus error(String message) {
        TStatus status = new TStatus(TStatusCode.ERROR_STATUS);
        status.setErrorMessage(message);
        return status;
    }

    private static TOperationHandle operation() {
        return new TOperationHandle(ThriftUtils.getHandleIdentifier(), TOperationType.EXECUTE_STATEMENT, true);
    }

    private static final class Remote implements AutoCloseable {
        private final String oldAddress = SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getDefaultValue();
        private final String oldSchemaDir = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        private final int oldTimeout = SqlRecConfigs.FLINK_SQL_GATEWAY_CONNECT_TIMEOUT.getDefaultValue();
        private final MockedConstruction<TSocket> sockets;
        private final MockedConstruction<TCLIService.Client> clients;
        private final GatewayClient proxy = new GatewayClient(new TOpenSessionReq());
        private final TSessionHandle session;
        private final TCLIService.Client client;

        private Remote() throws TException {
            SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue("gateway.invalid");
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue("");
            sockets = mockConstruction(TSocket.class, (socket, context) -> when(socket.isOpen()).thenReturn(true));
            clients = mockConstruction(TCLIService.Client.class, (mock, context) -> {
                TOpenSessionResp open = new TOpenSessionResp(success(), TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);
                open.setSessionHandle(new TSessionHandle(ThriftUtils.getHandleIdentifier()));
                when(mock.OpenSession(any())).thenReturn(open);
                when(mock.GetDelegationToken(any())).thenReturn(new TGetDelegationTokenResp(success()));
                when(mock.ExecuteStatement(any())).thenAnswer(invocation -> {
                    TExecuteStatementResp response = new TExecuteStatementResp(success());
                    response.setOperationHandle(operation());
                    return response;
                });
                TGetOperationStatusResp status = new TGetOperationStatusResp(success());
                status.setOperationState(TOperationState.FINISHED_STATE);
                when(mock.GetOperationStatus(any())).thenReturn(status);
                when(mock.CloseOperation(any())).thenReturn(new TCloseOperationResp(success()));
                when(mock.FetchResults(any())).thenReturn(new TFetchResultsResp(success()));
                TGetCrossReferenceResp cross = new TGetCrossReferenceResp(success());
                cross.setOperationHandle(operation());
                when(mock.GetCrossReference(any())).thenReturn(cross);
            });
            session = new TSessionHandle(ThriftUtils.getHandleIdentifier());
            TGetDelegationTokenReq request = new TGetDelegationTokenReq();
            request.setSessionHandle(session);
            proxy.getDelegationToken(request); // Establish a connection without state replay.
            client = clients.constructed().get(0);
            clearInvocations(client);
        }

        @Override
        public void close() {
            clients.close();
            sockets.close();
            SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue(oldAddress);
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(oldSchemaDir);
            SqlRecConfigs.FLINK_SQL_GATEWAY_CONNECT_TIMEOUT.setDefaultValue(oldTimeout);
        }
    }
}
