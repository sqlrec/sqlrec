package com.sqlrec.frontend.thrift;

import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class SessionManagerRoutingTest {
    @Test
    void metadataInitializationFailureDoesNotPublishAClient() {
        SessionManager manager = new SessionManager();
        try (var schema = mockStatic(CalciteSchemaFactory.class);
             var proxies = mockConstruction(GatewayClient.class)) {
            schema.when(CalciteSchemaFactory::createCalciteSchema).thenThrow(new IllegalStateException("HMS unavailable"));
            assertThrows(IllegalStateException.class, () -> manager.openSession(new TOpenSessionReq()));
            assertTrue(proxies.constructed().isEmpty());
        }
    }

    @Test
    void localDdlFailureReturnsAnErrorOperationWithoutForwarding() throws Exception {
        try (Session session = new Session()) {
            when(session.executor.executeSqlAsync("DROP TABLE missing"))
                    .thenThrow(new IllegalArgumentException("table does not exist"));
            TExecuteStatementResp response = session.manager.executeStatement(
                    new TExecuteStatementReq(session.handle, "DROP TABLE missing"));
            assertEquals(TStatusCode.SUCCESS_STATUS, response.getStatus().getStatusCode());
            verify(session.proxy, never()).executeStatement(any());
            clearInvocations(session.proxy);
            assertEquals(TOperationState.ERROR_STATE, session.manager.getOperationStatus(
                    new TGetOperationStatusReq(response.getOperationHandle())).getOperationState());
            verifyNoInteractions(session.proxy); // Local polling never enters the Gateway client.
            verify(session.executor).executeSqlAsync("DROP TABLE missing");
        }
    }

    @Test
    void crossReferenceIsRegisteredForFetchAndRemovedOnSuccessfulClose() throws Exception {
        try (Session session = new Session()) {
            TOperationHandle operation = new TOperationHandle(
                    ThriftUtils.getHandleIdentifier(), TOperationType.UNKNOWN, true);
            TGetCrossReferenceResp cross = new TGetCrossReferenceResp(success());
            cross.setOperationHandle(operation);
            when(session.proxy.getCrossReference(any())).thenReturn(cross);
            TFetchResultsResp fetched = new TFetchResultsResp(success());
            when(session.proxy.fetchResults(any())).thenReturn(fetched);
            when(session.proxy.closeOperation(any())).thenReturn(new TCloseOperationResp(success()));

            assertEquals(operation, session.manager.getCrossReference(
                    new TGetCrossReferenceReq(session.handle)).getOperationHandle());
            TFetchResultsReq fetch = new TFetchResultsReq(operation, TFetchOrientation.FETCH_NEXT, 10);
            assertSame(fetched, session.manager.fetchResults(fetch));
            session.manager.closeOperation(new TCloseOperationReq(operation));
            assertEquals(TStatusCode.INVALID_HANDLE_STATUS, session.manager.closeOperation(
                    new TCloseOperationReq(operation)).getStatus().getStatusCode());
            verify(session.proxy).fetchResults(fetch);
            verify(session.proxy).closeOperation(any());
        }
    }

    @Test
    void resetFailureDoesNotClearLocalSettings() throws Exception {
        try (Session session = new Session()) {
            doThrow(new TException("Gateway rejected RESET")).when(session.proxy).executeSessionCommand(anyString());
            TExecuteStatementResp response = session.manager.executeStatement(
                    new TExecuteStatementReq(session.handle, "RESET 'invalid.setting'"));
            assertEquals(TStatusCode.ERROR_STATUS, response.getStatus().getStatusCode());
            verify(session.executor, never()).resetSessionSettings(any());
            verify(session.proxy, never()).setSessionState(any(), any());
        }
    }

    @Test
    void successfulResetClearsLocalSettingsOnlyAfterRemoteConfirmation() throws Exception {
        try (Session session = new Session()) {
            TExecuteStatementResp response = session.manager.executeStatement(
                    new TExecuteStatementReq(session.handle, "RESET 'invalid.setting'"));
            assertEquals(TStatusCode.SUCCESS_STATUS, response.getStatus().getStatusCode());
            var order = inOrder(session.proxy, session.executor);
            order.verify(session.proxy).executeSessionCommand("RESET 'invalid.setting'");
            order.verify(session.executor).resetSessionSettings("invalid.setting");
            order.verify(session.proxy).setSessionState("default", Map.of());
            verify(session.proxy, never()).executeStatement(any());
        }
    }

    @Test
    void unknownOperationHandlesAreRejectedWithoutEnteringGateway() throws Exception {
        try (Session session = new Session()) {
            TOperationHandle unknown = new TOperationHandle(
                    ThriftUtils.getHandleIdentifier(), TOperationType.EXECUTE_STATEMENT, true);
            assertEquals(TStatusCode.INVALID_HANDLE_STATUS,
                    session.manager.cancelOperation(new TCancelOperationReq(unknown)).getStatus().getStatusCode());
            assertEquals(TStatusCode.INVALID_HANDLE_STATUS,
                    session.manager.closeOperation(new TCloseOperationReq(unknown)).getStatus().getStatusCode());
            assertThrows(TException.class, () -> session.manager.getQueryId(new TGetQueryIdReq(unknown)));
            assertThrows(TException.class, () -> session.manager.getOperationStatus(new TGetOperationStatusReq(unknown)));
            assertThrows(TException.class, () -> session.manager.getResultSetMetadata(new TGetResultSetMetadataReq(unknown)));
            assertThrows(TException.class, () -> session.manager.fetchResults(
                    new TFetchResultsReq(unknown, TFetchOrientation.FETCH_NEXT, 10)));
            verifyNoInteractions(session.proxy);
        }
    }

    @Test
    void rejectedRemoteSqlDoesNotRegisterItsReturnedHandle() throws Exception {
        try (Session session = new Session()) {
            TOperationHandle handle = new TOperationHandle(
                    ThriftUtils.getHandleIdentifier(), TOperationType.EXECUTE_STATEMENT, true);
            TExecuteStatementResp rejected = new TExecuteStatementResp(new TStatus(TStatusCode.ERROR_STATUS));
            rejected.setOperationHandle(handle);
            when(session.proxy.executeStatement(any())).thenReturn(rejected);
            assertSame(rejected, session.manager.executeStatement(new TExecuteStatementReq(session.handle, "SELECT * FROM remote_table")));
            assertEquals(TStatusCode.INVALID_HANDLE_STATUS,
                    session.manager.closeOperation(new TCloseOperationReq(handle)).getStatus().getStatusCode());
            verify(session.proxy, never()).closeOperation(any());
        }
    }

    @Test
    void failedRemoteCloseRetainsTheOperationForAnotherClose() throws Exception {
        try (Session session = new Session()) {
            TOperationHandle handle = new TOperationHandle(
                    ThriftUtils.getHandleIdentifier(), TOperationType.EXECUTE_STATEMENT, true);
            TExecuteStatementResp executed = new TExecuteStatementResp(success());
            executed.setOperationHandle(handle);
            when(session.proxy.executeStatement(any())).thenReturn(executed);
            when(session.proxy.closeOperation(any()))
                    .thenReturn(new TCloseOperationResp(new TStatus(TStatusCode.ERROR_STATUS)))
                    .thenReturn(new TCloseOperationResp(success()));
            session.manager.executeStatement(new TExecuteStatementReq(session.handle, "SELECT * FROM remote_table"));
            assertEquals(TStatusCode.ERROR_STATUS,
                    session.manager.closeOperation(new TCloseOperationReq(handle)).getStatus().getStatusCode());
            assertEquals(TStatusCode.SUCCESS_STATUS,
                    session.manager.closeOperation(new TCloseOperationReq(handle)).getStatus().getStatusCode());
            assertEquals(TStatusCode.INVALID_HANDLE_STATUS,
                    session.manager.closeOperation(new TCloseOperationReq(handle)).getStatus().getStatusCode());
        }
    }

    @Test
    void sessionCloseWaitsForExecutionAndRemovesTheRegisteredOperation() throws Exception {
        try (Session session = new Session()) {
            CountDownLatch executing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch closing = new CountDownLatch(1);
            when(session.executor.executeSqlAsync("SELECT 1")).thenAnswer(call -> {
                executing.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                return SqlProcessResult.msg("done", "msg");
            });
            var workers = Executors.newFixedThreadPool(2);
            try {
                var execution = workers.submit(() -> session.manager.executeStatement(new TExecuteStatementReq(session.handle, "SELECT 1")));
                assertTrue(executing.await(5, TimeUnit.SECONDS));
                var close = workers.submit(() -> {
                    closing.countDown();
                    return session.manager.closeSession(new TCloseSessionReq(session.handle));
                });
                assertTrue(closing.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                TOperationHandle handle = execution.get(5, TimeUnit.SECONDS).getOperationHandle();
                assertEquals(TStatusCode.SUCCESS_STATUS, close.get(5, TimeUnit.SECONDS).getStatus().getStatusCode());
                assertFalse(session.manager.hasSession(session.handle));
                assertEquals(TStatusCode.INVALID_HANDLE_STATUS,
                        session.manager.closeOperation(new TCloseOperationReq(handle)).getStatus().getStatusCode());
                assertThrows(TException.class, () -> session.manager.executeStatement(new TExecuteStatementReq(session.handle, "SELECT 2")));
                verify(session.proxy).closeSession();
            } finally {
                release.countDown();
                workers.shutdownNow();
            }
        }
    }

    private static TStatus success() {
        return new TStatus(TStatusCode.SUCCESS_STATUS);
    }

    private static final class Session implements AutoCloseable {
        private final TSessionHandle handle;
        private final MockedConstruction<SqlExecutor> executors = mockConstruction(SqlExecutor.class, (mock, context) -> {
            when(mock.getDefaultSchema()).thenReturn("default");
            when(mock.getSessionSettings()).thenReturn(Map.of());
        });
        private final MockedConstruction<GatewayClient> proxies = mockConstruction(GatewayClient.class, (mock, context) -> {
            when(mock.closeSession()).thenReturn(new TCloseSessionResp(success()));
        });
        private final SessionManager manager = new SessionManager();
        private final SqlExecutor executor;
        private final GatewayClient proxy;

        private Session() throws TException {
            handle = manager.openSession(new TOpenSessionReq()).getSessionHandle();
            executor = executors.constructed().get(0);
            proxy = proxies.constructed().get(0);
        }

        @Override
        public void close() throws TException {
            try {
                manager.closeSession(new TCloseSessionReq(handle));
            } finally {
                proxies.close();
                executors.close();
            }
        }
    }
}
