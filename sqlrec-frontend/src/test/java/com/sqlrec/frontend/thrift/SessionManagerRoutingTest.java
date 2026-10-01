package com.sqlrec.frontend.thrift;

import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class SessionManagerRoutingTest {
    @Test
    void metadataInitializationFailureDoesNotPublishAClient() {
        SessionManager manager = new SessionManager();
        try (var schema = mockStatic(CalciteSchemaFactory.class);
             var proxies = mockConstruction(ClientProxy.class)) {
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
            TExecuteStatementResp response = session.manager.ExecuteStatement(
                    new TExecuteStatementReq(session.handle, "DROP TABLE missing"));
            assertEquals(TStatusCode.SUCCESS_STATUS, response.getStatus().getStatusCode());
            verify(session.proxy, never()).ExecuteStatement(any());
            clearInvocations(session.proxy);
            assertEquals(TOperationState.ERROR_STATE, session.manager.GetOperationStatus(
                    new TGetOperationStatusReq(response.getOperationHandle())).getOperationState());
            verify(session.proxy).updateAccessTime(); // Local operation polling keeps the session alive.
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
            when(session.proxy.GetCrossReference(any())).thenReturn(cross);
            TFetchResultsResp fetched = new TFetchResultsResp(success());
            when(session.proxy.FetchResults(any())).thenReturn(fetched);
            when(session.proxy.CloseOperation(any())).thenReturn(new TCloseOperationResp(success()));

            assertEquals(operation, session.manager.GetCrossReference(
                    new TGetCrossReferenceReq(session.handle)).getOperationHandle());
            TFetchResultsReq fetch = new TFetchResultsReq(operation, TFetchOrientation.FETCH_NEXT, 10);
            assertSame(fetched, session.manager.FetchResults(fetch));
            session.manager.CloseOperation(new TCloseOperationReq(operation));
            assertEquals(TStatusCode.INVALID_HANDLE_STATUS, session.manager.CloseOperation(
                    new TCloseOperationReq(operation)).getStatus().getStatusCode());
            verify(session.proxy).FetchResults(fetch);
            verify(session.proxy).CloseOperation(any());
        }
    }

    @Test
    void resetFailureDoesNotClearLocalSettings() throws Exception {
        try (Session session = new Session()) {
            doThrow(new TException("Gateway rejected RESET")).when(session.proxy).executeSessionCommand(anyString());
            TExecuteStatementResp response = session.manager.ExecuteStatement(
                    new TExecuteStatementReq(session.handle, "RESET 'invalid.setting'"));
            assertEquals(TStatusCode.ERROR_STATUS, response.getStatus().getStatusCode());
            verify(session.executor, never()).resetSessionSettings(any());
            verify(session.proxy, never()).setSessionState(any(), any());
        }
    }

    @Test
    void successfulResetClearsLocalSettingsOnlyAfterRemoteConfirmation() throws Exception {
        try (Session session = new Session()) {
            TExecuteStatementResp response = session.manager.ExecuteStatement(
                    new TExecuteStatementReq(session.handle, "RESET 'invalid.setting'"));
            assertEquals(TStatusCode.SUCCESS_STATUS, response.getStatus().getStatusCode());
            var order = inOrder(session.proxy, session.executor);
            order.verify(session.proxy).executeSessionCommand("RESET 'invalid.setting'");
            order.verify(session.executor).resetSessionSettings("invalid.setting");
            order.verify(session.proxy).setSessionState("default", Map.of());
            verify(session.proxy, never()).ExecuteStatement(any());
        }
    }

    private static TStatus success() {
        return new TStatus(TStatusCode.SUCCESS_STATUS);
    }

    private static final class Session implements AutoCloseable {
        private final TSessionHandle handle = new TSessionHandle(ThriftUtils.getHandleIdentifier());
        private final MockedConstruction<SqlExecutor> executors = mockConstruction(SqlExecutor.class, (mock, context) -> {
            when(mock.getDefaultSchema()).thenReturn("default");
            when(mock.getSessionSettings()).thenReturn(Map.of());
        });
        private final MockedConstruction<ClientProxy> proxies = mockConstruction(ClientProxy.class, (mock, context) -> {
            TOpenSessionResp opened = new TOpenSessionResp(success(), TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);
            opened.setSessionHandle(handle);
            when(mock.OpenSession(any())).thenReturn(opened);
            when(mock.getSessionId()).thenReturn(handle.getSessionId());
            when(mock.CloseSession(any())).thenReturn(new TCloseSessionResp(success()));
        });
        private final SessionManager manager = new SessionManager();
        private final SqlExecutor executor;
        private final ClientProxy proxy;

        private Session() throws TException {
            manager.openSession(new TOpenSessionReq());
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
