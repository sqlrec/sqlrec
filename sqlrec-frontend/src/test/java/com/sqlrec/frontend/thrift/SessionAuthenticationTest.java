package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SessionAuthenticationTest {
    private final Boolean previousEnabled = SqlRecConfigs.AUTH_ENABLED.getDefaultValue();

    @BeforeEach
    void enableAuthentication() {
        SqlRecConfigs.AUTH_ENABLED.setDefaultValue(true);
    }

    @AfterEach
    void restoreConfiguration() {
        SqlRecConfigs.AUTH_ENABLED.setDefaultValue(previousEnabled);
    }

    @Test
    void rejectsUnsupportedProtocolsBeforeCreatingExecutorsOrGateways() throws Exception {
        SessionManager manager = new SessionManager();
        try (var executors = mockConstruction(SqlExecutor.class);
             var gateways = mockConstruction(GatewayClient.class)) {
            for (TProtocolVersion protocol : TProtocolVersion.values()) {
                if (protocol.getValue() >= TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue()) {
                    continue;
                }
                TOpenSessionReq request = new TOpenSessionReq(protocol);
                TOpenSessionResp response = manager.openSession(request);
                assertEquals(TStatusCode.ERROR_STATUS, response.getStatus().getStatusCode());
                assertEquals("0A000", response.getStatus().getSqlState());
                assertNull(response.getSessionHandle());
            }
            TOpenSessionReq missing = new TOpenSessionReq();
            missing.setClient_protocol(null);
            assertEquals(TStatusCode.ERROR_STATUS, manager.openSession(missing).getStatus().getStatusCode());
            assertTrue(executors.constructed().isEmpty());
            assertTrue(gateways.constructed().isEmpty());
        }
    }

    @Test
    void negotiatesTheClientProtocolIncludingPyHiveV6() throws Exception {
        SessionManager manager = new SessionManager();
        try (var executors = mockConstruction(SqlExecutor.class);
             var gateways = mockConstruction(GatewayClient.class)) {
            for (TProtocolVersion protocol : List.of(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6,
                    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8,
                    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
                    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11)) {
                // SASL has already authenticated the connection; OpenSession needs no password.
                TOpenSessionReq request = new TOpenSessionReq(protocol);
                request.setUsername("sqlrec");
                TOpenSessionResp response = manager.openSession(request);
                assertEquals(TStatusCode.SUCCESS_STATUS, response.getStatus().getStatusCode());
                assertEquals(Math.min(protocol.getValue(), TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10.getValue()),
                        response.getServerProtocolVersion().getValue());
                assertNotNull(response.getSessionHandle());
            }
        }
    }

    @Test
    void sessionsWorkWithoutForwardingLocalCredentials() throws Exception {
        SessionManager manager = new SessionManager();
        TOpenSessionReq request = login("sqlrec", "secret");
        request.setConfiguration(Map.of("use:database", "default"));
        try (var executors = mockConstruction(SqlExecutor.class);
             var gateways = mockConstruction(GatewayClient.class, (mock, context) -> {
                 TOpenSessionReq forwarded = (TOpenSessionReq) context.arguments().get(0);
                 assertFalse(forwarded.isSetUsername());
                 assertFalse(forwarded.isSetPassword());
                 assertEquals(request.getConfiguration(), forwarded.getConfiguration());
                 when(mock.closeSession()).thenReturn(new TCloseSessionResp(new TStatus(TStatusCode.SUCCESS_STATUS)));
             })) {
            TOpenSessionResp opened = manager.openSession(request);
            assertEquals(TStatusCode.SUCCESS_STATUS, opened.getStatus().getStatusCode());
            TSessionHandle handle = opened.getSessionHandle();
            assertTrue(manager.hasSession(handle));
            assertEquals("secret", request.getPassword()); // The caller's request is not modified.
            assertEquals(1, executors.constructed().size());
            assertEquals(1, gateways.constructed().size());

            TOperationHandle operation = manager.openMetadataOperation(handle, TOperationType.GET_CATALOGS,
                    () -> SqlProcessResult.msg("hive", "catalog"));
            assertEquals(TOperationState.FINISHED_STATE,
                    manager.getOperationStatus(new TGetOperationStatusReq(operation)).getOperationState());

            TSessionHandle forged = handle.deepCopy();
            forged.getSessionId().setSecret(ThriftUtils.getHandleIdentifier().getSecret());
            assertFalse(manager.hasSession(forged));
            assertThrows(TException.class, () -> manager.openMetadataOperation(forged, TOperationType.GET_CATALOGS,
                    () -> SqlProcessResult.msg("hive", "catalog")));

            manager.closeSession(new TCloseSessionReq(handle));
            assertFalse(manager.hasSession(handle));
            assertThrows(TException.class,
                    () -> manager.getOperationStatus(new TGetOperationStatusReq(operation)));
        }
    }

    @Test
    void disabledAuthenticationPreservesExistingGatewayCredentials() throws Exception {
        SqlRecConfigs.AUTH_ENABLED.setDefaultValue(false);
        SessionManager manager = new SessionManager();
        TOpenSessionReq request = login("existing-user", "existing-password");
        try (var executors = mockConstruction(SqlExecutor.class);
             var gateways = mockConstruction(GatewayClient.class, (mock, context) ->
                     assertEquals(request, context.arguments().get(0)))) {
            assertEquals(TStatusCode.SUCCESS_STATUS, manager.openSession(request).getStatus().getStatusCode());
            assertEquals(1, gateways.constructed().size());
        }
    }

    private static TOpenSessionReq login(String username, String password) {
        TOpenSessionReq request = new TOpenSessionReq();
        request.setUsername(username);
        request.setPassword(password);
        return request;
    }
}
