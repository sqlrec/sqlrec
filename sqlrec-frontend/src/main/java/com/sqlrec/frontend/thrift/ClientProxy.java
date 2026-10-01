package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A proxy owns one generated Thrift client and transport for a session.  Generated
 * clients and their protocols are not safe for concurrent request/response exchanges,
 * so remote TCLIService calls are synchronized for the whole reconnect/translate/
 * invoke/restore sequence.
 */
public class ClientProxy implements TCLIService.Iface {
    private static final Logger logger = LoggerFactory.getLogger(ClientProxy.class);

    private THandleIdentifier localSessionId;
    private THandleIdentifier remoteSessionId;
    private TCLIService.Client client;
    private TTransport transport;
    private final AtomicLong lastAccessTime;

    private volatile boolean connected;
    private TOpenSessionReq pendingOpenSessionReq;
    private String desiredDatabase = Consts.DEFAULT_SCHEMA_NAME;
    private Map<String, String> desiredSettings = new LinkedHashMap<>();
    private long stateVersion;
    private long appliedStateVersion = -1;
    private final Set<THandleIdentifier> remoteOperations = new HashSet<>();

    public synchronized void setSessionState(String database, Map<String, String> settings) {
        if (!database.equals(desiredDatabase) || !settings.equals(desiredSettings)) {
            desiredDatabase = database;
            desiredSettings = new LinkedHashMap<>(settings);
            stateVersion++;
        }
    }

    private void requireGatewayEnabled() throws TException {
        if (!SqlRecConfigs.isFlinkSqlGatewayEnabled()) {
            throw new TException("FLINK_GATEWAY_DISABLED: this statement requires Flink execution; "
                    + "configure FLINK_SQL_GATEWAY_ADDRESS to enable forwarding");
        }
        if (SqlRecConfigs.isFileSystemMetadata()) {
            throw new TException("Remote connection is not allowed in file system meta mode");
        }
    }

    public ClientProxy() {
        this.lastAccessTime = new AtomicLong(System.currentTimeMillis());
        this.connected = false;
    }

    @Override
    public synchronized TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
        this.localSessionId = ThriftUtils.getHandleIdentifier();
        this.pendingOpenSessionReq = req.deepCopy();

        TOpenSessionResp resp = new TOpenSessionResp();
        resp.setStatus(new TStatus(TStatusCode.SUCCESS_STATUS));
        resp.setSessionHandle(new TSessionHandle(localSessionId));
        resp.setServerProtocolVersion(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);
        resp.setConfiguration(new HashMap<>());

        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_SESSION_OPEN_COUNT)
                .increment();

        logger.info("Local session opened, sessionGuid: {}", ThriftUtils.safeHandleId(localSessionId));
        return resp;
    }

    private void ensureConnected() throws TException {
        requireGatewayEnabled();
        if (connected && transport != null && transport.isOpen()) {
            return;
        }

        markDisconnected();

        if (pendingOpenSessionReq == null) {
            throw new TException("Cannot (re)connect: OpenSession was never called");
        }

        TTransport transport = new TSocket(
                SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getValue().trim(),
                SqlRecConfigs.FLINK_SQL_GATEWAY_PORT.getValue(),
                SqlRecConfigs.FLINK_SQL_GATEWAY_CONNECT_TIMEOUT.getValue()
        );
        try {
            TProtocol protocol = new TBinaryProtocol(transport);
            TCLIService.Client client = new TCLIService.Client(protocol);
            transport.open();

            TOpenSessionResp remoteResp = client.OpenSession(pendingOpenSessionReq);
            requireSuccess(remoteResp.getStatus());
            if (remoteResp.getSessionHandle() == null) {
                throw new TException("Gateway OpenSession returned no session handle");
            }
            THandleIdentifier remoteSessionId = copyHandleId(remoteResp.getSessionHandle().getSessionId());

            // All remote operations succeeded, commit state atomically.
            // NOTE: pendingOpenSessionReq is intentionally retained so the session can be
            // re-opened after a future transport failure (see markDisconnected).
            this.client = client;
            this.transport = transport;
            this.remoteSessionId = remoteSessionId;
            this.connected = true;
            this.appliedStateVersion = -1;

            logger.info("Remote connection opened, localSessionGuid: {}, remoteSessionGuid: {}",
                    ThriftUtils.safeHandleId(localSessionId), ThriftUtils.safeHandleId(remoteSessionId));
        } catch (Exception e) {
            logger.error("Failed to open remote connection: {}", e.getMessage(), e);
            try {
                transport.close();
            } catch (Exception closeEx) {
                logger.warn("Failed to close transport during error handling", closeEx);
            }
            this.client = null;
            this.transport = null;
            throw new TException("FLINK_GATEWAY_UNAVAILABLE: could not open a Gateway session: " + e.getMessage(), e);
        }
    }

    /**
     * Tear down the current remote connection without dropping session identity, so the
     * next call to {@link #ensureConnected()} re-opens the remote session. Used after a
     * transport-level failure so the broken client/transport is not reused forever.
     */
    private void markDisconnected() {
        if (transport != null) {
            try {
                transport.close();
            } catch (Exception e) {
                logger.warn("Failed to close transport after remote error", e);
            }
        }
        this.client = null;
        this.transport = null;
        this.remoteSessionId = null;
        this.connected = false;
        this.appliedStateVersion = -1;
        this.remoteOperations.clear();
        // keep localSessionId and pendingOpenSessionReq for reconnect
    }

    @FunctionalInterface
    private interface RemoteCall<T> {
        T apply() throws TException;
    }

    /**
     * Invoke a remote thrift call, and on a transport-level failure mark the connection
     * disconnected so the next call re-opens it. The current call still fails (the
     * connection is genuinely broken), but the proxy recovers instead of staying broken.
     */
    private <T> T invokeRemote(RemoteCall<T> call) throws TException {
        try {
            return call.apply();
        } catch (TTransportException e) {
            markDisconnected();
            throw e;
        }
    }

    private <T> T invokeWithSessionHandle(TSessionHandle sessionHandle, RemoteCall<T> call)
            throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(sessionHandle);
        try {
            return invokeRemote(call);
        } finally {
            restoreSessionHandle(sessionHandle, originalSessionId);
        }
    }

    private <T> T invokeConnected(RemoteCall<T> call) throws TException {
        updateAccessTime();
        requireGatewayEnabled();
        if (!connected || transport == null || !transport.isOpen()) {
            throw new TException("FLINK_REMOTE_SESSION_LOST: remote operation handles cannot be resumed after reconnect");
        }
        return invokeRemote(call);
    }

    private static void requireSuccess(TStatus status) throws TException {
        if (!ThriftUtils.isSuccess(status)) {
            throw new TException(status == null ? "Gateway returned no status" : status.getErrorMessage());
        }
    }

    private void prepareStatement() throws TException {
        requireGatewayEnabled();
        ensureConnected();
        if (appliedStateVersion == stateVersion) {
            return;
        }
        String synchronizing = "table.sql-dialect";
        try {
            String dialect = desiredSettings.get("table.sql-dialect");
            if (dialect != null) {
                applyStateStatement("SET 'table.sql-dialect' = " + literal(dialect));
            }
            for (Map.Entry<String, String> setting : desiredSettings.entrySet()) {
                if (!setting.getKey().equals("table.sql-dialect")) {
                    synchronizing = setting.getKey();
                    applyStateStatement("SET " + literal(setting.getKey()) + " = " + literal(setting.getValue()));
                }
            }
            synchronizing = "current database";
            applyStateStatement("USE `" + desiredDatabase.replace("`", "``") + "`");
            appliedStateVersion = stateVersion;
        } catch (TException e) {
            throw new TException("FLINK_SESSION_STATE_SYNC_FAILED: could not apply " + synchronizing
                    + "; correct this setting and retry the remote statement", e);
        }
    }

    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private void applyStateStatement(String sql) throws TException {
        executeStateStatement(sql);
        if (!connected) {
            // Cleanup may lose the transport after the command succeeded. A new session must replay all state.
            throw new TException("Gateway connection lost while synchronizing session state");
        }
    }

    /** Confirm a remote configuration command before changing SQLRec's saved session overrides. */
    public synchronized void executeSessionCommand(String sql) throws TException {
        updateAccessTime();
        // RESET must work even if a pending SET is invalid; do not replay pending overrides first.
        ensureConnected();
        executeStateStatement(sql);
    }

    private void executeStateStatement(String sql) throws TException {
        TExecuteStatementReq request = new TExecuteStatementReq(new TSessionHandle(remoteSessionId), sql);
        request.setRunAsync(false);
        TExecuteStatementResp response = invokeRemote(() -> client.ExecuteStatement(request));
        requireSuccess(response.getStatus());
        TOperationHandle handle = response.getOperationHandle();
        if (handle == null) {
            throw new TException("Gateway state statement returned no operation handle");
        }
        try {
            // runAsync=false asks the endpoint to finish the statement before responding.
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    SqlRecConfigs.FLINK_SQL_GATEWAY_CONNECT_TIMEOUT.getValue());
            while (true) {
                TGetOperationStatusResp status = invokeRemote(() -> client.GetOperationStatus(new TGetOperationStatusReq(handle)));
                requireSuccess(status.getStatus());
                if (status.getOperationState() == TOperationState.FINISHED_STATE) {
                    break;
                }
                if (status.getOperationState() != TOperationState.RUNNING_STATE
                        && status.getOperationState() != TOperationState.PENDING_STATE
                        && status.getOperationState() != TOperationState.INITIALIZED_STATE) {
                    throw new TException("Gateway state statement failed: " + status.getErrorMessage());
                }
                if (System.nanoTime() >= deadline) {
                    throw new TException("Gateway session state synchronization timed out");
                }
                try {
                    Thread.sleep(25);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TException("Gateway state synchronization interrupted", e);
                }
            }
        } finally {
            if (connected) {
                try {
                    requireSuccess(invokeRemote(() -> client.CloseOperation(new TCloseOperationReq(handle))).getStatus());
                } catch (TException cleanupFailure) {
                    // Cleanup must not hide a completed command or replace its original error.
                    logger.warn("Failed to close Gateway session-state operation", cleanupFailure);
                }
            }
        }
    }

    private void requireRemoteOperation(TOperationHandle handle) throws TException {
        if (handle == null || !remoteOperations.contains(handle.getOperationId())) {
            throw new TException("FLINK_REMOTE_SESSION_LOST: remote operation is unknown or belongs to an earlier session");
        }
    }

    private void trackRemoteOperation(TStatus status, TOperationHandle handle) {
        if (ThriftUtils.isSuccess(status) && handle != null) {
            remoteOperations.add(copyHandleId(handle.getOperationId()));
        }
    }

    private THandleIdentifier translateSessionHandle(TSessionHandle sessionHandle) {
        if (sessionHandle != null && remoteSessionId != null) {
            THandleIdentifier originalSessionId = copyHandleId(sessionHandle.getSessionId());
            sessionHandle.setSessionId(copyHandleId(remoteSessionId));
            return originalSessionId;
        }
        return null;
    }

    private void restoreSessionHandle(TSessionHandle sessionHandle, THandleIdentifier originalSessionId) {
        if (sessionHandle != null && originalSessionId != null) {
            sessionHandle.setSessionId(originalSessionId);
        }
    }

    private static THandleIdentifier copyHandleId(THandleIdentifier source) {
        byte[] guidBytes = Arrays.copyOf(source.getGuid(), source.getGuid().length);
        byte[] secretBytes = Arrays.copyOf(source.getSecret(), source.getSecret().length);
        return new THandleIdentifier(ByteBuffer.wrap(guidBytes), ByteBuffer.wrap(secretBytes));
    }

    public THandleIdentifier getSessionId() {
        return localSessionId;
    }

    public long getLastAccessTime() {
        return lastAccessTime.get();
    }

    public void updateAccessTime() {
        lastAccessTime.set(System.currentTimeMillis());
    }

    @Override
    public synchronized TCloseSessionResp CloseSession(TCloseSessionReq req) throws TException {
        try {
            if (connected) {
                THandleIdentifier originalSessionId = translateSessionHandle(req.getSessionHandle());
                try {
                    return invokeRemote(() -> client.CloseSession(req));
                } finally {
                    restoreSessionHandle(req.getSessionHandle(), originalSessionId);
                }
            } else {
                return new TCloseSessionResp(new TStatus(TStatusCode.SUCCESS_STATUS));
            }
        } finally {
            markDisconnected();
            pendingOpenSessionReq = null;
            MetricsUtils.getCompositeMeterRegistry()
                    .counter(Consts.METRICS_SESSION_CLOSE_COUNT)
                    .increment();
        }
    }

    @Override
    public TGetInfoResp GetInfo(TGetInfoReq tGetInfoReq) throws TException {
        updateAccessTime();

        TGetInfoResp resp = new TGetInfoResp();
        resp.setStatus(new TStatus(TStatusCode.SUCCESS_STATUS));

        TGetInfoType infoType = tGetInfoReq.getInfoType();
        String infoValue = switch (infoType) {
            case CLI_DBMS_NAME -> "Apache Hive";
            case CLI_DBMS_VER -> "3.1.3";
            case CLI_SERVER_NAME, CLI_DATA_SOURCE_NAME -> "SQLRec";
            case CLI_CATALOG_NAME -> Consts.HIVE_CATALOG_NAME;
            case CLI_DATA_SOURCE_READ_ONLY -> SqlRecConfigs.isFileSystemMetadata() ? "Y" : "N";
            default -> "";
        };
        resp.setInfoValue(TGetInfoValue.stringValue(infoValue));

        return resp;
    }

    @Override
    public synchronized TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        prepareStatement();
        TExecuteStatementResp response = invokeWithSessionHandle(
                tExecuteStatementReq.getSessionHandle(),
                () -> client.ExecuteStatement(tExecuteStatementReq)
        );
        trackRemoteOperation(response.getStatus(), response.getOperationHandle());
        return response;
    }

    @Override
    public synchronized TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq tGetTypeInfoReq) throws TException {
        return invokeWithSessionHandle(
                tGetTypeInfoReq.getSessionHandle(),
                () -> client.GetTypeInfo(tGetTypeInfoReq)
        );
    }

    @Override
    public synchronized TGetCatalogsResp GetCatalogs(TGetCatalogsReq tGetCatalogsReq) throws TException {
        return invokeWithSessionHandle(
                tGetCatalogsReq.getSessionHandle(),
                () -> client.GetCatalogs(tGetCatalogsReq)
        );
    }

    @Override
    public synchronized TGetSchemasResp GetSchemas(TGetSchemasReq tGetSchemasReq) throws TException {
        return invokeWithSessionHandle(
                tGetSchemasReq.getSessionHandle(),
                () -> client.GetSchemas(tGetSchemasReq)
        );
    }

    @Override
    public synchronized TGetTablesResp GetTables(TGetTablesReq tGetTablesReq) throws TException {
        return invokeWithSessionHandle(
                tGetTablesReq.getSessionHandle(),
                () -> client.GetTables(tGetTablesReq)
        );
    }

    @Override
    public synchronized TGetTableTypesResp GetTableTypes(TGetTableTypesReq tGetTableTypesReq) throws TException {
        return invokeWithSessionHandle(
                tGetTableTypesReq.getSessionHandle(),
                () -> client.GetTableTypes(tGetTableTypesReq)
        );
    }

    @Override
    public synchronized TGetColumnsResp GetColumns(TGetColumnsReq tGetColumnsReq) throws TException {
        return invokeWithSessionHandle(
                tGetColumnsReq.getSessionHandle(),
                () -> client.GetColumns(tGetColumnsReq)
        );
    }

    @Override
    public synchronized TGetFunctionsResp GetFunctions(TGetFunctionsReq tGetFunctionsReq) throws TException {
        return invokeWithSessionHandle(
                tGetFunctionsReq.getSessionHandle(),
                () -> client.GetFunctions(tGetFunctionsReq)
        );
    }

    @Override
    public synchronized TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq tGetPrimaryKeysReq) throws TException {
        return invokeWithSessionHandle(
                tGetPrimaryKeysReq.getSessionHandle(),
                () -> client.GetPrimaryKeys(tGetPrimaryKeysReq)
        );
    }

    @Override
    public synchronized TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq tGetCrossReferenceReq) throws TException {
        TGetCrossReferenceResp response = invokeWithSessionHandle(
                tGetCrossReferenceReq.getSessionHandle(),
                () -> client.GetCrossReference(tGetCrossReferenceReq)
        );
        trackRemoteOperation(response.getStatus(), response.getOperationHandle());
        return response;
    }

    @Override
    public synchronized TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        requireRemoteOperation(tGetOperationStatusReq.getOperationHandle());
        return invokeConnected(() -> client.GetOperationStatus(tGetOperationStatusReq));
    }

    @Override
    public synchronized TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        requireRemoteOperation(tCancelOperationReq.getOperationHandle());
        return invokeConnected(() -> client.CancelOperation(tCancelOperationReq));
    }

    @Override
    public synchronized TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        requireRemoteOperation(tCloseOperationReq.getOperationHandle());
        TCloseOperationResp response = invokeConnected(() -> client.CloseOperation(tCloseOperationReq));
        if (ThriftUtils.isSuccess(response.getStatus())) {
            remoteOperations.remove(tCloseOperationReq.getOperationHandle().getOperationId());
        }
        return response;
    }

    @Override
    public synchronized TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        requireRemoteOperation(tGetResultSetMetadataReq.getOperationHandle());
        return invokeConnected(() -> client.GetResultSetMetadata(tGetResultSetMetadataReq));
    }

    @Override
    public synchronized TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        requireRemoteOperation(tFetchResultsReq.getOperationHandle());
        return invokeConnected(() -> client.FetchResults(tFetchResultsReq));
    }

    @Override
    public synchronized TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq tGetDelegationTokenReq) throws TException {
        return invokeWithSessionHandle(
                tGetDelegationTokenReq.getSessionHandle(),
                () -> client.GetDelegationToken(tGetDelegationTokenReq)
        );
    }

    @Override
    public synchronized TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq tCancelDelegationTokenReq) throws TException {
        return invokeWithSessionHandle(
                tCancelDelegationTokenReq.getSessionHandle(),
                () -> client.CancelDelegationToken(tCancelDelegationTokenReq)
        );
    }

    @Override
    public synchronized TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq tRenewDelegationTokenReq) throws TException {
        return invokeWithSessionHandle(
                tRenewDelegationTokenReq.getSessionHandle(),
                () -> client.RenewDelegationToken(tRenewDelegationTokenReq)
        );
    }

    @Override
    public synchronized TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        requireRemoteOperation(tGetQueryIdReq.getOperationHandle());
        return invokeConnected(() -> client.GetQueryId(tGetQueryIdReq));
    }

    @Override
    public synchronized TSetClientInfoResp SetClientInfo(TSetClientInfoReq tSetClientInfoReq) throws TException {
        return invokeWithSessionHandle(
                tSetClientInfoReq.getSessionHandle(),
                () -> client.SetClientInfo(tSetClientInfoReq)
        );
    }
}
