package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Owns one Gateway connection. Generated Thrift clients must be used serially. */
public final class GatewayClient {
    private static final Logger logger = LoggerFactory.getLogger(GatewayClient.class);

    private THandleIdentifier remoteSessionId;
    private TCLIService.Client client;
    private TTransport transport;

    private boolean connected;
    private TOpenSessionReq pendingOpenSessionReq;
    private String desiredDatabase = Consts.DEFAULT_SCHEMA_NAME;
    private Map<String, String> desiredSettings = new LinkedHashMap<>();
    private boolean stateDirty = true;
    private final Set<THandleIdentifier> remoteOperations = new HashSet<>();

    public synchronized void setSessionState(String database, Map<String, String> settings) {
        if (!database.equals(desiredDatabase) || !settings.equals(desiredSettings)) {
            desiredDatabase = database;
            desiredSettings = new LinkedHashMap<>(settings);
            stateDirty = true;
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

    public GatewayClient(TOpenSessionReq request) {
        this.pendingOpenSessionReq = request.deepCopy();
    }

    private void ensureConnected() throws TException {
        requireGatewayEnabled();
        if (connected && transport != null && transport.isOpen()) {
            return;
        }

        markDisconnected();

        if (pendingOpenSessionReq == null) {
            throw new TException("Gateway client was closed");
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
            THandleIdentifier remoteSessionId = remoteResp.getSessionHandle().getSessionId().deepCopy();

            // Publish the connection only after OpenSession succeeds. Retain the request for reconnect.
            this.client = client;
            this.transport = transport;
            this.remoteSessionId = remoteSessionId;
            this.connected = true;
            this.stateDirty = true;

            logger.info("Gateway session opened, remoteSessionGuid: {}", ThriftUtils.safeHandleId(remoteSessionId));
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
     * Tear down the current remote connection while retaining the open request, so the
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
        this.stateDirty = true;
        this.remoteOperations.clear();
        // Keep the open request for reconnect.
    }

    @FunctionalInterface
    private interface RemoteCall<T> {
        T apply() throws TException;
    }

    /**
     * Invoke a remote thrift call, and on a transport-level failure mark the connection
     * disconnected so the next call re-opens it. The current call still fails (the
     * connection is genuinely broken), but the client recovers instead of staying broken.
     */
    private <T> T invokeRemote(RemoteCall<T> call) throws TException {
        try {
            return call.apply();
        } catch (TTransportException e) {
            markDisconnected();
            throw e;
        }
    }

    private <T> T invokeConnected(RemoteCall<T> call) throws TException {
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
        ensureConnected();
        if (!stateDirty) {
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
            stateDirty = false;
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
            remoteOperations.add(handle.getOperationId().deepCopy());
        }
    }

    public synchronized TCloseSessionResp closeSession() throws TException {
        try {
            if (!connected) {
                return new TCloseSessionResp(new TStatus(TStatusCode.SUCCESS_STATUS));
            }
            TCloseSessionReq request = new TCloseSessionReq(remoteSessionHandle());
            return invokeRemote(() -> client.CloseSession(request));
        } finally {
            markDisconnected();
            pendingOpenSessionReq = null;
        }
    }

    private TSessionHandle remoteSessionHandle() {
        return new TSessionHandle(remoteSessionId.deepCopy());
    }

    public synchronized TExecuteStatementResp executeStatement(TExecuteStatementReq request) throws TException {
        prepareStatement();
        TExecuteStatementReq remote = request.deepCopy();
        remote.setSessionHandle(remoteSessionHandle());
        TExecuteStatementResp response = invokeRemote(() -> client.ExecuteStatement(remote));
        trackRemoteOperation(response.getStatus(), response.getOperationHandle());
        return response;
    }

    public synchronized TGetCrossReferenceResp getCrossReference(TGetCrossReferenceReq request) throws TException {
        ensureConnected();
        TGetCrossReferenceReq remote = request.deepCopy();
        remote.setSessionHandle(remoteSessionHandle());
        TGetCrossReferenceResp response = invokeRemote(() -> client.GetCrossReference(remote));
        trackRemoteOperation(response.getStatus(), response.getOperationHandle());
        return response;
    }

    public synchronized TGetOperationStatusResp getOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        requireRemoteOperation(tGetOperationStatusReq.getOperationHandle());
        return invokeConnected(() -> client.GetOperationStatus(tGetOperationStatusReq));
    }

    public synchronized TCancelOperationResp cancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        requireRemoteOperation(tCancelOperationReq.getOperationHandle());
        return invokeConnected(() -> client.CancelOperation(tCancelOperationReq));
    }

    public synchronized TCloseOperationResp closeOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        requireRemoteOperation(tCloseOperationReq.getOperationHandle());
        TCloseOperationResp response = invokeConnected(() -> client.CloseOperation(tCloseOperationReq));
        if (ThriftUtils.isSuccess(response.getStatus())) {
            remoteOperations.remove(tCloseOperationReq.getOperationHandle().getOperationId());
        }
        return response;
    }

    public synchronized TGetResultSetMetadataResp getResultSetMetadata(TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        requireRemoteOperation(tGetResultSetMetadataReq.getOperationHandle());
        return invokeConnected(() -> client.GetResultSetMetadata(tGetResultSetMetadataReq));
    }

    public synchronized TFetchResultsResp fetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        requireRemoteOperation(tFetchResultsReq.getOperationHandle());
        return invokeConnected(() -> client.FetchResults(tFetchResultsReq));
    }

    public synchronized TGetDelegationTokenResp getDelegationToken(TGetDelegationTokenReq request) throws TException {
        ensureConnected();
        TGetDelegationTokenReq remote = request.deepCopy();
        remote.setSessionHandle(remoteSessionHandle());
        return invokeRemote(() -> client.GetDelegationToken(remote));
    }

    public synchronized TCancelDelegationTokenResp cancelDelegationToken(TCancelDelegationTokenReq request) throws TException {
        ensureConnected();
        TCancelDelegationTokenReq remote = request.deepCopy();
        remote.setSessionHandle(remoteSessionHandle());
        return invokeRemote(() -> client.CancelDelegationToken(remote));
    }

    public synchronized TRenewDelegationTokenResp renewDelegationToken(TRenewDelegationTokenReq request) throws TException {
        ensureConnected();
        TRenewDelegationTokenReq remote = request.deepCopy();
        remote.setSessionHandle(remoteSessionHandle());
        return invokeRemote(() -> client.RenewDelegationToken(remote));
    }

    public synchronized TGetQueryIdResp getQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        requireRemoteOperation(tGetQueryIdReq.getOperationHandle());
        return invokeConnected(() -> client.GetQueryId(tGetQueryIdReq));
    }

}
