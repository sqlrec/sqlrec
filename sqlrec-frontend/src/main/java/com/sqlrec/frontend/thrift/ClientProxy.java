package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.ExecEnv;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * A proxy owns one generated Thrift client and transport for a session.  Generated
 * clients and their protocols are not safe for concurrent request/response exchanges,
 * so every TCLIService entry point is synchronized for the whole reconnect/translate/
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

    public ClientProxy() {
        this.lastAccessTime = new AtomicLong(System.currentTimeMillis());
        this.connected = false;
    }

    @Override
    public synchronized TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
        this.localSessionId = ThriftUtils.getHandleIdentifier();
        this.pendingOpenSessionReq = req;

        TOpenSessionResp resp = new TOpenSessionResp();
        resp.setStatus(new TStatus(TStatusCode.SUCCESS_STATUS));
        resp.setSessionHandle(new TSessionHandle(localSessionId));
        resp.setServerProtocolVersion(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);
        resp.setConfiguration(new HashMap<>());

        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_SESSION_OPEN_COUNT)
                .increment();

        logger.info("Session opened (mock), sessionGuid: {}", ThriftUtils.safeHandleId(localSessionId));
        return resp;
    }

    private synchronized void ensureConnected() throws TException {
        if (connected && transport != null && transport.isOpen()) {
            return;
        }

        // Clean up any stale transport left over from a previous broken connection.
        if (transport != null) {
            try {
                transport.close();
            } catch (Exception ignore) {
                // ignore
            }
            this.client = null;
            this.transport = null;
            this.remoteSessionId = null;
        }
        this.connected = false;

        if (ExecEnv.isFileSystemMeta()) {
            throw new TException("Remote connection is not allowed in file system meta mode");
        }
        if (pendingOpenSessionReq == null) {
            throw new TException("Cannot (re)connect: OpenSession was never called");
        }

        TTransport transport = new TSocket(
                SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getValue(),
                SqlRecConfigs.FLINK_SQL_GATEWAY_PORT.getValue(),
                SqlRecConfigs.FLINK_SQL_GATEWAY_CONNECT_TIMEOUT.getValue()
        );
        try {
            TProtocol protocol = new TBinaryProtocol(transport);
            TCLIService.Client client = new TCLIService.Client(protocol);
            transport.open();

            TOpenSessionResp remoteResp = client.OpenSession(pendingOpenSessionReq);
            THandleIdentifier remoteSessionId = copyHandleId(remoteResp.getSessionHandle().getSessionId());

            // All remote operations succeeded, commit state atomically.
            // NOTE: pendingOpenSessionReq is intentionally retained so the session can be
            // re-opened after a future transport failure (see markDisconnected).
            this.client = client;
            this.transport = transport;
            this.remoteSessionId = remoteSessionId;
            this.connected = true;

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
            throw e;
        }
    }

    /**
     * Tear down the current remote connection without dropping session identity, so the
     * next call to {@link #ensureConnected()} re-opens the remote session. Used after a
     * transport-level failure so the broken client/transport is not reused forever.
     */
    private synchronized void markDisconnected() {
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
        ensureConnected();
        return invokeRemote(call);
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
            if (transport != null) {
                try {
                    transport.close();
                } catch (Exception e) {
                    logger.warn("Failed to close transport for sessionGuid: {}", ThriftUtils.safeHandleId(localSessionId), e);
                }
            }
            connected = false;
            client = null;
            transport = null;
            remoteSessionId = null;
            MetricsUtils.getCompositeMeterRegistry()
                    .counter(Consts.METRICS_SESSION_CLOSE_COUNT)
                    .increment();
        }
    }

    @Override
    public synchronized TGetInfoResp GetInfo(TGetInfoReq tGetInfoReq) throws TException {
        updateAccessTime();

        TGetInfoResp resp = new TGetInfoResp();
        resp.setStatus(new TStatus(TStatusCode.SUCCESS_STATUS));

        TGetInfoType infoType = tGetInfoReq.getInfoType();
        String infoValue = switch (infoType) {
            case CLI_DBMS_NAME -> "Apache Hive";
            case CLI_DBMS_VER -> "3.1.3";
            case CLI_SERVER_NAME, CLI_DATA_SOURCE_NAME -> "SQLRec";
            case CLI_CATALOG_NAME -> "hive";
            case CLI_DATA_SOURCE_READ_ONLY -> "N";
            default -> "";
        };
        resp.setInfoValue(TGetInfoValue.stringValue(infoValue));

        return resp;
    }

    @Override
    public synchronized TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        return invokeWithSessionHandle(
                tExecuteStatementReq.getSessionHandle(),
                () -> client.ExecuteStatement(tExecuteStatementReq)
        );
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
        return invokeWithSessionHandle(
                tGetCrossReferenceReq.getSessionHandle(),
                () -> client.GetCrossReference(tGetCrossReferenceReq)
        );
    }

    @Override
    public synchronized TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        return invokeConnected(() -> client.GetOperationStatus(tGetOperationStatusReq));
    }

    @Override
    public synchronized TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        return invokeConnected(() -> client.CancelOperation(tCancelOperationReq));
    }

    @Override
    public synchronized TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        return invokeConnected(() -> client.CloseOperation(tCloseOperationReq));
    }

    @Override
    public synchronized TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        return invokeConnected(() -> client.GetResultSetMetadata(tGetResultSetMetadataReq));
    }

    @Override
    public synchronized TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
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
