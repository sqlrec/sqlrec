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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

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
    public TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
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
        if (connected) {
            return;
        }

        if (ExecEnv.isFileSystemMeta()) {
            throw new TException("Remote connection is not allowed in file system meta mode");
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

            // All remote operations succeeded, commit state atomically
            this.client = client;
            this.transport = transport;
            this.remoteSessionId = remoteSessionId;
            this.pendingOpenSessionReq = null;
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

    private THandleIdentifier copyHandleId(THandleIdentifier source) {
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
    public TCloseSessionResp CloseSession(TCloseSessionReq req) throws TException {
        try {
            if (connected) {
                THandleIdentifier originalSessionId = translateSessionHandle(req.getSessionHandle());
                try {
                    return client.CloseSession(req);
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
    public TGetInfoResp GetInfo(TGetInfoReq tGetInfoReq) throws TException {
        updateAccessTime();

        TGetInfoResp resp = new TGetInfoResp();
        resp.setStatus(new TStatus(TStatusCode.SUCCESS_STATUS));

        TGetInfoType infoType = tGetInfoReq.getInfoType();
        String infoValue;
        switch (infoType) {
            case CLI_DBMS_NAME:
                infoValue = "Apache Hive";
                break;
            case CLI_DBMS_VER:
                infoValue = "3.1.3";
                break;
            case CLI_SERVER_NAME:
                infoValue = "SQLRec";
                break;
            case CLI_CATALOG_NAME:
                infoValue = "hive";
                break;
            case CLI_DATA_SOURCE_NAME:
                infoValue = "SQLRec";
                break;
            case CLI_DATA_SOURCE_READ_ONLY:
                infoValue = "N";
                break;
            default:
                infoValue = "";
        }
        resp.setInfoValue(TGetInfoValue.stringValue(infoValue));

        return resp;
    }

    @Override
    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tExecuteStatementReq.getSessionHandle());
        try {
            return client.ExecuteStatement(tExecuteStatementReq);
        } finally {
            restoreSessionHandle(tExecuteStatementReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq tGetTypeInfoReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tGetTypeInfoReq.getSessionHandle());
        try {
            return client.GetTypeInfo(tGetTypeInfoReq);
        } finally {
            restoreSessionHandle(tGetTypeInfoReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetCatalogsResp GetCatalogs(TGetCatalogsReq tGetCatalogsReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tGetCatalogsReq.getSessionHandle());
        try {
            return client.GetCatalogs(tGetCatalogsReq);
        } finally {
            restoreSessionHandle(tGetCatalogsReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetSchemasResp GetSchemas(TGetSchemasReq tGetSchemasReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tGetSchemasReq.getSessionHandle());
        try {
            return client.GetSchemas(tGetSchemasReq);
        } finally {
            restoreSessionHandle(tGetSchemasReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetTablesResp GetTables(TGetTablesReq tGetTablesReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tGetTablesReq.getSessionHandle());
        try {
            return client.GetTables(tGetTablesReq);
        } finally {
            restoreSessionHandle(tGetTablesReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetTableTypesResp GetTableTypes(TGetTableTypesReq tGetTableTypesReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tGetTableTypesReq.getSessionHandle());
        try {
            return client.GetTableTypes(tGetTableTypesReq);
        } finally {
            restoreSessionHandle(tGetTableTypesReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetColumnsResp GetColumns(TGetColumnsReq tGetColumnsReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tGetColumnsReq.getSessionHandle());
        try {
            return client.GetColumns(tGetColumnsReq);
        } finally {
            restoreSessionHandle(tGetColumnsReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetFunctionsResp GetFunctions(TGetFunctionsReq tGetFunctionsReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tGetFunctionsReq.getSessionHandle());
        try {
            return client.GetFunctions(tGetFunctionsReq);
        } finally {
            restoreSessionHandle(tGetFunctionsReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq tGetPrimaryKeysReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tGetPrimaryKeysReq.getSessionHandle());
        try {
            return client.GetPrimaryKeys(tGetPrimaryKeysReq);
        } finally {
            restoreSessionHandle(tGetPrimaryKeysReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq tGetCrossReferenceReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tGetCrossReferenceReq.getSessionHandle());
        try {
            return client.GetCrossReference(tGetCrossReferenceReq);
        } finally {
            restoreSessionHandle(tGetCrossReferenceReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        updateAccessTime();
        ensureConnected();
        return client.GetOperationStatus(tGetOperationStatusReq);
    }

    @Override
    public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        updateAccessTime();
        ensureConnected();
        return client.CancelOperation(tCancelOperationReq);
    }

    @Override
    public TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        updateAccessTime();
        ensureConnected();
        return client.CloseOperation(tCloseOperationReq);
    }

    @Override
    public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        updateAccessTime();
        ensureConnected();
        return client.GetResultSetMetadata(tGetResultSetMetadataReq);
    }

    @Override
    public TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        updateAccessTime();
        ensureConnected();
        return client.FetchResults(tFetchResultsReq);
    }

    @Override
    public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq tGetDelegationTokenReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tGetDelegationTokenReq.getSessionHandle());
        try {
            return client.GetDelegationToken(tGetDelegationTokenReq);
        } finally {
            restoreSessionHandle(tGetDelegationTokenReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq tCancelDelegationTokenReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tCancelDelegationTokenReq.getSessionHandle());
        try {
            return client.CancelDelegationToken(tCancelDelegationTokenReq);
        } finally {
            restoreSessionHandle(tCancelDelegationTokenReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq tRenewDelegationTokenReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tRenewDelegationTokenReq.getSessionHandle());
        try {
            return client.RenewDelegationToken(tRenewDelegationTokenReq);
        } finally {
            restoreSessionHandle(tRenewDelegationTokenReq.getSessionHandle(), originalSessionId);
        }
    }

    @Override
    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        updateAccessTime();
        ensureConnected();
        return client.GetQueryId(tGetQueryIdReq);
    }

    @Override
    public TSetClientInfoResp SetClientInfo(TSetClientInfoReq tSetClientInfoReq) throws TException {
        updateAccessTime();
        ensureConnected();
        THandleIdentifier originalSessionId = translateSessionHandle(tSetClientInfoReq.getSessionHandle());
        try {
            return client.SetClientInfo(tSetClientInfoReq);
        } finally {
            restoreSessionHandle(tSetClientInfoReq.getSessionHandle(), originalSessionId);
        }
    }
}
