package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.MetricsUtils;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class ClientProxy implements TCLIService.Iface {
    private static final Logger logger = LoggerFactory.getLogger(ClientProxy.class);

    private THandleIdentifier sessionId;
    private final TCLIService.Client client;
    private final TTransport transport;
    private final AtomicLong lastAccessTime;

    public ClientProxy() throws TException {
        TTransport transport = new TSocket(
                SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getValue(),
                SqlRecConfigs.FLINK_SQL_GATEWAY_PORT.getValue(),
                SqlRecConfigs.FLINK_SQL_GATEWAY_CONNECT_TIMEOUT.getValue()
        );
        try {
            TProtocol protocol = new TBinaryProtocol(transport);
            this.client = new TCLIService.Client(protocol);
            transport.open();
        } catch (Exception e) {
            logger.error("Failed to create client: {}", e.getMessage(), e);
            try {
                transport.close();
            } catch (Exception closeEx) {
                logger.warn("Failed to close transport during error handling", closeEx);
            }
            throw e;
        }
        this.transport = transport;
        this.lastAccessTime = new AtomicLong(System.currentTimeMillis());
    }

    public TOpenSessionResp openSession(TOpenSessionReq req) throws TException {
        TOpenSessionResp resp = client.OpenSession(req);
        this.sessionId = resp.getSessionHandle().getSessionId();

        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_SESSION_OPEN_COUNT)
                .increment();

        logger.info("Session opened successfully, sessionGuid: {}", Utils.safeHandleId(sessionId));
        return resp;
    }

    public THandleIdentifier getSessionId() {
        return sessionId;
    }

    public long getLastAccessTime() {
        return lastAccessTime.get();
    }

    public void updateAccessTime() {
        lastAccessTime.set(System.currentTimeMillis());
    }

    public void close() {
        try {
            transport.close();
        } catch (Exception e) {
            logger.warn("Failed to close transport for sessionGuid: {}", Utils.safeHandleId(sessionId), e);
        }
    }

    public TCloseSessionResp closeSession(TCloseSessionReq req) throws TException {
        try {
            return client.CloseSession(req);
        } finally {
            close();
            MetricsUtils.getCompositeMeterRegistry()
                    .counter(Consts.METRICS_SESSION_CLOSE_COUNT)
                    .increment();
        }
    }

    @Override
    public TOpenSessionResp OpenSession(TOpenSessionReq tOpenSessionReq) throws TException {
        updateAccessTime();
        return client.OpenSession(tOpenSessionReq);
    }

    @Override
    public TCloseSessionResp CloseSession(TCloseSessionReq tCloseSessionReq) throws TException {
        return closeSession(tCloseSessionReq);
    }

    @Override
    public TGetInfoResp GetInfo(TGetInfoReq tGetInfoReq) throws TException {
        updateAccessTime();
        return client.GetInfo(tGetInfoReq);
    }

    @Override
    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        updateAccessTime();
        return client.ExecuteStatement(tExecuteStatementReq);
    }

    @Override
    public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq tGetTypeInfoReq) throws TException {
        updateAccessTime();
        return client.GetTypeInfo(tGetTypeInfoReq);
    }

    @Override
    public TGetCatalogsResp GetCatalogs(TGetCatalogsReq tGetCatalogsReq) throws TException {
        updateAccessTime();
        return client.GetCatalogs(tGetCatalogsReq);
    }

    @Override
    public TGetSchemasResp GetSchemas(TGetSchemasReq tGetSchemasReq) throws TException {
        updateAccessTime();
        return client.GetSchemas(tGetSchemasReq);
    }

    @Override
    public TGetTablesResp GetTables(TGetTablesReq tGetTablesReq) throws TException {
        updateAccessTime();
        return client.GetTables(tGetTablesReq);
    }

    @Override
    public TGetTableTypesResp GetTableTypes(TGetTableTypesReq tGetTableTypesReq) throws TException {
        updateAccessTime();
        return client.GetTableTypes(tGetTableTypesReq);
    }

    @Override
    public TGetColumnsResp GetColumns(TGetColumnsReq tGetColumnsReq) throws TException {
        updateAccessTime();
        return client.GetColumns(tGetColumnsReq);
    }

    @Override
    public TGetFunctionsResp GetFunctions(TGetFunctionsReq tGetFunctionsReq) throws TException {
        updateAccessTime();
        return client.GetFunctions(tGetFunctionsReq);
    }

    @Override
    public TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq tGetPrimaryKeysReq) throws TException {
        updateAccessTime();
        return client.GetPrimaryKeys(tGetPrimaryKeysReq);
    }

    @Override
    public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq tGetCrossReferenceReq) throws TException {
        updateAccessTime();
        return client.GetCrossReference(tGetCrossReferenceReq);
    }

    @Override
    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        updateAccessTime();
        return client.GetOperationStatus(tGetOperationStatusReq);
    }

    @Override
    public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        updateAccessTime();
        return client.CancelOperation(tCancelOperationReq);
    }

    @Override
    public TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        updateAccessTime();
        return client.CloseOperation(tCloseOperationReq);
    }

    @Override
    public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        updateAccessTime();
        return client.GetResultSetMetadata(tGetResultSetMetadataReq);
    }

    @Override
    public TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        updateAccessTime();
        return client.FetchResults(tFetchResultsReq);
    }

    @Override
    public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq tGetDelegationTokenReq) throws TException {
        updateAccessTime();
        return client.GetDelegationToken(tGetDelegationTokenReq);
    }

    @Override
    public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq tCancelDelegationTokenReq) throws TException {
        updateAccessTime();
        return client.CancelDelegationToken(tCancelDelegationTokenReq);
    }

    @Override
    public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq tRenewDelegationTokenReq) throws TException {
        updateAccessTime();
        return client.RenewDelegationToken(tRenewDelegationTokenReq);
    }

    @Override
    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        updateAccessTime();
        return client.GetQueryId(tGetQueryIdReq);
    }

    @Override
    public TSetClientInfoResp SetClientInfo(TSetClientInfoReq tSetClientInfoReq) throws TException {
        updateAccessTime();
        return client.SetClientInfo(tSetClientInfoReq);
    }
}
