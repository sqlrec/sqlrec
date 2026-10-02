package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;

import java.util.concurrent.Callable;

public class TCLIServiceImpl implements TCLIService.Iface {
    private final SessionManager sessionManager = new SessionManager();

    private JdbcMetadataExecutor metadata() {
        return new JdbcMetadataExecutor(MetadataAccessFactory.getInstance());
    }

    private record MetadataOutcome(TStatus status, TOperationHandle handle) {}

    private MetadataOutcome metadataOperation(TSessionHandle session, TOperationType type,
            Callable<SqlProcessResult> query) {
        try {
            return new MetadataOutcome(new TStatus(TStatusCode.SUCCESS_STATUS),
                    sessionManager.openMetadataOperation(session, type, query));
        } catch (Exception e) {
            return new MetadataOutcome(ThriftUtils.errorStatus(e), null);
        }
    }

    public TCLIServiceImpl() {
        sessionManager.startTimeoutChecker();
    }

    public void stop() {
        sessionManager.stopTimeoutChecker();
    }

    @Override
    public TOpenSessionResp OpenSession(TOpenSessionReq tOpenSessionReq) throws TException {
        return sessionManager.openSession(tOpenSessionReq);
    }

    @Override
    public TCloseSessionResp CloseSession(TCloseSessionReq tCloseSessionReq) throws TException {
        return sessionManager.closeSession(tCloseSessionReq);
    }

    @Override
    public TGetInfoResp GetInfo(TGetInfoReq tGetInfoReq) throws TException {
        if (!sessionManager.hasSession(tGetInfoReq.getSessionHandle())) {
            return new TGetInfoResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS), TGetInfoValue.stringValue(""));
        }
        String value = switch (tGetInfoReq.getInfoType()) {
            case CLI_DBMS_NAME -> "Apache Hive";
            case CLI_DBMS_VER -> "3.1.3";
            case CLI_SERVER_NAME, CLI_DATA_SOURCE_NAME -> "SQLRec";
            case CLI_CATALOG_NAME -> Consts.HIVE_CATALOG_NAME;
            case CLI_DATA_SOURCE_READ_ONLY -> SqlRecConfigs.isFileSystemMetadata() ? "Y" : "N";
            default -> "";
        };
        return new TGetInfoResp(new TStatus(TStatusCode.SUCCESS_STATUS), TGetInfoValue.stringValue(value));
    }

    @Override
    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        return sessionManager.executeStatement(tExecuteStatementReq);
    }

    @Override
    public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq req) throws TException {
        MetadataOutcome result = metadataOperation(req.getSessionHandle(), TOperationType.GET_TYPE_INFO,
                () -> metadata().typeInfo());
        TGetTypeInfoResp response = new TGetTypeInfoResp(result.status());
        if (result.handle() != null) {
            response.setOperationHandle(result.handle());
        }
        return response;
    }

    @Override
    public TGetCatalogsResp GetCatalogs(TGetCatalogsReq req) throws TException {
        MetadataOutcome result = metadataOperation(req.getSessionHandle(), TOperationType.GET_CATALOGS,
                () -> metadata().catalogs());
        TGetCatalogsResp response = new TGetCatalogsResp(result.status());
        if (result.handle() != null) {
            response.setOperationHandle(result.handle());
        }
        return response;
    }

    @Override
    public TGetSchemasResp GetSchemas(TGetSchemasReq req) throws TException {
        MetadataOutcome result = metadataOperation(req.getSessionHandle(), TOperationType.GET_SCHEMAS,
                () -> metadata().schemas(req.getCatalogName(), req.getSchemaName()));
        TGetSchemasResp response = new TGetSchemasResp(result.status());
        if (result.handle() != null) {
            response.setOperationHandle(result.handle());
        }
        return response;
    }

    @Override
    public TGetTablesResp GetTables(TGetTablesReq req) throws TException {
        MetadataOutcome result = metadataOperation(req.getSessionHandle(), TOperationType.GET_TABLES,
                () -> metadata().tables(req.getCatalogName(), req.getSchemaName(), req.getTableName(), req.getTableTypes()));
        TGetTablesResp response = new TGetTablesResp(result.status());
        if (result.handle() != null) {
            response.setOperationHandle(result.handle());
        }
        return response;
    }

    @Override
    public TGetTableTypesResp GetTableTypes(TGetTableTypesReq req) throws TException {
        MetadataOutcome result = metadataOperation(req.getSessionHandle(), TOperationType.GET_TABLE_TYPES,
                () -> metadata().tableTypes());
        TGetTableTypesResp response = new TGetTableTypesResp(result.status());
        if (result.handle() != null) {
            response.setOperationHandle(result.handle());
        }
        return response;
    }

    @Override
    public TGetColumnsResp GetColumns(TGetColumnsReq req) throws TException {
        MetadataOutcome result = metadataOperation(req.getSessionHandle(), TOperationType.GET_COLUMNS,
                () -> metadata().columns(req.getCatalogName(), req.getSchemaName(), req.getTableName(), req.getColumnName()));
        TGetColumnsResp response = new TGetColumnsResp(result.status());
        if (result.handle() != null) {
            response.setOperationHandle(result.handle());
        }
        return response;
    }

    @Override
    public TGetFunctionsResp GetFunctions(TGetFunctionsReq req) throws TException {
        MetadataOutcome result = metadataOperation(req.getSessionHandle(), TOperationType.GET_FUNCTIONS,
                () -> metadata().functions(req.getCatalogName(), req.getSchemaName(), req.getFunctionName()));
        TGetFunctionsResp response = new TGetFunctionsResp(result.status());
        if (result.handle() != null) {
            response.setOperationHandle(result.handle());
        }
        return response;
    }

    @Override
    public TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq req) throws TException {
        MetadataOutcome result = metadataOperation(req.getSessionHandle(), TOperationType.UNKNOWN,
                () -> metadata().primaryKeys(req.getCatalogName(), req.getSchemaName(), req.getTableName()));
        TGetPrimaryKeysResp response = new TGetPrimaryKeysResp(result.status());
        if (result.handle() != null) {
            response.setOperationHandle(result.handle());
        }
        return response;
    }

    @Override
    public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq req) throws TException {
        return sessionManager.getCrossReference(req);
    }

    @Override
    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        return sessionManager.getOperationStatus(tGetOperationStatusReq);
    }

    @Override
    public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        return sessionManager.cancelOperation(tCancelOperationReq);
    }

    @Override
    public TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        return sessionManager.closeOperation(tCloseOperationReq);
    }

    @Override
    public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        return sessionManager.getResultSetMetadata(tGetResultSetMetadataReq);
    }

    @Override
    public TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        return sessionManager.fetchResults(tFetchResultsReq);
    }

    @Override
    public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq req) throws TException {
        GatewayClient client = sessionManager.getGateway(req.getSessionHandle().getSessionId());
        return client == null ? new TGetDelegationTokenResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS))
                : client.getDelegationToken(req);
    }

    @Override
    public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq req) throws TException {
        GatewayClient client = sessionManager.getGateway(req.getSessionHandle().getSessionId());
        return client == null ? new TCancelDelegationTokenResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS))
                : client.cancelDelegationToken(req);
    }

    @Override
    public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq req) throws TException {
        GatewayClient client = sessionManager.getGateway(req.getSessionHandle().getSessionId());
        return client == null ? new TRenewDelegationTokenResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS))
                : client.renewDelegationToken(req);
    }

    @Override
    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        return sessionManager.getQueryId(tGetQueryIdReq);
    }

    @Override
    public TSetClientInfoResp SetClientInfo(TSetClientInfoReq tSetClientInfoReq) throws TException {
        if (!sessionManager.hasSession(tSetClientInfoReq.getSessionHandle())) {
            return new TSetClientInfoResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS));
        }
        return new TSetClientInfoResp(new TStatus(TStatusCode.SUCCESS_STATUS));
    }
}
