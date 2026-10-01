package com.sqlrec.frontend.thrift;

import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.executor.SqlProcessResult;
import java.util.concurrent.Callable;

import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;

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
            TStatus status = new TStatus(TStatusCode.ERROR_STATUS);
            status.setSqlState(e instanceof UnsupportedOperationException ? "0A000" : "HY000");
            status.setErrorMessage(e.getMessage());
            return new MetadataOutcome(status, null);
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
        TCLIService.Iface client = sessionManager.getClient(tGetInfoReq.getSessionHandle().getSessionId());
        if (client == null) {
            return new TGetInfoResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS),
                    TGetInfoValue.stringValue(""));
        }
        return client.GetInfo(tGetInfoReq);
    }

    @Override
    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        return sessionManager.ExecuteStatement(tExecuteStatementReq);
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
        return sessionManager.GetCrossReference(req);
    }

    @Override
    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        return sessionManager.GetOperationStatus(tGetOperationStatusReq);
    }

    @Override
    public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        return sessionManager.CancelOperation(tCancelOperationReq);
    }

    @Override
    public TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        return sessionManager.CloseOperation(tCloseOperationReq);
    }

    @Override
    public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        return sessionManager.GetResultSetMetadata(tGetResultSetMetadataReq);
    }

    @Override
    public TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        return sessionManager.FetchResults(tFetchResultsReq);
    }

    @Override
    public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq req) throws TException {
        TCLIService.Iface client = sessionManager.getClient(req.getSessionHandle().getSessionId());
        return client == null ? new TGetDelegationTokenResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS))
                : client.GetDelegationToken(req);
    }

    @Override
    public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq req) throws TException {
        TCLIService.Iface client = sessionManager.getClient(req.getSessionHandle().getSessionId());
        return client == null ? new TCancelDelegationTokenResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS))
                : client.CancelDelegationToken(req);
    }

    @Override
    public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq req) throws TException {
        TCLIService.Iface client = sessionManager.getClient(req.getSessionHandle().getSessionId());
        return client == null ? new TRenewDelegationTokenResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS))
                : client.RenewDelegationToken(req);
    }

    @Override
    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        return sessionManager.GetQueryId(tGetQueryIdReq);
    }

    @Override
    public TSetClientInfoResp SetClientInfo(TSetClientInfoReq tSetClientInfoReq) throws TException {
        if (sessionManager.getClient(tSetClientInfoReq.getSessionHandle().getSessionId()) == null) {
            return new TSetClientInfoResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS));
        }
        return new TSetClientInfoResp(new TStatus(TStatusCode.SUCCESS_STATUS));
    }
}
