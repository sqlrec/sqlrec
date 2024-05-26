package com.sqlrec.frontend.service;

import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;

public class TCLIServiceImpl implements TCLIService.Iface {
    private SessionManager sessionManager = new SessionManager();

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
        return sessionManager.getHiveClient(tGetInfoReq.getSessionHandle().getSessionId()).GetInfo(tGetInfoReq);
    }

    @Override
    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        return sessionManager.ExecuteStatement(tExecuteStatementReq);
    }

    @Override
    public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq tGetTypeInfoReq) throws TException {
        return sessionManager.getHiveClient(tGetTypeInfoReq.getSessionHandle().getSessionId()).GetTypeInfo(tGetTypeInfoReq);
    }

    @Override
    public TGetCatalogsResp GetCatalogs(TGetCatalogsReq tGetCatalogsReq) throws TException {
        return sessionManager.getHiveClient(tGetCatalogsReq.getSessionHandle().getSessionId()).GetCatalogs(tGetCatalogsReq);
    }

    @Override
    public TGetSchemasResp GetSchemas(TGetSchemasReq tGetSchemasReq) throws TException {
        return sessionManager.getHiveClient(tGetSchemasReq.getSessionHandle().getSessionId()).GetSchemas(tGetSchemasReq);
    }

    @Override
    public TGetTablesResp GetTables(TGetTablesReq tGetTablesReq) throws TException {
        return sessionManager.getHiveClient(tGetTablesReq.getSessionHandle().getSessionId()).GetTables(tGetTablesReq);
    }

    @Override
    public TGetTableTypesResp GetTableTypes(TGetTableTypesReq tGetTableTypesReq) throws TException {
        return sessionManager.getHiveClient(tGetTableTypesReq.getSessionHandle().getSessionId()).GetTableTypes(tGetTableTypesReq);
    }

    @Override
    public TGetColumnsResp GetColumns(TGetColumnsReq tGetColumnsReq) throws TException {
        return sessionManager.getHiveClient(tGetColumnsReq.getSessionHandle().getSessionId()).GetColumns(tGetColumnsReq);
    }

    @Override
    public TGetFunctionsResp GetFunctions(TGetFunctionsReq tGetFunctionsReq) throws TException {
        return sessionManager.getHiveClient(tGetFunctionsReq.getSessionHandle().getSessionId()).GetFunctions(tGetFunctionsReq);
    }

    @Override
    public TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq tGetPrimaryKeysReq) throws TException {
        return sessionManager.getHiveClient(tGetPrimaryKeysReq.getSessionHandle().getSessionId()).GetPrimaryKeys(tGetPrimaryKeysReq);
    }

    @Override
    public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq tGetCrossReferenceReq) throws TException {
        return sessionManager.getHiveClient(tGetCrossReferenceReq.getSessionHandle().getSessionId()).GetCrossReference(tGetCrossReferenceReq);
    }

    @Override
    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        return sessionManager
                .getHiveClientByOperationId(tGetOperationStatusReq.getOperationHandle().getOperationId())
                .GetOperationStatus(tGetOperationStatusReq);
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
        return sessionManager
                .getHiveClientByOperationId(tGetResultSetMetadataReq.getOperationHandle().getOperationId())
                .GetResultSetMetadata(tGetResultSetMetadataReq);
    }

    @Override
    public TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        return sessionManager
                .getHiveClientByOperationId(tFetchResultsReq.getOperationHandle().getOperationId())
                .FetchResults(tFetchResultsReq);
    }

    @Override
    public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq tGetDelegationTokenReq) throws TException {
        return sessionManager.getHiveClient(tGetDelegationTokenReq.getSessionHandle().getSessionId()).GetDelegationToken(tGetDelegationTokenReq);
    }

    @Override
    public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq tCancelDelegationTokenReq) throws TException {
        return sessionManager.getHiveClient(tCancelDelegationTokenReq.getSessionHandle().getSessionId()).CancelDelegationToken(tCancelDelegationTokenReq);
    }

    @Override
    public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq tRenewDelegationTokenReq) throws TException {
        return sessionManager.getHiveClient(tRenewDelegationTokenReq.getSessionHandle().getSessionId()).RenewDelegationToken(tRenewDelegationTokenReq);
    }

    @Override
    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        return sessionManager
                .getHiveClientByOperationId(tGetQueryIdReq.getOperationHandle().getOperationId())
                .GetQueryId(tGetQueryIdReq);
    }

    @Override
    public TSetClientInfoResp SetClientInfo(TSetClientInfoReq tSetClientInfoReq) throws TException {
        return sessionManager.getHiveClient(tSetClientInfoReq.getSessionHandle().getSessionId()).SetClientInfo(tSetClientInfoReq);
    }
}
