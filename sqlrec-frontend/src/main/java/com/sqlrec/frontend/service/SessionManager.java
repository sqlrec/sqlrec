package com.sqlrec.frontend.service;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
    private Map<THandleIdentifier, TCLIService.Client> hiveClientMap = new ConcurrentHashMap<>();
    private Map<THandleIdentifier, THandleIdentifier> operationToSessionMap = new ConcurrentHashMap<>();
    private Map<THandleIdentifier, SqlProcessor> sqlProcessorMap = new ConcurrentHashMap<>();

    public TOpenSessionResp openSession(TOpenSessionReq tOpenSessionReq) throws TException {
        TTransport transport = new TSocket("127.0.0.1", 30000, 300000);

        TProtocol protocol = new TBinaryProtocol(transport);
        TCLIService.Client client = new TCLIService.Client(protocol);

        transport.open();

        TOpenSessionResp resp = client.OpenSession(tOpenSessionReq);
        hiveClientMap.put(resp.getSessionHandle().getSessionId(), client);
        sqlProcessorMap.put(resp.getSessionHandle().getSessionId(), new SqlProcessor());

        return resp;
    }

    public TCloseSessionResp closeSession(TCloseSessionReq tCloseSessionReq) throws TException {
        TCLIService.Client client = getHiveClient(tCloseSessionReq.getSessionHandle().getSessionId());
        if (client != null) {
            hiveClientMap.remove(tCloseSessionReq.getSessionHandle().getSessionId());
            sqlProcessorMap.remove(tCloseSessionReq.getSessionHandle().getSessionId());
            return client.CloseSession(tCloseSessionReq);
        }
        return null;
    }

    public TCLIService.Client getHiveClient(THandleIdentifier sessionId) {
        return hiveClientMap.get(sessionId);
    }

    public TCLIService.Client getHiveClientByOperationId(THandleIdentifier operationId) {
        if (operationToSessionMap.containsKey(operationId)) {
            return getHiveClient(operationToSessionMap.get(operationId));
        }
        return null;
    }

    private SqlProcessor getSqlProcessor(THandleIdentifier sessionId) throws TException {
        SqlProcessor sqlProcessor = sqlProcessorMap.get(sessionId);
        if (sqlProcessor == null) {
            throw new TException("session not found");
        }
        return sqlProcessor;
    }

    private SqlProcessor getSqlProcessorByOperationId(THandleIdentifier operationId) throws TException {
        if (operationToSessionMap.containsKey(operationId)) {
            return getSqlProcessor(operationToSessionMap.get(operationId));
        }
        return null;
    }

    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        THandleIdentifier sessionId = tExecuteStatementReq.getSessionHandle().getSessionId();
        TExecuteStatementResp resp = null;

        SqlProcessor sqlProcessor = getSqlProcessor(sessionId);
        try {
            SqlProcessResult sqlProcessResult = sqlProcessor.tryExecuteSql(tExecuteStatementReq.getStatement());
            if (sqlProcessResult != null) {
                TOperationHandle operationHandle = new TOperationHandle(
                        sqlProcessResult.handleIdentifier, TOperationType.EXECUTE_STATEMENT, true
                );
                resp = new TExecuteStatementResp(new TStatus(TStatusCode.SUCCESS_STATUS));
                resp.setOperationHandle(operationHandle);
            }
        } catch (Exception e) {
            throw new TException(e);
        }

        if (resp == null) {
            TCLIService.Client client = getHiveClient(sessionId);
            resp = client.ExecuteStatement(tExecuteStatementReq);
        }

        operationToSessionMap.put(resp.getOperationHandle().getOperationId(), sessionId);
        return resp;
    }

    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        THandleIdentifier handleIdentifier = tGetOperationStatusReq.getOperationHandle().getOperationId();
        SqlProcessor sqlProcessor = getSqlProcessorByOperationId(handleIdentifier);
        if (sqlProcessor != null) {
            SqlProcessResult sqlProcessResult = sqlProcessor.getProcessProcessResult(handleIdentifier);
            if (sqlProcessResult != null) {
                TGetOperationStatusResp resp = new TGetOperationStatusResp(new TStatus(TStatusCode.SUCCESS_STATUS));
                resp.setOperationState(TOperationState.FINISHED_STATE);
                resp.setHasResultSet(true);
                resp.setOperationCompletedIsSet(true);
                return resp;
            }
        }

        TGetOperationStatusResp resp = getHiveClientByOperationId(handleIdentifier)
                .GetOperationStatus(tGetOperationStatusReq);
        return resp;
    }

    public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        THandleIdentifier handleIdentifier = tGetResultSetMetadataReq.getOperationHandle().getOperationId();
        SqlProcessor sqlProcessor = getSqlProcessorByOperationId(handleIdentifier);
        if (sqlProcessor != null) {
            SqlProcessResult sqlProcessResult = sqlProcessor.getProcessProcessResult(handleIdentifier);
            if (sqlProcessResult != null) {
                TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp(new TStatus(TStatusCode.SUCCESS_STATUS));
                if (sqlProcessResult.fields != null) {
                    resp.setSchema(Utils.convertFieldsToTTableSchema(sqlProcessResult.fields));
                }
                return resp;
            }
        }

        TGetResultSetMetadataResp resp = getHiveClientByOperationId(handleIdentifier)
                .GetResultSetMetadata(tGetResultSetMetadataReq);
        return resp;
    }

    public TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        THandleIdentifier handleIdentifier = tFetchResultsReq.getOperationHandle().getOperationId();
        SqlProcessor sqlProcessor = getSqlProcessorByOperationId(handleIdentifier);
        if (sqlProcessor != null) {
            SqlProcessResult sqlProcessResult = sqlProcessor.getProcessProcessResult(handleIdentifier);
            if (sqlProcessResult != null) {
                TFetchResultsResp resp = new TFetchResultsResp(new TStatus(TStatusCode.SUCCESS_STATUS));
                if (tFetchResultsReq.getFetchType() == FetchType.QUERY_OUTPUT.toTFetchType()) {
                    if (sqlProcessResult.fields != null) {
                        resp.setResults(Utils.convertObjectArrayToTRowSet(sqlProcessResult.enumerable, sqlProcessResult.fields));
                        sqlProcessResult.enumerable = null;
                    }
                } else {
                    Enumerable<Object[]> msgEnumerable = Utils.getMsgEnumerable(sqlProcessResult.msg);
                    resp.setResults(Utils.convertObjectArrayToTRowSet(msgEnumerable, Utils.getStringTypeFields("log")));
                    sqlProcessResult.msg = null;
                }
                resp.setHasMoreRows(false);
                return resp;
            }
        }

        TFetchResultsResp resp = getHiveClientByOperationId(handleIdentifier)
                .FetchResults(tFetchResultsReq);
        return resp;
    }

    public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        THandleIdentifier operationId = tCancelOperationReq.getOperationHandle().getOperationId();
        THandleIdentifier sessionId = operationToSessionMap.get(operationId);
        operationToSessionMap.remove(operationId);

        SqlProcessor sqlProcessor = getSqlProcessor(sessionId);
        if (sqlProcessor != null && sqlProcessor.getProcessProcessResult(operationId) != null) {
            sqlProcessor.closeProcessProcessResult(operationId);
            return new TCancelOperationResp(new TStatus(TStatusCode.SUCCESS_STATUS));
        }

        TCLIService.Client client = getHiveClient(sessionId);
        if (client != null) {
            return client.CancelOperation(tCancelOperationReq);
        }
        return null;
    }

    public TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        THandleIdentifier operationId = tCloseOperationReq.getOperationHandle().getOperationId();
        THandleIdentifier sessionId = operationToSessionMap.get(operationId);
        operationToSessionMap.remove(operationId);

        SqlProcessor sqlProcessor = getSqlProcessor(sessionId);
        if (sqlProcessor != null && sqlProcessor.getProcessProcessResult(operationId) != null) {
            sqlProcessor.closeProcessProcessResult(operationId);
            return new TCloseOperationResp(new TStatus(TStatusCode.SUCCESS_STATUS));
        }

        TCLIService.Client client = getHiveClient(sessionId);
        if (client != null) {
            return client.CloseOperation(tCloseOperationReq);
        }
        return null;
    }

    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        THandleIdentifier operationId = tGetQueryIdReq.getOperationHandle().getOperationId();
        SqlProcessor sqlProcessor = getSqlProcessorByOperationId(operationId);
        if (sqlProcessor != null && sqlProcessor.getProcessProcessResult(operationId) != null) {
            return new TGetQueryIdResp(sqlProcessor.getProcessProcessResult(operationId).queryId);
        }

        TCLIService.Client client = getHiveClientByOperationId(operationId);
        if (client != null) {
            return client.GetQueryId(tGetQueryIdReq);
        }
        return null;
    }
}
