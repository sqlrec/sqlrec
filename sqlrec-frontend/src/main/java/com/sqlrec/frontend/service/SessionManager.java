package com.sqlrec.frontend.service;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.frontend.common.SqlProcessResult;
import com.sqlrec.frontend.common.SqlProcessor;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);

    private final Map<THandleIdentifier, TCLIService.Client> hiveClientMap = new ConcurrentHashMap<>();
    private final Map<THandleIdentifier, THandleIdentifier> operationToSessionMap = new ConcurrentHashMap<>();
    private final Map<THandleIdentifier, SqlProcessor> sqlProcessorMap = new ConcurrentHashMap<>();

    private final Map<THandleIdentifier, Long> sessionLastAccessTime = new ConcurrentHashMap<>();
    private final SessionTimeoutChecker timeoutChecker;

    public SessionManager() {
        this.timeoutChecker = new SessionTimeoutChecker(sessionLastAccessTime, this::cleanupSession);
    }

    public void startTimeoutChecker() {
        timeoutChecker.start();
    }

    public void stopTimeoutChecker() {
        timeoutChecker.stop();
    }

    private void cleanupSession(THandleIdentifier sessionId) {
        try {
            TCloseSessionReq tCloseSessionReq = new TCloseSessionReq();
            tCloseSessionReq.setSessionHandle(new TSessionHandle(sessionId));
            closeSession(tCloseSessionReq);
            logger.info("Session cleaned up: {}", sessionId);
        } catch (TException e) {
            logger.error("Failed to close session: {}", e.getMessage(), e);
        }
    }

    private void updateSessionAccessTime(THandleIdentifier sessionId) {
        sessionLastAccessTime.put(sessionId, System.currentTimeMillis());
    }

    public TOpenSessionResp openSession(TOpenSessionReq tOpenSessionReq) throws TException {
        logger.info("Opening session, user: {}, current map sizes - hiveClient: {}, sqlProcessor: {}, operationToSession: {}",
                tOpenSessionReq.getUsername(), hiveClientMap.size(), sqlProcessorMap.size(), operationToSessionMap.size());

        TTransport transport = new TSocket(
                SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getValue(),
                SqlRecConfigs.FLINK_SQL_GATEWAY_PORT.getValue(),
                SqlRecConfigs.FLINK_SQL_GATEWAY_CONNECT_TIMEOUT.getValue()
        );

        try {
            TProtocol protocol = new TBinaryProtocol(transport);
            TCLIService.Client client = new TCLIService.Client(protocol);
            transport.open();

            TOpenSessionResp resp = client.OpenSession(tOpenSessionReq);
            THandleIdentifier sessionId = resp.getSessionHandle().getSessionId();
            hiveClientMap.put(sessionId, client);
            sqlProcessorMap.put(sessionId, new SqlProcessor());
            updateSessionAccessTime(sessionId);
            logger.info("Session opened successfully, sessionId: {}", sessionId);
            return resp;
        } catch (Exception e) {
            logger.error("Failed to open session: {}", e.getMessage(), e);
            try {
                transport.close();
            } catch (Exception closeEx) {
                logger.warn("Failed to close transport during error handling", closeEx);
            }
            throw e;
        }
    }

    public TCloseSessionResp closeSession(TCloseSessionReq tCloseSessionReq) throws TException {
        THandleIdentifier sessionId = tCloseSessionReq.getSessionHandle().getSessionId();
        logger.info("Closing session, sessionId: {}, current map sizes - hiveClient: {}, sqlProcessor: {}, operationToSession: {}",
                sessionId, hiveClientMap.size(), sqlProcessorMap.size(), operationToSessionMap.size());

        TCLIService.Client client = getHiveClient(sessionId);
        if (client == null) {
            logger.warn("Session not found, sessionId: {}", sessionId);
            TCloseSessionResp resp = new TCloseSessionResp(new TStatus(TStatusCode.ERROR_STATUS));
            resp.getStatus().setErrorMessage("Session not found");
            sessionLastAccessTime.remove(sessionId);
            return resp;
        }

        hiveClientMap.remove(sessionId);
        sqlProcessorMap.remove(sessionId);
        sessionLastAccessTime.remove(sessionId);

        int removedOperations = 0;
        for (Map.Entry<THandleIdentifier, THandleIdentifier> entry : operationToSessionMap.entrySet()) {
            if (entry.getValue().equals(sessionId)) {
                operationToSessionMap.remove(entry.getKey());
                removedOperations++;
            }
        }

        TCloseSessionResp resp;
        try {
            resp = client.CloseSession(tCloseSessionReq);
        } finally {
            try {
                client.getInputProtocol().getTransport().close();
            } catch (Exception e) {
                logger.warn("Failed to close transport for session: {}", sessionId, e);
            }
        }
        logger.info("Session closed successfully, sessionId: {}, removed operations: {}", sessionId, removedOperations);
        return resp;
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
        updateSessionAccessTime(sessionId);
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
        logger.info("Executing statement, sessionId: {}, sql: {}", sessionId, tExecuteStatementReq.getStatement());

        TExecuteStatementResp resp = null;

        SqlProcessor sqlProcessor = getSqlProcessor(sessionId);
        try {
            SqlProcessResult sqlProcessResult = sqlProcessor.tryExecuteSql(tExecuteStatementReq.getStatement());
            if (sqlProcessResult != null) {
                TOperationHandle operationHandle = new TOperationHandle(
                        sqlProcessResult.getHandleIdentifier(), TOperationType.EXECUTE_STATEMENT, true
                );
                resp = new TExecuteStatementResp(new TStatus(TStatusCode.SUCCESS_STATUS));
                resp.setOperationHandle(operationHandle);
                logger.info("Statement executed by local SqlProcessor, operationId: {}", operationHandle.getOperationId());
            }
        } catch (Exception e) {
            logger.error("Failed to execute statement via SqlProcessor: {}", e.getMessage(), e);
            throw new TException(e);
        }

        if (resp == null) {
            TCLIService.Client client = getHiveClient(sessionId);
            if (client == null) {
                throw new TException("No client found for session: " + sessionId);
            }
            resp = client.ExecuteStatement(tExecuteStatementReq);
            logger.info("Statement executed by remote client, operationId: {}", resp.getOperationHandle().getOperationId());
        }

        THandleIdentifier operationId = resp.getOperationHandle().getOperationId();
        operationToSessionMap.put(operationId, sessionId);
        logger.info("Statement executed, sessionId: {}, operationId: {}, operationToSessionMap size: {}",
                sessionId, operationId, operationToSessionMap.size());
        return resp;
    }

    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        THandleIdentifier handleIdentifier = tGetOperationStatusReq.getOperationHandle().getOperationId();
        SqlProcessor sqlProcessor = getSqlProcessorByOperationId(handleIdentifier);
        if (sqlProcessor != null) {
            SqlProcessResult sqlProcessResult = sqlProcessor.getProcessResult(handleIdentifier);
            if (sqlProcessResult != null) {
                TOperationState operationState;
                if (sqlProcessResult.getException() != null) {
                    operationState = TOperationState.ERROR_STATE;
                } else {
                    try {
                        if (sqlProcessResult.isCompleted()) {
                            operationState = TOperationState.FINISHED_STATE;
                        } else {
                            operationState = TOperationState.RUNNING_STATE;
                        }
                    } catch (Exception e) {
                        sqlProcessResult.setException(e);
                        operationState = TOperationState.ERROR_STATE;
                    }
                }
                TGetOperationStatusResp resp = new TGetOperationStatusResp(new TStatus(TStatusCode.SUCCESS_STATUS));
                resp.setOperationState(operationState);
                resp.setHasResultSet(true);
                if (operationState == TOperationState.FINISHED_STATE || operationState == TOperationState.ERROR_STATE) {
                    resp.setOperationCompletedIsSet(true);
                }
                resp.setErrorMessage(sqlProcessResult.getMsg());
                return resp;
            }
        }

        TCLIService.Client client = getHiveClientByOperationId(handleIdentifier);
        if (client == null) {
            throw new TException("No client found for operation: " + handleIdentifier);
        }
        return client.GetOperationStatus(tGetOperationStatusReq);
    }

    public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        THandleIdentifier handleIdentifier = tGetResultSetMetadataReq.getOperationHandle().getOperationId();
        SqlProcessor sqlProcessor = getSqlProcessorByOperationId(handleIdentifier);
        if (sqlProcessor != null) {
            SqlProcessResult sqlProcessResult = sqlProcessor.getProcessResult(handleIdentifier);
            if (sqlProcessResult != null) {
                TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp(new TStatus(TStatusCode.SUCCESS_STATUS));
                if (sqlProcessResult.getFields() != null) {
                    resp.setSchema(Utils.convertFieldsToTTableSchema(sqlProcessResult.getFields()));
                } else {
                    resp.setSchema(Utils.convertFieldsToTTableSchema(DataTypeUtils.getStringTypeField("sys_warn")));
                }
                return resp;
            }
        }

        TCLIService.Client client = getHiveClientByOperationId(handleIdentifier);
        if (client == null) {
            throw new TException("No client found for operation: " + handleIdentifier);
        }
        return client.GetResultSetMetadata(tGetResultSetMetadataReq);
    }

    public TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        THandleIdentifier handleIdentifier = tFetchResultsReq.getOperationHandle().getOperationId();
        SqlProcessor sqlProcessor = getSqlProcessorByOperationId(handleIdentifier);
        if (sqlProcessor != null) {
            SqlProcessResult sqlProcessResult = sqlProcessor.getProcessResult(handleIdentifier);
            if (sqlProcessResult != null) {
                TFetchResultsResp resp = new TFetchResultsResp(new TStatus(TStatusCode.SUCCESS_STATUS));
                if (tFetchResultsReq.getFetchType() == 0) {
                    if (sqlProcessResult.getFields() != null) {
                        resp.setResults(Utils.convertObjectArrayToTRowSet(sqlProcessResult.getEnumerable(), sqlProcessResult.getFields()));
                        sqlProcessResult.setEnumerable(null);
                    } else {
                        resp.setResults(Utils.convertObjectArrayToTRowSet(
                                DataTransformUtils.getMsgEnumerable("no output"),
                                DataTypeUtils.getStringTypeField("sys_warn"))
                        );
                    }
                } else {
                    resp.setResults(Utils.convertObjectArrayToTRowSet(null, DataTypeUtils.getStringTypeField("log")));
                }
                resp.setHasMoreRows(false);
                return resp;
            }
        }

        TCLIService.Client client = getHiveClientByOperationId(handleIdentifier);
        if (client == null) {
            throw new TException("No client found for operation: " + handleIdentifier);
        }
        return client.FetchResults(tFetchResultsReq);
    }

    public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        THandleIdentifier operationId = tCancelOperationReq.getOperationHandle().getOperationId();
        THandleIdentifier sessionId = operationToSessionMap.get(operationId);
        logger.info("Canceling operation, operationId: {}, sessionId: {}", operationId, sessionId);

        operationToSessionMap.remove(operationId);

        SqlProcessor sqlProcessor = getSqlProcessor(sessionId);
        if (sqlProcessor != null && sqlProcessor.getProcessResult(operationId) != null) {
            sqlProcessor.closeProcessResult(operationId);
            logger.info("Operation canceled (local), operationId: {}", operationId);
            return new TCancelOperationResp(new TStatus(TStatusCode.SUCCESS_STATUS));
        }

        TCLIService.Client client = getHiveClient(sessionId);
        if (client != null) {
            logger.info("Operation canceled (remote), operationId: {}", operationId);
            return client.CancelOperation(tCancelOperationReq);
        }
        logger.warn("Operation cancel failed, no client found, operationId: {}", operationId);
        return null;
    }

    public TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        THandleIdentifier operationId = tCloseOperationReq.getOperationHandle().getOperationId();
        THandleIdentifier sessionId = operationToSessionMap.get(operationId);
        logger.info("Closing operation, operationId: {}, sessionId: {}, operationToSessionMap size: {}",
                operationId, sessionId, operationToSessionMap.size());

        operationToSessionMap.remove(operationId);

        SqlProcessor sqlProcessor = getSqlProcessor(sessionId);
        if (sqlProcessor != null && sqlProcessor.getProcessResult(operationId) != null) {
            sqlProcessor.closeProcessResult(operationId);
            logger.info("Operation closed (local), operationId: {}, remaining operationToSessionMap size: {}",
                    operationId, operationToSessionMap.size());
            return new TCloseOperationResp(new TStatus(TStatusCode.SUCCESS_STATUS));
        }

        TCLIService.Client client = getHiveClient(sessionId);
        if (client != null) {
            logger.info("Operation closed (remote), operationId: {}", operationId);
            return client.CloseOperation(tCloseOperationReq);
        }
        logger.warn("Operation close failed, no client found, operationId: {}", operationId);
        return null;
    }

    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        THandleIdentifier operationId = tGetQueryIdReq.getOperationHandle().getOperationId();
        SqlProcessor sqlProcessor = getSqlProcessorByOperationId(operationId);
        if (sqlProcessor != null && sqlProcessor.getProcessResult(operationId) != null) {
            return new TGetQueryIdResp(sqlProcessor.getProcessResult(operationId).getQueryId());
        }

        TCLIService.Client client = getHiveClientByOperationId(operationId);
        if (client != null) {
            return client.GetQueryId(tGetQueryIdReq);
        }
        return null;
    }
}
