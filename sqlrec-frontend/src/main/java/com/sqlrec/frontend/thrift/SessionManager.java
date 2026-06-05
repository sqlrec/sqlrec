package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.frontend.common.SqlProcessResult;
import com.sqlrec.frontend.common.SqlProcessor;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);

    private final Map<THandleIdentifier, ClientProxy> clientMap = new ConcurrentHashMap<>();
    private final Map<THandleIdentifier, THandleIdentifier> operationToSessionMap = new ConcurrentHashMap<>();
    private final Map<THandleIdentifier, SqlProcessor> sqlProcessorMap = new ConcurrentHashMap<>();

    private final SessionTimeoutChecker timeoutChecker;

    public SessionManager() {
        this.timeoutChecker = new SessionTimeoutChecker(clientMap, this::cleanupSession);

        MetricsUtils.getCompositeMeterRegistry()
                .gauge(Consts.METRICS_SESSION_ACTIVE_COUNT, clientMap, Map::size);
        MetricsUtils.getCompositeMeterRegistry()
                .gauge(Consts.METRICS_OPERATION_ACTIVE_COUNT, operationToSessionMap, Map::size);
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
            logger.info("Session cleaned up, sessionGuid: {}", Utils.safeHandleId(sessionId));
        } catch (TException e) {
            logger.error("Failed to close session: {}", e.getMessage(), e);
        }
    }

    public TOpenSessionResp openSession(TOpenSessionReq tOpenSessionReq) throws TException {
        logger.info("Opening session, user: {}, current map sizes - client: {}, sqlProcessor: {}, operationToSession: {}",
                tOpenSessionReq.getUsername(), clientMap.size(), sqlProcessorMap.size(), operationToSessionMap.size());

        ClientProxy proxy = new ClientProxy();
        TOpenSessionResp resp = proxy.openSession(tOpenSessionReq);
        THandleIdentifier sessionId = proxy.getSessionId();
        clientMap.put(sessionId, proxy);
        sqlProcessorMap.put(sessionId, new SqlProcessor());

        logger.info("Session opened successfully, sessionGuid: {}", Utils.safeHandleId(sessionId));
        return resp;
    }

    public TCloseSessionResp closeSession(TCloseSessionReq tCloseSessionReq) throws TException {
        THandleIdentifier sessionId = tCloseSessionReq.getSessionHandle().getSessionId();
        logger.info("Closing session, sessionGuid: {}, current map sizes - client: {}, sqlProcessor: {}, operationToSession: {}",
                Utils.safeHandleId(sessionId), clientMap.size(), sqlProcessorMap.size(), operationToSessionMap.size());

        ClientProxy proxy = clientMap.remove(sessionId);
        if (proxy == null) {
            logger.warn("Session not found, sessionGuid: {}", Utils.safeHandleId(sessionId));
            TCloseSessionResp resp = new TCloseSessionResp(new TStatus(TStatusCode.ERROR_STATUS));
            resp.getStatus().setErrorMessage("Session not found");
            return resp;
        }

        sqlProcessorMap.remove(sessionId);

        int removedOperations = 0;
        for (Map.Entry<THandleIdentifier, THandleIdentifier> entry : operationToSessionMap.entrySet()) {
            if (entry.getValue().equals(sessionId)) {
                operationToSessionMap.remove(entry.getKey());
                removedOperations++;
            }
        }

        TCloseSessionResp resp = proxy.closeSession(tCloseSessionReq);
        logger.info("Session closed successfully, sessionGuid: {}, removed operations: {}", Utils.safeHandleId(sessionId), removedOperations);
        return resp;
    }

    public TCLIService.Iface getClient(THandleIdentifier sessionId) {
        return clientMap.get(sessionId);
    }

    public TCLIService.Iface getClientByOperationId(THandleIdentifier operationId) {
        if (operationToSessionMap.containsKey(operationId)) {
            return getClient(operationToSessionMap.get(operationId));
        }
        return null;
    }

    private SqlProcessor getSqlProcessor(THandleIdentifier sessionId) throws TException {
        SqlProcessor sqlProcessor = sqlProcessorMap.get(sessionId);
        if (sqlProcessor == null) {
            throw new TException("session not found");
        }
        ClientProxy proxy = clientMap.get(sessionId);
        if (proxy != null) {
            proxy.updateAccessTime();
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
        logger.info("Executing statement, sessionGuid: {}, sql: {}", Utils.safeHandleId(sessionId), tExecuteStatementReq.getStatement());

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
                logger.info("Statement executed by local SqlProcessor, operationGuid: {}", Utils.safeHandleId(operationHandle.getOperationId()));
            }
        } catch (Exception e) {
            logger.error("Failed to execute statement via SqlProcessor: {}", e.getMessage(), e);
            throw new TException(e);
        }

        if (resp == null) {
            TCLIService.Iface client = getClient(sessionId);
            if (client == null) {
                throw new TException("No client found for session, sessionGuid: " + Utils.safeHandleId(sessionId));
            }
            resp = client.ExecuteStatement(tExecuteStatementReq);
            logger.info("Statement executed by remote client, operationGuid: {}", Utils.safeHandleId(resp.getOperationHandle().getOperationId()));
        }

        THandleIdentifier operationId = resp.getOperationHandle().getOperationId();
        operationToSessionMap.put(operationId, sessionId);

        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_OPERATION_OPEN_COUNT)
                .increment();

        logger.info("Statement executed, sessionGuid: {}, operationGuid: {}, operationToSessionMap size: {}",
                Utils.safeHandleId(sessionId), Utils.safeHandleId(operationId), operationToSessionMap.size());
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
                        logger.error("Failed to get operation status: {}", e.getMessage(), e);
                        sqlProcessResult.setException(e);
                        sqlProcessResult.setMsg(e.getMessage() + " stack trace: " + ExceptionUtils.getStackTrace(e));
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

        TCLIService.Iface client = getClientByOperationId(handleIdentifier);
        if (client == null) {
            throw new TException("No client found for operation, operationGuid: " + Utils.safeHandleId(handleIdentifier));
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

        TCLIService.Iface client = getClientByOperationId(handleIdentifier);
        if (client == null) {
            throw new TException("No client found for operation, operationGuid: " + Utils.safeHandleId(handleIdentifier));
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

        TCLIService.Iface client = getClientByOperationId(handleIdentifier);
        if (client == null) {
            throw new TException("No client found for operation, operationGuid: " + Utils.safeHandleId(handleIdentifier));
        }
        return client.FetchResults(tFetchResultsReq);
    }

    public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        THandleIdentifier operationId = tCancelOperationReq.getOperationHandle().getOperationId();
        THandleIdentifier sessionId = operationToSessionMap.get(operationId);
        logger.info("Canceling operation, operationGuid: {}, sessionGuid: {}", Utils.safeHandleId(operationId), Utils.safeHandleId(sessionId));

        operationToSessionMap.remove(operationId);

        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_OPERATION_CLOSE_COUNT)
                .increment();

        SqlProcessor sqlProcessor = getSqlProcessor(sessionId);
        if (sqlProcessor != null && sqlProcessor.getProcessResult(operationId) != null) {
            sqlProcessor.closeProcessResult(operationId);
            logger.info("Operation canceled (local), operationGuid: {}", Utils.safeHandleId(operationId));
            return new TCancelOperationResp(new TStatus(TStatusCode.SUCCESS_STATUS));
        }

        TCLIService.Iface client = getClient(sessionId);
        if (client != null) {
            logger.info("Operation canceled (remote), operationGuid: {}", Utils.safeHandleId(operationId));
            return client.CancelOperation(tCancelOperationReq);
        }
        logger.warn("Operation cancel failed, no client found, operationGuid: {}", Utils.safeHandleId(operationId));
        return null;
    }

    public TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        THandleIdentifier operationId = tCloseOperationReq.getOperationHandle().getOperationId();
        THandleIdentifier sessionId = operationToSessionMap.get(operationId);
        logger.info("Closing operation, operationGuid: {}, sessionGuid: {}, operationToSessionMap size: {}",
                Utils.safeHandleId(operationId), Utils.safeHandleId(sessionId), operationToSessionMap.size());

        operationToSessionMap.remove(operationId);

        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_OPERATION_CLOSE_COUNT)
                .increment();

        SqlProcessor sqlProcessor = getSqlProcessor(sessionId);
        if (sqlProcessor != null && sqlProcessor.getProcessResult(operationId) != null) {
            sqlProcessor.closeProcessResult(operationId);
            logger.info("Operation closed (local), operationGuid: {}, remaining operationToSessionMap size: {}",
                    Utils.safeHandleId(operationId), operationToSessionMap.size());
            return new TCloseOperationResp(new TStatus(TStatusCode.SUCCESS_STATUS));
        }

        TCLIService.Iface client = getClient(sessionId);
        if (client != null) {
            logger.info("Operation closed (remote), operationGuid: {}", Utils.safeHandleId(operationId));
            return client.CloseOperation(tCloseOperationReq);
        }
        logger.warn("Operation close failed, no client found, operationGuid: {}", Utils.safeHandleId(operationId));
        return null;
    }

    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        THandleIdentifier operationId = tGetQueryIdReq.getOperationHandle().getOperationId();
        SqlProcessor sqlProcessor = getSqlProcessorByOperationId(operationId);
        if (sqlProcessor != null && sqlProcessor.getProcessResult(operationId) != null) {
            return new TGetQueryIdResp(sqlProcessor.getProcessResult(operationId).getQueryId());
        }

        TCLIService.Iface client = getClientByOperationId(operationId);
        if (client != null) {
            return client.GetQueryId(tGetQueryIdReq);
        }
        return null;
    }
}
