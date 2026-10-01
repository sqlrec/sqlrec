package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.sql.parser.ddl.SqlReset;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);

    private final Map<THandleIdentifier, ClientProxy> clientMap = new ConcurrentHashMap<>();
    private final Map<THandleIdentifier, THandleIdentifier> operationToSessionMap = new ConcurrentHashMap<>();
    private final Map<THandleIdentifier, SqlExecutor> sqlExecutorMap = new ConcurrentHashMap<>();
    private final Map<THandleIdentifier, SqlOperation> operationMap = new ConcurrentHashMap<>();

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
            logger.info("Session cleaned up, sessionGuid: {}", ThriftUtils.safeHandleId(sessionId));
        } catch (TException e) {
            logger.error("Failed to close session: {}", e.getMessage(), e);
        }
    }

    public TOpenSessionResp openSession(TOpenSessionReq tOpenSessionReq) throws TException {
        logger.info("Opening session, user: {}, current map sizes - client: {}, sqlExecutor: {}, operation: {}",
                tOpenSessionReq.getUsername(), clientMap.size(), sqlExecutorMap.size(), operationMap.size());

        // Build the executor before publishing a session; a metadata initialization failure must not leak it.
        SqlExecutor executor = new SqlExecutor();
        ClientProxy proxy = new ClientProxy();
        TOpenSessionResp resp = proxy.OpenSession(tOpenSessionReq);
        THandleIdentifier sessionId = proxy.getSessionId();
        clientMap.put(sessionId, proxy);
        sqlExecutorMap.put(sessionId, executor);

        logger.info("Session opened successfully, sessionGuid: {}", ThriftUtils.safeHandleId(sessionId));
        return resp;
    }

    public TCloseSessionResp closeSession(TCloseSessionReq tCloseSessionReq) throws TException {
        SqlExecutor executor = sqlExecutorMap.get(tCloseSessionReq.getSessionHandle().getSessionId());
        if (executor == null) {
            return new TCloseSessionResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS));
        }
        synchronized (executor) {
            return closeSessionLocked(tCloseSessionReq);
        }
    }

    private TCloseSessionResp closeSessionLocked(TCloseSessionReq tCloseSessionReq) throws TException {
        THandleIdentifier sessionId = tCloseSessionReq.getSessionHandle().getSessionId();
        logger.info("Closing session, sessionGuid: {}, current map sizes - client: {}, sqlExecutor: {}, operation: {}",
                ThriftUtils.safeHandleId(sessionId), clientMap.size(), sqlExecutorMap.size(), operationMap.size());

        ClientProxy proxy = clientMap.remove(sessionId);
        if (proxy == null) {
            logger.warn("Session not found, sessionGuid: {}", ThriftUtils.safeHandleId(sessionId));
            TCloseSessionResp resp = new TCloseSessionResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS));
            resp.getStatus().setErrorMessage("Session not found");
            return resp;
        }

        sqlExecutorMap.remove(sessionId);

        List<THandleIdentifier> operationsToRemove = new ArrayList<>();
        for (Map.Entry<THandleIdentifier, THandleIdentifier> entry : operationToSessionMap.entrySet()) {
            if (entry.getValue().equals(sessionId)) {
                operationsToRemove.add(entry.getKey());
            }
        }
        for (THandleIdentifier operationId : operationsToRemove) {
            operationToSessionMap.remove(operationId);
            operationMap.remove(operationId);
        }
        int removedOperations = operationsToRemove.size();

        TCloseSessionResp resp = proxy.CloseSession(tCloseSessionReq);
        logger.info("Session closed successfully, sessionGuid: {}, removed operations: {}", ThriftUtils.safeHandleId(sessionId), removedOperations);
        return resp;
    }

    public TCLIService.Iface getClient(THandleIdentifier sessionId) {
        ClientProxy client = clientMap.get(sessionId);
        if (client != null) {
            client.updateAccessTime();
        }
        return client;
    }

    public TCLIService.Iface getClientByOperationId(THandleIdentifier operationId) {
        THandleIdentifier sessionId = operationToSessionMap.get(operationId);
        return sessionId == null ? null : getClient(sessionId);
    }

    private void touchOperation(THandleIdentifier operationId) {
        // Local metadata polling and paging must keep its owning session alive too.
        getClientByOperationId(operationId);
    }

    private TCLIService.Iface getRequiredClientByOperationId(THandleIdentifier operationId)
            throws TException {
        TCLIService.Iface client = getClientByOperationId(operationId);
        if (client == null) {
            throw new TException("No client found for operation, operationGuid: "
                    + ThriftUtils.safeHandleId(operationId));
        }
        return client;
    }

    private SqlExecutor getSqlExecutor(THandleIdentifier sessionId) throws TException {
        SqlExecutor sqlExecutor = sqlExecutorMap.get(sessionId);
        if (sqlExecutor == null) {
            throw new TException("session not found");
        }
        ClientProxy proxy = clientMap.get(sessionId);
        if (proxy != null) {
            proxy.updateAccessTime();
        }
        return sqlExecutor;
    }

    private void requireActiveSession(THandleIdentifier sessionId, SqlExecutor executor) throws TException {
        if (sqlExecutorMap.get(sessionId) != executor) {
            throw new TException("Session was closed");
        }
    }

    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        SqlExecutor executor = getSqlExecutor(tExecuteStatementReq.getSessionHandle().getSessionId());
        synchronized (executor) {
            try {
                requireActiveSession(tExecuteStatementReq.getSessionHandle().getSessionId(), executor);
                return executeStatement(tExecuteStatementReq);
            } catch (TException e) {
                TStatus status = new TStatus(TStatusCode.ERROR_STATUS);
                status.setErrorMessage(e.getMessage());
                status.setSqlState(e.getMessage() != null && e.getMessage().startsWith("FLINK_GATEWAY_UNAVAILABLE")
                        ? "08001" : "HY000");
                return new TExecuteStatementResp(status);
            }
        }
    }

    private TExecuteStatementResp executeStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        THandleIdentifier sessionId = tExecuteStatementReq.getSessionHandle().getSessionId();
        logger.info("Executing statement, sessionGuid: {}, sql: {}", ThriftUtils.safeHandleId(sessionId), tExecuteStatementReq.getStatement());

        THandleIdentifier operationId = ThriftUtils.getHandleIdentifier();
        String queryId = ThriftUtils.getQueryId();
        SqlExecutor sqlExecutor = getSqlExecutor(sessionId);
        SqlOperation operation = executeLocally(
                sqlExecutor,
                tExecuteStatementReq.getStatement(),
                operationId,
                queryId
        );

        TExecuteStatementResp resp;
        if (operation != null) {
            ClientProxy proxy = clientMap.get(sessionId);
            if (proxy != null && operation.getException() == null) {
                proxy.setSessionState(sqlExecutor.getDefaultSchema(), sqlExecutor.getSessionSettings());
            }
            operationMap.put(operationId, operation);
            resp = createLocalExecuteResponse(operationId);
        } else {
            resp = executeRemotely(tExecuteStatementReq, sessionId);
        }

        if (!ThriftUtils.isSuccess(resp.getStatus())) {
            return resp;
        }

        TOperationHandle operationHandle = resp.getOperationHandle();
        if (operationHandle == null) {
            // Defensive guard: should not happen for the local-execution path, but avoid NPE if it does.
            throw new TException("ExecuteStatement returned no operation handle, sessionGuid: "
                    + ThriftUtils.safeHandleId(sessionId));
        }
        operationId = operationHandle.getOperationId();
        operationToSessionMap.put(operationId, sessionId);

        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_OPERATION_OPEN_COUNT)
                .increment();

        logger.info("Statement executed, sessionGuid: {}, operationGuid: {}, operationToSessionMap size: {}",
                ThriftUtils.safeHandleId(sessionId), ThriftUtils.safeHandleId(operationId), operationToSessionMap.size());
        return resp;
    }

    private SqlOperation executeLocally(
            SqlExecutor sqlExecutor,
            String sql,
            THandleIdentifier operationId,
            String queryId
    ) {
        try {
            SqlProcessResult coreResult = sqlExecutor.executeSqlAsync(sql);
            if (coreResult != null) {
                logger.info("Statement executed by local SqlExecutor, operationGuid: {}",
                        ThriftUtils.safeHandleId(operationId));
                return new SqlOperation(coreResult, operationId, queryId);
            }
        } catch (Exception e) {
            logger.error("Failed to execute statement via SqlExecutor: {}", e.getMessage(), e);
            SqlOperation operation = new SqlOperation(new SqlProcessResult(), operationId, queryId);
            operation.setException(e);
            operation.setMsg("exec error: " + ExceptionUtils.getStackTrace(e));
            return operation;
        }
        return null;
    }

    private TExecuteStatementResp createLocalExecuteResponse(THandleIdentifier operationId) {
        TOperationHandle operationHandle = new TOperationHandle(
                operationId, TOperationType.EXECUTE_STATEMENT, true
        );
        TExecuteStatementResp response = new TExecuteStatementResp(
                new TStatus(TStatusCode.SUCCESS_STATUS)
        );
        response.setOperationHandle(operationHandle);
        return response;
    }

    private TExecuteStatementResp executeRemotely(
            TExecuteStatementReq request,
            THandleIdentifier sessionId
    ) throws TException {
        TCLIService.Iface client = getClient(sessionId);
        if (client == null) {
            throw new TException("No client found for session, sessionGuid: "
                    + ThriftUtils.safeHandleId(sessionId));
        }
        try {
            if (CompileManager.parseSql(request.getStatement()) instanceof SqlReset reset) {
                ClientProxy proxy = clientMap.get(sessionId);
                proxy.executeSessionCommand(request.getStatement());
                SqlExecutor executor = getSqlExecutor(sessionId);
                executor.resetSessionSettings(reset.getKeyString());
                proxy.setSessionState(executor.getDefaultSchema(), executor.getSessionSettings());
                THandleIdentifier id = ThriftUtils.getHandleIdentifier();
                operationMap.put(id, new SqlOperation(SqlProcessResult.msg("Flink settings reset", "msg"),
                        id, ThriftUtils.getQueryId()));
                return createLocalExecuteResponse(id);
            }
        } catch (TException e) {
            throw e;
        } catch (Exception e) {
            throw new TException(e);
        }
        TExecuteStatementResp response = client.ExecuteStatement(request);
        if (response.getOperationHandle() != null) {
            logger.info("Statement executed by remote client, operationGuid: {}",
                    ThriftUtils.safeHandleId(response.getOperationHandle().getOperationId()));
        }
        return response;
    }

    public TOperationHandle openMetadataOperation(TSessionHandle session, TOperationType type,
            java.util.concurrent.Callable<SqlProcessResult> query) throws Exception {
        SqlExecutor executor = getSqlExecutor(session.getSessionId());
        synchronized (executor) {
            requireActiveSession(session.getSessionId(), executor);
            SqlProcessResult result = query.call();
            THandleIdentifier id = ThriftUtils.getHandleIdentifier();
            operationMap.put(id, SqlOperation.metadata(result, id, ThriftUtils.getQueryId()));
            operationToSessionMap.put(id, session.getSessionId());
            MetricsUtils.getCompositeMeterRegistry().counter(Consts.METRICS_OPERATION_OPEN_COUNT).increment();
            return new TOperationHandle(id, type, true);
        }
    }

    public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq request) throws TException {
        THandleIdentifier sessionId = request.getSessionHandle().getSessionId();
        SqlExecutor executor = getSqlExecutor(sessionId);
        synchronized (executor) {
            requireActiveSession(sessionId, executor);
            TGetCrossReferenceResp response = clientMap.get(sessionId).GetCrossReference(request);
            if (ThriftUtils.isSuccess(response.getStatus()) && response.getOperationHandle() != null) {
                operationToSessionMap.put(response.getOperationHandle().getOperationId(), sessionId);
                MetricsUtils.getCompositeMeterRegistry().counter(Consts.METRICS_OPERATION_OPEN_COUNT).increment();
            }
            return response;
        }
    }

    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq) throws TException {
        THandleIdentifier handleIdentifier = tGetOperationStatusReq.getOperationHandle().getOperationId();
        touchOperation(handleIdentifier);
        SqlOperation operation = operationMap.get(handleIdentifier);
        if (operation != null) {
            TOperationState operationState = getOperationState(operation);
            TGetOperationStatusResp resp = new TGetOperationStatusResp(new TStatus(TStatusCode.SUCCESS_STATUS));
            resp.setOperationState(operationState);
            resp.setHasResultSet(true);
            resp.setErrorMessage(operation.getMsg());
            return resp;
        }

        return getRequiredClientByOperationId(handleIdentifier)
                .GetOperationStatus(tGetOperationStatusReq);
    }

    private TOperationState getOperationState(SqlOperation operation) {
        if (operation.getException() != null) {
            return TOperationState.ERROR_STATE;
        }
        try {
            return operation.isCompleted()
                    ? TOperationState.FINISHED_STATE
                    : TOperationState.RUNNING_STATE;
        } catch (Exception e) {
            logger.error("Failed to get operation status: {}", e.getMessage(), e);
            operation.setException(e);
            operation.setMsg(e.getMessage() + " stack trace: " + ExceptionUtils.getStackTrace(e));
            return TOperationState.ERROR_STATE;
        }
    }

    public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        THandleIdentifier handleIdentifier = tGetResultSetMetadataReq.getOperationHandle().getOperationId();
        touchOperation(handleIdentifier);
        SqlOperation operation = operationMap.get(handleIdentifier);
        if (operation != null) {
            TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp(new TStatus(TStatusCode.SUCCESS_STATUS));
            if (operation.getFields() != null) {
                resp.setSchema(ThriftUtils.convertFieldsToTTableSchema(operation.getFields()));
            } else {
                resp.setSchema(ThriftUtils.convertFieldsToTTableSchema(DataTypeUtils.getStringTypeField("sys_warn")));
            }
            return resp;
        }

        return getRequiredClientByOperationId(handleIdentifier)
                .GetResultSetMetadata(tGetResultSetMetadataReq);
    }

    public TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        THandleIdentifier handleIdentifier = tFetchResultsReq.getOperationHandle().getOperationId();
        touchOperation(handleIdentifier);
        SqlOperation operation = operationMap.get(handleIdentifier);
        if (operation != null) {
            TFetchResultsResp resp = new TFetchResultsResp(new TStatus(TStatusCode.SUCCESS_STATUS));
            if (tFetchResultsReq.getFetchType() != 0) {
                resp.setResults(ThriftUtils.convertObjectArrayToTRowSet(null, DataTypeUtils.getStringTypeField("log")));
                resp.setHasMoreRows(false);
                return resp;
            }
            if (!operation.isMetadataOperation()) {
                if (operation.getFields() != null) {
                    resp.setResults(ThriftUtils.convertObjectArrayToTRowSet(operation.getEnumerable(), operation.getFields()));
                    operation.setEnumerable(null);
                } else {
                    resp.setResults(ThriftUtils.convertObjectArrayToTRowSet(
                            DataTransformUtils.getMsgEnumerable("no output"),
                            DataTypeUtils.getStringTypeField("sys_warn")));
                }
                resp.setHasMoreRows(false);
                return resp;
            }
            try {
                SqlOperation.ResultPage page = operation.fetch(tFetchResultsReq.getOrientation(), tFetchResultsReq.getMaxRows());
                TRowSet rows = ThriftUtils.convertObjectArrayToTRowSet(page.rows(),
                        operation.getFields() == null ? DataTypeUtils.getStringTypeField("sys_warn") : operation.getFields());
                rows.setStartRowOffset(page.offset());
                resp.setResults(rows);
                resp.setHasMoreRows(page.hasMoreRows());
            } catch (Exception e) {
                TStatus status = new TStatus(TStatusCode.ERROR_STATUS);
                status.setErrorMessage(e.getMessage());
                status.setSqlState(e instanceof UnsupportedOperationException ? "0A000" : "HY000");
                resp.setStatus(status);
            }
            return resp;
        }

        return getRequiredClientByOperationId(handleIdentifier).FetchResults(tFetchResultsReq);
    }

    public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        THandleIdentifier operationId = tCancelOperationReq.getOperationHandle().getOperationId();
        THandleIdentifier sessionId = operationToSessionMap.get(operationId);
        logger.info("Canceling operation, operationGuid: {}, sessionGuid: {}", ThriftUtils.safeHandleId(operationId), ThriftUtils.safeHandleId(sessionId));

        if (operationMap.remove(operationId) != null) {
            operationToSessionMap.remove(operationId);
            MetricsUtils.getCompositeMeterRegistry().counter(Consts.METRICS_OPERATION_CLOSE_COUNT).increment();
            logger.info("Operation canceled (local), operationGuid: {}", ThriftUtils.safeHandleId(operationId));
            return new TCancelOperationResp(new TStatus(TStatusCode.SUCCESS_STATUS));
        }

        TCLIService.Iface client = getClient(sessionId);
        if (client != null) {
            logger.info("Operation canceled (remote), operationGuid: {}", ThriftUtils.safeHandleId(operationId));
            return client.CancelOperation(tCancelOperationReq);
        }
        return new TCancelOperationResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS));
    }

    public TCloseOperationResp CloseOperation(TCloseOperationReq request) throws TException {
        THandleIdentifier id = request.getOperationHandle().getOperationId();
        touchOperation(id);
        THandleIdentifier session = operationToSessionMap.get(id);
        if (operationMap.remove(id) != null) {
            operationToSessionMap.remove(id);
            MetricsUtils.getCompositeMeterRegistry().counter(Consts.METRICS_OPERATION_CLOSE_COUNT).increment();
            return new TCloseOperationResp(new TStatus(TStatusCode.SUCCESS_STATUS));
        }
        TCLIService.Iface client = session == null ? null : getClient(session);
        if (client == null) {
            return new TCloseOperationResp(new TStatus(TStatusCode.INVALID_HANDLE_STATUS));
        }
        TCloseOperationResp response = client.CloseOperation(request);
        if (ThriftUtils.isSuccess(response.getStatus())) {
            operationToSessionMap.remove(id);
            MetricsUtils.getCompositeMeterRegistry().counter(Consts.METRICS_OPERATION_CLOSE_COUNT).increment();
        }
        return response;
    }

    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        THandleIdentifier operationId = tGetQueryIdReq.getOperationHandle().getOperationId();
        touchOperation(operationId);
        SqlOperation operation = operationMap.get(operationId);
        if (operation != null) {
            return new TGetQueryIdResp(operation.getQueryId());
        }

        TCLIService.Iface client = getClientByOperationId(operationId);
        if (client != null) {
            return client.GetQueryId(tGetQueryIdReq);
        }
        return null;
    }
}
