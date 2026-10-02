package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.flink.sql.parser.ddl.SqlReset;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);

    private final Map<THandleIdentifier, Session> sessions = new ConcurrentHashMap<>();
    private final Map<THandleIdentifier, OperationEntry> operations = new ConcurrentHashMap<>();
    private final SessionTimeoutChecker timeoutChecker = new SessionTimeoutChecker(this::expireSessions);

    /** The monitor protects execution/registration against session close. */
    private static final class Session {
        private final THandleIdentifier id;
        private final SqlExecutor executor;
        private final GatewayClient gateway;
        private volatile long lastAccessTime = System.currentTimeMillis();

        private Session(THandleIdentifier id, SqlExecutor executor, GatewayClient gateway) {
            this.id = id;
            this.executor = executor;
            this.gateway = gateway;
        }

        private void touch() {
            lastAccessTime = System.currentTimeMillis();
        }

        private void updateGatewayState() {
            gateway.setSessionState(executor.getDefaultSchema(), executor.getSessionSettings());
        }
    }

    /** A registered entry is always present; only remote entries have no local result. */
    private record OperationEntry(Session session, SqlOperation local) {
        private static OperationEntry local(Session session, SqlOperation result) {
            return new OperationEntry(session, result);
        }

        private static OperationEntry remote(Session session) {
            return new OperationEntry(session, null);
        }

        private boolean isLocal() {
            return local != null;
        }
    }

    public SessionManager() {
        MetricsUtils.getCompositeMeterRegistry().gauge(Consts.METRICS_SESSION_ACTIVE_COUNT, sessions, Map::size);
        MetricsUtils.getCompositeMeterRegistry().gauge(Consts.METRICS_OPERATION_ACTIVE_COUNT, operations, Map::size);
    }

    public void startTimeoutChecker() {
        timeoutChecker.start();
    }

    public void stopTimeoutChecker() {
        timeoutChecker.stop();
    }

    private void expireSessions(long timeout) {
        for (Session session : sessions.values()) {
            synchronized (session) {
                // Recheck after acquiring the lock: an in-flight request may have just finished.
                if (sessions.get(session.id) == session && System.currentTimeMillis() - session.lastAccessTime > timeout) {
                    try {
                        closeSessionLocked(session);
                    } catch (TException e) {
                        logger.warn("Failed to close expired session, sessionGuid: {}", ThriftUtils.safeHandleId(session.id), e);
                    }
                }
            }
        }
    }

    public TOpenSessionResp openSession(TOpenSessionReq request) throws TException {
        // Initialize metadata before publishing the session.
        SqlExecutor executor = new SqlExecutor();
        THandleIdentifier id = ThriftUtils.getHandleIdentifier();
        Session session = new Session(id, executor, new GatewayClient(request));
        sessions.put(id, session);
        MetricsUtils.getCompositeMeterRegistry().counter(Consts.METRICS_SESSION_OPEN_COUNT).increment();
        logger.info("Local session opened, sessionGuid: {}", ThriftUtils.safeHandleId(id));

        TOpenSessionResp response = new TOpenSessionResp(successStatus(), TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);
        response.setSessionHandle(new TSessionHandle(id.deepCopy()));
        response.setConfiguration(Map.of());
        return response;
    }

    public TCloseSessionResp closeSession(TCloseSessionReq request) throws TException {
        Session session = findSession(request.getSessionHandle().getSessionId());
        if (session == null) {
            return new TCloseSessionResp(invalidHandleStatus());
        }
        synchronized (session) {
            return closeSessionLocked(session);
        }
    }

    private TCloseSessionResp closeSessionLocked(Session session) throws TException {
        if (!sessions.remove(session.id, session)) {
            return new TCloseSessionResp(invalidHandleStatus());
        }
        operations.entrySet().removeIf(entry -> entry.getValue().session() == session);
        try {
            return session.gateway.closeSession();
        } finally {
            MetricsUtils.getCompositeMeterRegistry().counter(Consts.METRICS_SESSION_CLOSE_COUNT).increment();
            logger.info("Session closed, sessionGuid: {}", ThriftUtils.safeHandleId(session.id));
        }
    }

    private Session findSession(THandleIdentifier id) {
        Session session = id == null ? null : sessions.get(id);
        if (session != null) {
            session.touch();
        }
        return session;
    }

    public boolean hasSession(TSessionHandle handle) {
        return handle != null && findSession(handle.getSessionId()) != null;
    }

    public GatewayClient getGateway(THandleIdentifier id) {
        Session session = findSession(id);
        return session == null ? null : session.gateway;
    }

    private Session requireSession(TSessionHandle handle) throws TException {
        Session session = handle == null ? null : findSession(handle.getSessionId());
        if (session == null) {
            throw new TException("Session not found");
        }
        return session;
    }

    private void requireActiveSession(Session session) throws TException {
        if (sessions.get(session.id) != session) {
            throw new TException("Session was closed");
        }
    }

    private OperationEntry findOperation(THandleIdentifier id) {
        OperationEntry entry = id == null ? null : operations.get(id);
        if (entry == null || sessions.get(entry.session().id) != entry.session()) {
            return null;
        }
        entry.session().touch();
        return entry;
    }

    private OperationEntry requireOperation(THandleIdentifier id) throws TException {
        OperationEntry entry = findOperation(id);
        if (entry == null) {
            throw new TException("Operation not found: " + ThriftUtils.safeHandleId(id));
        }
        return entry;
    }

    private TOperationHandle registerLocalOperation(Session session, TOperationType type, SqlOperation result) {
        TOperationHandle handle = new TOperationHandle(ThriftUtils.getHandleIdentifier(), type, true);
        registerOperation(handle, OperationEntry.local(session, result));
        return handle;
    }

    private void registerOperation(TOperationHandle handle, OperationEntry entry) {
        operations.put(handle.getOperationId().deepCopy(), entry);
        MetricsUtils.getCompositeMeterRegistry().counter(Consts.METRICS_OPERATION_OPEN_COUNT).increment();
    }

    private void removeOperation(THandleIdentifier id, OperationEntry entry) {
        if (operations.remove(id, entry)) {
            MetricsUtils.getCompositeMeterRegistry().counter(Consts.METRICS_OPERATION_CLOSE_COUNT).increment();
        }
    }

    public TExecuteStatementResp executeStatement(TExecuteStatementReq request) throws TException {
        Session session = requireSession(request.getSessionHandle());
        synchronized (session) {
            try {
                requireActiveSession(session);
                SqlOperation local = executeLocally(session.executor, request.getStatement());
                if (local != null) {
                    if (local.getException() == null) {
                        session.updateGatewayState();
                    }
                    return localExecuteResponse(session, local);
                }
                return executeRemotely(session, request);
            } catch (TException e) {
                return new TExecuteStatementResp(ThriftUtils.errorStatus(e));
            } finally {
                session.touch();
            }
        }
    }

    private SqlOperation executeLocally(SqlExecutor executor, String sql) {
        try {
            SqlProcessResult result = executor.executeSqlAsync(sql);
            return result == null ? null : new SqlOperation(result, ThriftUtils.getQueryId());
        } catch (Exception e) {
            logger.error("Failed to execute local SQL", e);
            SqlOperation operation = new SqlOperation(new SqlProcessResult(), ThriftUtils.getQueryId());
            operation.fail(e);
            return operation;
        }
    }

    private TExecuteStatementResp localExecuteResponse(Session session, SqlOperation local) {
        TExecuteStatementResp response = new TExecuteStatementResp(successStatus());
        response.setOperationHandle(registerLocalOperation(session, TOperationType.EXECUTE_STATEMENT, local));
        return response;
    }

    private TExecuteStatementResp executeRemotely(Session session, TExecuteStatementReq request) throws TException {
        try {
            if (CompileManager.parseSql(request.getStatement()) instanceof SqlReset reset) {
                // RESET bypasses pending SET overrides and commits local state only after remote success.
                session.gateway.executeSessionCommand(request.getStatement());
                session.executor.resetSessionSettings(reset.getKeyString());
                session.updateGatewayState();
                return localExecuteResponse(session, new SqlOperation(
                        SqlProcessResult.msg("Flink settings reset", "msg"), ThriftUtils.getQueryId()));
            }
        } catch (TException e) {
            throw e;
        } catch (Exception e) {
            throw new TException(e);
        }
        TExecuteStatementResp response = session.gateway.executeStatement(request);
        if (ThriftUtils.isSuccess(response.getStatus())) {
            if (response.getOperationHandle() == null) {
                throw new TException("Gateway ExecuteStatement returned no operation handle");
            }
            registerOperation(response.getOperationHandle(), OperationEntry.remote(session));
        }
        return response;
    }

    public TOperationHandle openMetadataOperation(TSessionHandle handle, TOperationType type,
            Callable<SqlProcessResult> query) throws Exception {
        Session session = requireSession(handle);
        synchronized (session) {
            try {
                requireActiveSession(session);
                SqlOperation result = SqlOperation.metadata(query.call(), ThriftUtils.getQueryId());
                return registerLocalOperation(session, type, result);
            } finally {
                session.touch();
            }
        }
    }

    public TGetCrossReferenceResp getCrossReference(TGetCrossReferenceReq request) throws TException {
        Session session = requireSession(request.getSessionHandle());
        synchronized (session) {
            try {
                requireActiveSession(session);
                TGetCrossReferenceResp response = session.gateway.getCrossReference(request);
                if (ThriftUtils.isSuccess(response.getStatus()) && response.getOperationHandle() != null) {
                    registerOperation(response.getOperationHandle(), OperationEntry.remote(session));
                }
                return response;
            } finally {
                session.touch();
            }
        }
    }

    public TGetOperationStatusResp getOperationStatus(TGetOperationStatusReq request) throws TException {
        OperationEntry entry = requireOperation(request.getOperationHandle().getOperationId());
        if (!entry.isLocal()) {
            return entry.session().gateway.getOperationStatus(request);
        }
        TGetOperationStatusResp response = new TGetOperationStatusResp(successStatus());
        response.setOperationState(entry.local().getState());
        response.setHasResultSet(true);
        response.setErrorMessage(entry.local().getMsg());
        return response;
    }

    public TGetResultSetMetadataResp getResultSetMetadata(TGetResultSetMetadataReq request) throws TException {
        OperationEntry entry = requireOperation(request.getOperationHandle().getOperationId());
        if (!entry.isLocal()) {
            return entry.session().gateway.getResultSetMetadata(request);
        }
        TGetResultSetMetadataResp response = new TGetResultSetMetadataResp(successStatus());
        response.setSchema(ThriftUtils.convertFieldsToTTableSchema(entry.local().getFields()));
        return response;
    }

    public TFetchResultsResp fetchResults(TFetchResultsReq request) throws TException {
        OperationEntry entry = requireOperation(request.getOperationHandle().getOperationId());
        if (!entry.isLocal()) {
            return entry.session().gateway.fetchResults(request);
        }
        TFetchResultsResp response = new TFetchResultsResp(successStatus());
        if (request.getFetchType() != 0) {
            response.setResults(ThriftUtils.convertObjectArrayToTRowSet(null, DataTypeUtils.getStringTypeField("log")));
            response.setHasMoreRows(false);
            return response;
        }
        try {
            SqlOperation.ResultPage page = entry.local().fetch(request.getOrientation(), request.getMaxRows());
            response.setResults(page.rows());
            response.setHasMoreRows(page.hasMoreRows());
        } catch (Exception e) {
            response.setStatus(ThriftUtils.errorStatus(e));
        }
        return response;
    }

    public TCancelOperationResp cancelOperation(TCancelOperationReq request) throws TException {
        THandleIdentifier id = request.getOperationHandle().getOperationId();
        OperationEntry entry = findOperation(id);
        if (entry == null) {
            return new TCancelOperationResp(invalidHandleStatus());
        }
        if (!entry.isLocal()) {
            return entry.session().gateway.cancelOperation(request);
        }
        removeOperation(id, entry);
        return new TCancelOperationResp(successStatus());
    }

    public TCloseOperationResp closeOperation(TCloseOperationReq request) throws TException {
        THandleIdentifier id = request.getOperationHandle().getOperationId();
        OperationEntry entry = findOperation(id);
        if (entry == null) {
            return new TCloseOperationResp(invalidHandleStatus());
        }
        TCloseOperationResp response = entry.isLocal() ? new TCloseOperationResp(successStatus())
                : entry.session().gateway.closeOperation(request);
        if (ThriftUtils.isSuccess(response.getStatus())) {
            removeOperation(id, entry);
        }
        return response;
    }

    public TGetQueryIdResp getQueryId(TGetQueryIdReq request) throws TException {
        OperationEntry entry = requireOperation(request.getOperationHandle().getOperationId());
        return entry.isLocal() ? new TGetQueryIdResp(entry.local().getQueryId())
                : entry.session().gateway.getQueryId(request);
    }

    private static TStatus successStatus() {
        return new TStatus(TStatusCode.SUCCESS_STATUS);
    }

    private static TStatus invalidHandleStatus() {
        return new TStatus(TStatusCode.INVALID_HANDLE_STATUS);
    }
}
