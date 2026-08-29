package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.utils.ExecutorServiceUtils;
import com.sqlrec.utils.SchemaUtils;
import com.sqlrec.utils.TraceUtils;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.api.trace.Span;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ProxyAllBindable extends BindableInterface {
    private static final Logger log = LoggerFactory.getLogger(ProxyAllBindable.class);

    private final BindableInterface delegate;

    public ProxyAllBindable(BindableInterface delegate) {
        this.delegate = delegate;
    }

    public static BindableInterface wrap(BindableInterface bindable) {
        if (bindable instanceof ProxyAllBindable) {
            return bindable;
        }
        return new ProxyAllBindable(bindable);
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        if (context.isCancelled()) {
            recordCancelled(context);
            throw new RuntimeException("node " + getName() + " execution cancelled before start");
        }

        long startTime = System.currentTimeMillis();
        long count = 0;
        String status = "success";
        boolean debugPrint = isDebugPrintEnabled(context);
        String logId = context.getLogId();
        String nodeName = getName();

        ExecuteContextImpl traceContext = ((ExecuteContextImpl) context).clone();
        Span span = TraceUtils.startSpan(traceContext, nodeName);

        if (debugPrint) {
            log.info("[{}] node [{}] start execution", logId, nodeName);
        }

        Throwable error = null;
        try {
            Enumerable<Object[]> result = executeWithRecovery(schema, context, traceContext);
            if (context.isCancelled()) {
                throw new RuntimeException("node " + nodeName + " execution cancelled");
            }
            count = printAndCountResult(schema, context, debugPrint, result);

            if (debugPrint) {
                log.info("[{}] node [{}] execution complete, cost {} ms", logId, nodeName,
                        System.currentTimeMillis() - startTime);
            }
            return result;
        } catch (Throwable failure) {
            error = failure;
            if (failure instanceof Error) {
                status = "error";
                throw (Error) failure;
            }
            if (context.isCancelled()) {
                status = "cancelled";
                recordCancelled(context);
                throw propagate(failure);
            }
            boolean timeout = failure instanceof TimeoutException;
            status = timeout ? "timeout" : "error";
            String message = timeout
                    ? "Node " + nodeName + " execution timeout"
                    : "Node " + nodeName + " execution failed";
            throw new RuntimeException(message, failure);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            TraceUtils.endSpan(span, logId, duration, count, status, error);
            Tags tags = MetricsUtils.createTags(context.getMetricsTags(), "name", getName(), "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_NODE_EXEC_DURATION, tags)
                    .record(duration, TimeUnit.MILLISECONDS);
            MetricsUtils.getCompositeMeterRegistry()
                    .summary(Consts.METRICS_NODE_DATA_SIZE, tags)
                    .record(count);
        }
    }

    private Enumerable<Object[]> executeWithRecovery(
            CalciteSchema schema,
            ExecuteContext context,
            ExecuteContextImpl nodeContext
    ) throws Throwable {
        try {
            return executeDelegate(schema, nodeContext);
        } catch (Throwable failure) {
            Throwable cause = unwrapExecutionFailure(failure);
            if (!(cause instanceof InterruptedException) && !(cause instanceof Error)) {
                Enumerable<Object[]> recovered = recoverIgnoredCacheFailure(schema, context, cause);
                if (recovered != null) {
                    return recovered;
                }
            }
            throw cause;
        }
    }

    private Enumerable<Object[]> executeDelegate(CalciteSchema schema, ExecuteContextImpl nodeContext)
            throws Throwable {
        long timeout = SqlRecConfigs.NODE_EXEC_TIMEOUT.getValue(nodeContext.getVariables());
        if (timeout <= 0 || !delegate.isTimeoutAble(schema, nodeContext)) {
            return delegate.bind(schema, nodeContext);
        }

        CompletableFuture<Enumerable<Object[]>> future = CompletableFuture.supplyAsync(
                () -> delegate.bind(schema, nodeContext),
                ExecutorServiceUtils.getExecutorService()
        );
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Throwable failure) {
            nodeContext.cancel();
            future.cancel(true);
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (failure instanceof TimeoutException) {
                TimeoutException timeoutException = new TimeoutException(
                        "Task execution timeout after " + timeout + "ms"
                );
                timeoutException.initCause(failure);
                throw timeoutException;
            }
            throw failure;
        }
    }

    private Throwable unwrapExecutionFailure(Throwable failure) {
        if (failure instanceof ExecutionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private RuntimeException propagate(Throwable failure) {
        if (failure instanceof RuntimeException) {
            return (RuntimeException) failure;
        }
        return new RuntimeException(failure.getMessage(), failure);
    }

    private Enumerable<Object[]> recoverIgnoredCacheFailure(
            CalciteSchema schema,
            ExecuteContext context,
            Throwable failure
    ) {
        if (!delegate.isIgnoreException() || context.isCancelled()) {
            return null;
        }

        String cacheTableName = delegate.getCacheTableName();
        List<RelDataTypeField> cacheTableDataFields = delegate.getCacheTableDataFields();
        if (StringUtils.isEmpty(cacheTableName)
                || cacheTableDataFields == null) {
            return null;
        }

        log.warn("ignore exception when bind cache table {}: {}", cacheTableName, failure.getMessage(), failure);
        Tags tags = MetricsUtils.createTags(context.getMetricsTags(), "name", getName());
        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_CACHE_TABLE_IGNORE_EXCEPTION, tags)
                .increment();

        CacheTable cacheTable = new CacheTable(cacheTableName, Linq4j.emptyEnumerable(), cacheTableDataFields);
        cacheTable.setCreateSql(delegate.getSql());
        if (context.isCancelled()) {
            throw new RuntimeException("node " + getName() + " execution cancelled");
        }
        schema.add(cacheTableName, cacheTable);

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{cacheTableName, 0L});
        return Linq4j.asEnumerable(rows);
    }

    private void recordCancelled(ExecuteContext context) {
        Tags tags = MetricsUtils.createTags(context.getMetricsTags(), "name", getName());
        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_NODE_CANCELLED, tags)
                .increment();
    }

    private boolean isDebugPrintEnabled(ExecuteContext context) {
        Map<String, String> vars = context.getVariables();
        if (vars != null && vars.containsKey(SqlRecConfigs.DEBUG_PRINT.getKey())) {
            return SqlRecConfigs.DEBUG_PRINT.getValue(vars);
        }
        return SqlRecConfigs.DEBUG_PRINT.getValue();
    }

    private void printNodeResult(ExecuteContext context, String nodeName,
                                 Enumerable<Object[]> data, List<RelDataTypeField> fields) {
        String logId = context.getLogId();
        log.info("[{}] node [{}] output:", logId, nodeName);
        List<String> tableLines = DataTransformUtils.formatAsTable(data, fields);
        for (String line : tableLines) {
            log.info("[{}] {}", logId, line);
        }
    }

    private long printAndCountResult(CalciteSchema schema, ExecuteContext context,
                                     boolean debugPrint, Enumerable<Object[]> result) {
        String logId = context.getLogId();
        String nodeName = getName();

        if (result == null) {
            if (debugPrint) {
                log.info("[{}] node [{}] return data is null", logId, nodeName);
            }
            return 0;
        }

        boolean isSelect = delegate instanceof CalciteBindable
                && ((CalciteBindable) delegate).getSqlNode() instanceof SqlSelect;
        if (debugPrint && isSelect) {
            printNodeResult(context, nodeName, result, delegate.getReturnDataFields());
        }

        String cacheTableName = delegate.getCacheTableName();
        if (StringUtils.isNotEmpty(cacheTableName)) {
            CacheTable cacheTable = SchemaUtils.tryGetCacheTable(cacheTableName, schema);
            if (cacheTable == null) {
                return 0;
            }
            Enumerable<Object[]> cacheData = cacheTable.scan(null);
            long count = cacheData.count();
            if (debugPrint) {
                printNodeResult(context, nodeName, cacheData, cacheTable.getDataFields());
            }
            return count;
        }

        long count = result.count();
        if (debugPrint && !isSelect) {
            printNodeResult(context, nodeName, result, delegate.getReturnDataFields());
        }
        return count;
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return delegate.getReturnDataFields();
    }

    @Override
    public boolean isParallelizable() {
        return delegate.isParallelizable();
    }

    @Override
    public Set<String> getReadTables() {
        return delegate.getReadTables();
    }

    @Override
    public Set<String> getWriteTables() {
        return delegate.getWriteTables();
    }

    @Override
    public long getCreateTime() {
        return delegate.getCreateTime();
    }

    @Override
    public Set<String> getAccessTables() {
        return delegate.getAccessTables();
    }

    @Override
    public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
        return delegate.isTimeoutAble(schema, context);
    }

    @Override
    public boolean isUnionSql() {
        return delegate.isUnionSql();
    }

    @Override
    public boolean containsReturn() {
        return delegate.containsReturn();
    }

    @Override
    public void setIgnoreException(boolean ignoreException) {
        delegate.setIgnoreException(ignoreException);
    }

    @Override
    public boolean isIgnoreException() {
        return delegate.isIgnoreException();
    }

    @Override
    public Set<String> getDependencyJavaFuncName() {
        return delegate.getDependencyJavaFuncName();
    }

    @Override
    public Set<String> getDependencySqlFuncName() {
        return delegate.getDependencySqlFuncName();
    }

    @Override
    public Map<String, String> getAllDependSqlFunctionMap() {
        return delegate.getAllDependSqlFunctionMap();
    }

    @Override
    public String getCacheTableName() {
        return delegate.getCacheTableName();
    }

    @Override
    public List<RelDataTypeField> getCacheTableDataFields() {
        return delegate.getCacheTableDataFields();
    }

    public BindableInterface getDelegate() {
        return delegate;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public void setName(String name) {
        delegate.setName(name);
    }

    @Override
    public String getSql() {
        return delegate.getSql();
    }

    @Override
    public void setSql(String sql) {
        delegate.setSql(sql);
    }

    @Override
    public String getLogicalPlan() {
        return delegate.getLogicalPlan();
    }

    @Override
    public String getPhysicalPlan() {
        return delegate.getPhysicalPlan();
    }

    @Override
    public String getJavaExpression() {
        return delegate.getJavaExpression();
    }
}
