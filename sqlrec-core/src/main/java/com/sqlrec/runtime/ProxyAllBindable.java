package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.utils.SchemaUtils;
import com.sqlrec.utils.TraceUtils;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.api.trace.Span;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ProxyAllBindable extends BindableInterface {
    private static final Logger log = LoggerFactory.getLogger(ProxyAllBindable.class);

    private final BindableInterface delegate;

    public ProxyAllBindable(BindableInterface delegate) {
        this.delegate = delegate;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        long startTime = System.currentTimeMillis();
        long count = 0;
        String status = "success";
        boolean debugPrint = isDebugPrintEnabled(context);
        String logId = context.getLogId();
        String nodeName = getName();

        ExecuteContext traceContext = ((ExecuteContextImpl) context).clone();
        Span span = TraceUtils.startSpan(traceContext, nodeName);

        if (debugPrint) {
            log.info("[{}] node [{}] start execution", logId, nodeName);
        }

        Throwable error = null;
        try {
            Enumerable<Object[]> result = delegate.bind(schema, traceContext);
            count = printAndCountResult(schema, context, debugPrint, result);

            if (debugPrint) {
                log.info("[{}] node [{}] execution complete, cost {} ms", logId, nodeName,
                        System.currentTimeMillis() - startTime);
            }
            return result;
        } catch (Throwable e) {
            status = "error";
            error = e;
            throw new RuntimeException("Node " + nodeName + " execution failed", e);
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
        if (isSelect) {
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
    public void setIgnoreException(boolean ignoreException) {
        delegate.setIgnoreException(ignoreException);
    }

    @Override
    public boolean isIgnoreException() {
        return delegate.isIgnoreException();
    }

    @Override
    public String getDependencyJavaFuncName() {
        return delegate.getDependencyJavaFuncName();
    }

    @Override
    public String getDependencySqlFuncName() {
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
