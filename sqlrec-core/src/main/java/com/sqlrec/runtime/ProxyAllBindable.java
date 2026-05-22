package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.utils.SchemaUtils;
import io.micrometer.core.instrument.Tags;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
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

        try {
            Enumerable<Object[]> result = delegate.bind(schema, context);
            if (result != null) {
                String cacheTableName = delegate.getCacheTableName();
                if (StringUtils.isNotEmpty(cacheTableName)) {
                    CacheTable cacheTable = SchemaUtils.tryGetCacheTable(cacheTableName, schema);
                    if (cacheTable != null) {
                        count = cacheTable.scan(null).count();
                    }
                } else {
                    count = result.count();
                }
            }
            return result;
        } catch (Throwable e) {
            log.error("exec node {} error", getName(), e);
            status = "error";
            throw e;
        } finally {
            Tags tags = MetricsUtils.createTags(context.getMetricsTags(), "name", getName(), "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_NODE_EXEC_DURATION, tags)
                    .record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            MetricsUtils.getCompositeMeterRegistry()
                    .summary(Consts.METRICS_NODE_DATA_SIZE, tags)
                    .record(count);
        }
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
