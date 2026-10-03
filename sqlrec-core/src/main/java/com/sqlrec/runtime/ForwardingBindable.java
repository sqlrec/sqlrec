package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import org.apache.calcite.jdbc.CalciteSchema;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Shares dependency and plan metadata forwarding across bindable wrappers.
 *
 * <p>A null delegate retains the normal metadata defaults for dynamic calls and table returns.
 * Execution, identity, result schemas and RETURN behavior belong to each wrapper. Wrappers
 * override capabilities and table access when they introduce their own semantics.
 */
public abstract class ForwardingBindable extends BindableInterface {
    protected abstract BindableInterface metadataDelegate();

    @Override
    public boolean isParallelizable() {
        BindableInterface delegate = metadataDelegate();
        return delegate != null && delegate.isParallelizable();
    }

    @Override
    public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
        BindableInterface delegate = metadataDelegate();
        return delegate == null ? super.isTimeoutAble(schema, context) : delegate.isTimeoutAble(schema, context);
    }

    @Override
    public Set<String> getReadTables() {
        BindableInterface delegate = metadataDelegate();
        return delegate == null ? new HashSet<>() : delegate.getReadTables();
    }

    @Override
    public Set<String> getWriteTables() {
        BindableInterface delegate = metadataDelegate();
        return delegate == null ? new HashSet<>() : delegate.getWriteTables();
    }

    @Override
    public Set<String> getDependencyJavaFuncName() {
        BindableInterface delegate = metadataDelegate();
        return delegate == null ? super.getDependencyJavaFuncName() : delegate.getDependencyJavaFuncName();
    }

    @Override
    public Set<String> getDependencySqlFuncName() {
        BindableInterface delegate = metadataDelegate();
        return delegate == null ? super.getDependencySqlFuncName() : delegate.getDependencySqlFuncName();
    }

    @Override
    public Map<String, String> getAllDependSqlFunctionMap() {
        BindableInterface delegate = metadataDelegate();
        return delegate == null ? super.getAllDependSqlFunctionMap() : delegate.getAllDependSqlFunctionMap();
    }

    @Override
    public boolean isUnionSql() {
        BindableInterface delegate = metadataDelegate();
        return delegate != null && delegate.isUnionSql();
    }

    @Override
    public String getLogicalPlan() {
        BindableInterface delegate = metadataDelegate();
        return delegate == null ? super.getLogicalPlan() : delegate.getLogicalPlan();
    }

    @Override
    public String getPhysicalPlan() {
        BindableInterface delegate = metadataDelegate();
        return delegate == null ? super.getPhysicalPlan() : delegate.getPhysicalPlan();
    }

    @Override
    public String getJavaExpression() {
        BindableInterface delegate = metadataDelegate();
        return delegate == null ? super.getJavaExpression() : delegate.getJavaExpression();
    }
}
