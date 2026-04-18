package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.*;

public abstract class BindableInterface {
    private final long createTime = System.currentTimeMillis();
    private boolean ignoreException = false;

    public BindableInterface() {
    }

    public abstract Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context);

    public abstract List<RelDataTypeField> getReturnDataFields();

    public abstract boolean isParallelizable();

    public abstract Set<String> getReadTables();

    public abstract Set<String> getWriteTables();

    public long getCreateTime() {
        return createTime;
    }

    public Set<String> getAccessTables() {
        Set<String> accessTables = new HashSet<>(getReadTables());
        accessTables.addAll(getWriteTables());
        return accessTables;
    }

    public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
        return true;
    }

    public boolean isUnionSql() {
        return false;
    }

    public void setIgnoreException(boolean ignoreException) {
        this.ignoreException = ignoreException;
    }

    public boolean isIgnoreException() {
        return ignoreException;
    }

    public String getDependencyJavaFuncName() {
        return null;
    }

    public String getDependencySqlFuncName() {
        return null;
    }

    public Map<String, String> getAllDependSqlFunctionMap() {
        return new HashMap<>();
    }

    public String getCacheTableName() {
        return null;
    }

    public List<RelDataTypeField> getCacheTableDataFields() {
        return null;
    }
}
