package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class BindableInterface {
    private final long createTime;

    public BindableInterface() {
        this.createTime = System.currentTimeMillis();
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
}
