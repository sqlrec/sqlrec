package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Executes a RETURN statement and records its result in the current function frame. */
public class ReturnBindable extends ForwardingBindable {
    private final String tableName;
    private final BindableInterface delegate;
    private final List<RelDataTypeField> returnDataFields;

    public ReturnBindable(String tableName, List<RelDataTypeField> returnDataFields) {
        this.tableName = tableName;
        this.delegate = null;
        this.returnDataFields = returnDataFields;
    }

    public ReturnBindable(BindableInterface delegate) {
        this.tableName = null;
        this.delegate = delegate;
        this.returnDataFields = delegate == null ? null : delegate.getReturnDataFields();
    }

    @Override
    protected BindableInterface metadataDelegate() {
        return delegate;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        Enumerable<Object[]> result = null;
        if (tableName != null) {
            CacheTable cacheTable = SchemaUtils.getCacheTable(tableName, schema);
            result = cacheTable.scan(null);
        } else if (delegate != null) {
            result = delegate.bind(schema, context);
        }

        if (!(context instanceof ExecuteContextImpl)) {
            throw new RuntimeException("return statement context must be ExecuteContextImpl");
        }
        ((ExecuteContextImpl) context).returnFromFunction(result);
        return result;
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return returnDataFields;
    }

    @Override
    public boolean isParallelizable() {
        return delegate == null || delegate.isParallelizable();
    }

    @Override
    public boolean containsReturn() {
        return true;
    }

    @Override
    public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
        return delegate != null && delegate.isTimeoutAble(schema, context);
    }

    @Override
    public Set<String> getReadTables() {
        if (tableName != null) {
            return Set.of(tableName);
        }
        return delegate == null ? new HashSet<>() : delegate.getReadTables();
    }
}
