package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;

public class CacheTableBindable extends ForwardingBindable {
    private final String tableName;
    private final BindableInterface bindable;

    public CacheTableBindable(String tableName, BindableInterface bindable) {
        this.tableName = tableName;
        this.bindable = bindable;

        List<RelDataTypeField> bindableFields = bindable.getReturnDataFields();
        if (bindableFields == null || bindableFields.isEmpty()) {
            throw new RuntimeException("bindable return data fields is null or empty");
        }
    }

    @Override
    protected BindableInterface metadataDelegate() {
        return bindable;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        Enumerable<Object[]> enumerable = bindable.bind(schema, context);
        if (enumerable == null) {
            enumerable = Linq4j.emptyEnumerable();
        }

        if (context.isCancelled()) {
            // a cancelled branch no longer writes the cache table, avoiding side effects from zombie branches
            throw new RuntimeException("cache table " + tableName + " execution cancelled");
        }

        CacheTable cacheTable = new CacheTable(tableName, enumerable, bindable.getReturnDataFields());
        cacheTable.setCreateSql(getSql());
        schema.add(tableName, cacheTable);

        // return cache table counts
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[]{tableName, (long) enumerable.count()});
        return Linq4j.asEnumerable(list);
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return Arrays.asList(
                DataTypeUtils.getRelDataTypeField("table_name", 0, SqlTypeName.VARCHAR),
                DataTypeUtils.getRelDataTypeField("count", 1, SqlTypeName.BIGINT)
        );
    }

    @Override
    public Set<String> getWriteTables() {
        Set<String> writeTables = new HashSet<>(bindable.getWriteTables());
        writeTables.add(tableName);
        return writeTables;
    }

    public List<RelDataTypeField> getTableDataFields() {
        return bindable.getReturnDataFields();
    }

    public String getTableName() {
        return tableName;
    }

    public BindableInterface getBindable() {
        return bindable;
    }

    @Override
    public String getCacheTableName() {
        return tableName;
    }

    public List<RelDataTypeField> getCacheTableDataFields() {
        return bindable.getReturnDataFields();
    }
}
