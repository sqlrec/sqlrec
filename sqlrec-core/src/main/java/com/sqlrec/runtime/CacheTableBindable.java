package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.utils.NodeUtils;
import com.sqlrec.utils.SchemaUtils;
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
        this.tableName = NodeUtils.normalizeTableName(tableName);
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
        // Finish lazy results inside the execution budget, before publishing any table.
        List<Object[]> rows = enumerable == null ? Collections.emptyList() : enumerable.toList();
        CacheTable cacheTable = new CacheTable(tableName, Linq4j.asEnumerable(rows), bindable.getReturnDataFields());
        cacheTable.setCreateSql(getSql());
        publish(schema, context, cacheTable);

        // return cache table counts
        return Linq4j.singletonEnumerable(new Object[]{tableName, (long) rows.size()});
    }

    void ensureTable(CalciteSchema schema, ExecuteContext context) {
        publish(schema, context, new CacheTable(tableName, Linq4j.emptyEnumerable(), getTableDataFields()), true);
    }

    static void publish(CalciteSchema schema, ExecuteContext context, CacheTable table) {
        publish(schema, context, table, false);
    }

    // All runtime cache writes, including recovery and missing ELSE tables, enter here.
    private static void publish(CalciteSchema schema, ExecuteContext context, CacheTable table, boolean ifAbsent) {
        synchronized (schema) {
            if (context.isCancelled()) {
                throw new RuntimeException("cache table " + table.getTableName() + " execution cancelled");
            }
            if (ifAbsent) {
                CacheTable existing = SchemaUtils.tryGetCacheTable(table.getTableName(), schema);
                if (existing != null) {
                    DataTypeUtils.checkTableSchemaSame(table.getDataFields(), existing.getDataFields());
                    return;
                }
            }
            schema.add(table.getTableName(), table);
        }
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
