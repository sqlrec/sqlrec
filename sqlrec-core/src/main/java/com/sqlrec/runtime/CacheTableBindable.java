package com.sqlrec.runtime;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;

public class CacheTableBindable implements BindableInterface {
    private String tableName;
    private BindableInterface bindable;
    private String createSql;

    public CacheTableBindable(String tableName, BindableInterface bindable, String createSql) {
        this.tableName = tableName;
        this.bindable = bindable;
        this.createSql = createSql;

        List<RelDataTypeField> bindableFields = bindable.getReturnDataFields();
        if (bindableFields == null || bindableFields.isEmpty()) {
            throw new RuntimeException("bindable return data fields is null or empty");
        }
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        Enumerable<Object[]> enumerable = bindable.bind(schema, context);
        if (enumerable == null) {
            enumerable = Linq4j.emptyEnumerable();
        }

        CacheTable cacheTable = new CacheTable(tableName, enumerable, bindable.getReturnDataFields());
        cacheTable.setCreateSql(createSql);
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
    public boolean isParallelizable() {
        return true;
    }

    @Override
    public Set<String> getReadTables() {
        return bindable.getReadTables();
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
}
