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

public class CacheTableBindable extends BindableInterface {
    private String tableName;
    private BindableInterface bindable;

    public CacheTableBindable(String tableName, BindableInterface bindable) {
        this.tableName = tableName;
        this.bindable = bindable;

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
    public boolean isParallelizable() {
        return bindable.isParallelizable();
    }

    @Override
    public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
        return bindable.isTimeoutAble(schema, context);
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

    public BindableInterface getBindable() {
        return bindable;
    }

    public boolean isUnionSql() {
        return bindable.isUnionSql();
    }

    @Override
    public String getCacheTableName() {
        return tableName;
    }

    @Override
    public Set<String> getDependencySqlFuncName() {
        return bindable.getDependencySqlFuncName();
    }

    @Override
    public Set<String> getDependencyJavaFuncName() {
        return bindable.getDependencyJavaFuncName();
    }

    @Override
    public Map<String, String> getAllDependSqlFunctionMap() {
        return bindable.getAllDependSqlFunctionMap();
    }

    public List<RelDataTypeField> getCacheTableDataFields() {
        return bindable.getReturnDataFields();
    }

    @Override
    public String getLogicalPlan() {
        return bindable.getLogicalPlan();
    }

    @Override
    public String getPhysicalPlan() {
        return bindable.getPhysicalPlan();
    }

    @Override
    public String getJavaExpression() {
        return bindable.getJavaExpression();
    }
}
