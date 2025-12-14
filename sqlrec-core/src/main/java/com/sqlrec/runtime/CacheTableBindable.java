package com.sqlrec.runtime;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CacheTableBindable extends BindableInterface {
    private static final Logger log = LoggerFactory.getLogger(CacheTableBindable.class);

    private String tableName;
    private BindableInterface bindable;
    private String createSql;
    private boolean ignoreException;

    public CacheTableBindable(String tableName, BindableInterface bindable, String createSql) {
        this.tableName = tableName;
        this.bindable = bindable;
        this.createSql = createSql;
        this.ignoreException = false;

        List<RelDataTypeField> bindableFields = bindable.getReturnDataFields();
        if (bindableFields == null || bindableFields.isEmpty()) {
            throw new RuntimeException("bindable return data fields is null or empty");
        }
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        Enumerable<Object[]> enumerable = null;
        try {
            enumerable = bindable.bind(schema, context);
        } catch (Exception e) {
            if (ignoreException) {
                log.warn("ignore exception when bind cache table {}: {}", tableName, e.getMessage(), e);
            } else {
                throw e;
            }
        }
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

    public BindableInterface getBindable() {
        return bindable;
    }

    public boolean isIgnoreException() {
        return ignoreException;
    }

    public void setIgnoreException(boolean ignoreException) {
        this.ignoreException = ignoreException;
    }
}
