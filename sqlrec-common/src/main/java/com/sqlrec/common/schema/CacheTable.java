package com.sqlrec.common.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class CacheTable extends SqlRecTable implements ScannableTable {
    private String tableName;
    private Enumerable<Object[]> enumerable;
    private List<RelDataTypeField> dataFields;
    private String createSql;

    public CacheTable(String tableName, Enumerable<Object[]> enumerable, List<RelDataTypeField> dataFields) {
        this.tableName = tableName;
        this.enumerable = enumerable;
        this.dataFields = dataFields;
        this.createSql = "";
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(dataFields);
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
        return enumerable;
    }

    public List<RelDataTypeField> getDataFields() {
        return dataFields;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCreateSql() {
        return createSql;
    }

    public void setCreateSql(String createSql) {
        this.createSql = createSql;
    }
}
