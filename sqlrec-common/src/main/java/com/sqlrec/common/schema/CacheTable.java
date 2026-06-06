package com.sqlrec.common.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class CacheTable extends SqlRecTable implements ScannableTable {
    private Enumerable<Object[]> enumerable;
    private List<RelDataTypeField> dataFields;

    public CacheTable(String tableName, Enumerable<Object[]> enumerable, List<RelDataTypeField> dataFields) {
        this.name = tableName;
        this.enumerable = enumerable;
        this.dataFields = dataFields;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(dataFields);
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
        if (enumerable == null) {
            return Linq4j.emptyEnumerable();
        }
        return enumerable;
    }

    public List<RelDataTypeField> getDataFields() {
        return dataFields;
    }
}
