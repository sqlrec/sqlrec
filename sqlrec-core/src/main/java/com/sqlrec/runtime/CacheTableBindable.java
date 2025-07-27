package com.sqlrec.runtime;

import com.sqlrec.schema.CacheTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

public class CacheTableBindable implements BindableInterface {
    private String tableName;
    private BindableInterface bindable;

    public CacheTableBindable(String tableName, BindableInterface bindable) {
        this.tableName = tableName;
        this.bindable = bindable;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema) {
        Enumerable<Object[]> enumerable = bindable.bind(schema);
        schema.add(tableName, new CacheTable(tableName, enumerable, bindable.getReturnDataFields()));
        return null;
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return bindable.getReturnDataFields();
    }

    public String getTableName() {
        return tableName;
    }
}
