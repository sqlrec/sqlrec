package com.sqlrec.runtime;

import com.sqlrec.schema.CacheTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;

import java.util.List;
import java.util.Map;

public class FunctionBindable implements BindableInterface {
    private List<Map.Entry<String, List<RelDataTypeField>>> inputTables;
    private List<BindableInterface> bindableList;
    private String returnTableName;
    private List<RelDataTypeField> returnDataFields;

    public FunctionBindable(List<Map.Entry<String, List<RelDataTypeField>>> inputTables, List<BindableInterface> bindableList, String returnTableName, List<RelDataTypeField> returnDataFields) {
        this.inputTables = inputTables;
        this.bindableList = bindableList;
        this.returnTableName = returnTableName;
        this.returnDataFields = returnDataFields;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema) {
        for (BindableInterface bindable : bindableList) {
            bindable.bind(schema);
        }
        Table table = schema.getTable(returnTableName, false).getTable();
        if (table instanceof CacheTable) {
            return ((CacheTable) table).scan(null);
        } else {
            throw new RuntimeException("function not return cache table");
        }
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return returnDataFields;
    }

    public List<Map.Entry<String, List<RelDataTypeField>>> getInputTables() {
        return inputTables;
    }
}
