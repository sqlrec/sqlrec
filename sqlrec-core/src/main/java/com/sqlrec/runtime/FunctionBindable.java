package com.sqlrec.runtime;

import com.sqlrec.common.schema.CacheTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

public class FunctionBindable implements BindableInterface {
    private String funName;
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

        if (returnTableName == null) {
            return null;
        }

        CalciteSchema.TableEntry tableEntry = schema.getTable(returnTableName, false);
        if (tableEntry == null) {
            throw new RuntimeException("function return table not exist");
        }
        Table table = tableEntry.getTable();
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

    public void addInputTable(String tableName, List<RelDataTypeField> dataFields) {
        inputTables.add(new AbstractMap.SimpleEntry<>(tableName, dataFields));
    }

    public void setInputTables(List<Map.Entry<String, List<RelDataTypeField>>> inputTables) {
        this.inputTables = inputTables;
    }

    public List<BindableInterface> getBindableList() {
        return bindableList;
    }

    public void setBindableList(List<BindableInterface> bindableList) {
        this.bindableList = bindableList;
    }

    public String getReturnTableName() {
        return returnTableName;
    }

    public void setReturnTableName(String returnTableName) {
        this.returnTableName = returnTableName;
    }

    public void setReturnDataFields(List<RelDataTypeField> returnDataFields) {
        this.returnDataFields = returnDataFields;
    }

    public String getFunName() {
        return funName;
    }

    public void setFunName(String funName) {
        this.funName = funName;
    }
}
