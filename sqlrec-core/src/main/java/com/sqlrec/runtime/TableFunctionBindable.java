package com.sqlrec.runtime;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.TableFunction;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TableFunctionBindable implements BindableInterface {
    TableFunction tableFunction;
    private String inputTableName;
    private List<RelDataTypeField> returnDataFields;

    public TableFunctionBindable(TableFunction tableFunction, String inputTableName, CalciteSchema schema) {
        this.tableFunction = tableFunction;
        this.inputTableName = inputTableName;
        this.returnDataFields = getTableFunctionOutputField(tableFunction, inputTableName, schema);
    }

    private List<RelDataTypeField> getTableFunctionOutputField(TableFunction tableFunction, String inputTableName, CalciteSchema schema) {
        CacheTable cacheTable = getCacheTable(inputTableName, schema);
        CacheTable tmpTable = new CacheTable(
                cacheTable.getTableName(),
                Linq4j.asEnumerable(new ArrayList<>()),
                cacheTable.getDataFields()
        );
        CacheTable outputTable = tableFunction.eval(tmpTable);
        return outputTable.getDataFields();
    }

    private CacheTable getCacheTable(String inputTableName, CalciteSchema schema) {
        Table table = Objects.
                requireNonNull(schema.getTable(inputTableName, false), "input table not found: " + inputTableName)
                .getTable();
        if (!(table instanceof CacheTable)) {
            throw new RuntimeException("input table must be cache table for table function");
        }
        return (CacheTable) table;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema) {
        CacheTable cacheTable = getCacheTable(inputTableName, schema);
        CacheTable outputTable = tableFunction.eval(cacheTable);
        return outputTable.scan(null);
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return returnDataFields;
    }
}
