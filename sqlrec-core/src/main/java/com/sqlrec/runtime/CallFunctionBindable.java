package com.sqlrec.runtime;

import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.Map;

public class CallFunctionBindable implements BindableInterface {
    private String funName;
    private List<String> inputTables;
    private FunctionBindable functionBindable;

    public CallFunctionBindable(String funName, List<String> inputTables, FunctionBindable functionBindable) {
        this.funName = funName;
        this.inputTables = inputTables;
        this.functionBindable = functionBindable;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema) {
        List<Map.Entry<String, List<RelDataTypeField>>> tablePlaceholders = functionBindable.getInputTables();
        CalciteSchema tmpSchema = HmsSchema.getHmsCalciteSchema();
        for (Map.Entry<String, List<RelDataTypeField>> entry : tablePlaceholders) {
            String tableName = entry.getKey();
            // todo check table schema
            tmpSchema.add(tableName, schema.getTable(tableName, false).getTable());
        }
        return functionBindable.bind(tmpSchema);
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return functionBindable.getReturnDataFields();
    }
}
