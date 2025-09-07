package com.sqlrec.runtime;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;

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
        for (int i = 0; i < tablePlaceholders.size(); i++) {
            String inputTable = inputTables.get(i);
            CalciteSchema.TableEntry inputTableEntry = schema.getTable(inputTable, false);
            if (inputTableEntry == null) {
                throw new RuntimeException("function input table not found: " + inputTable);
            }
            Table inputTableObj = inputTableEntry.getTable();

            if (inputTableObj instanceof CacheTable) {
                CacheTable cacheTable = (CacheTable) inputTableObj;
                List<RelDataTypeField> desiredFields = tablePlaceholders.get(i).getValue();
                List<RelDataTypeField> givenFields = cacheTable.getDataFields();
                DataTypeUtils.checkTableSchemaCompatible(desiredFields, givenFields);
            }

            String placeholderTableName = tablePlaceholders.get(i).getKey();
            tmpSchema.add(placeholderTableName, inputTableObj);
        }
        return functionBindable.bind(tmpSchema);
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return functionBindable.getReturnDataFields();
    }
}
