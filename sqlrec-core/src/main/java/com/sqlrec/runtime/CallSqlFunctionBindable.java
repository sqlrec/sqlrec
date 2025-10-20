package com.sqlrec.runtime;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;

import java.util.*;

public class CallSqlFunctionBindable implements BindableInterface {
    private String funName;
    private List<String> inputTables;
    private List<String> tablePlaceholders;
    private SqlFunctionBindable sqlFunctionBindable;
    private boolean isAsync;

    public CallSqlFunctionBindable(String funName, List<String> inputTables, SqlFunctionBindable sqlFunctionBindable, boolean isAsync) {
        this.funName = funName;
        this.inputTables = inputTables;
        this.sqlFunctionBindable = sqlFunctionBindable;
        this.isAsync = isAsync;
        this.tablePlaceholders = new ArrayList<>();
        for (Map.Entry<String, List<RelDataTypeField>> placeholder : sqlFunctionBindable.getInputTables()) {
            tablePlaceholders.add(placeholder.getKey());
        }
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        List<Map.Entry<String, List<RelDataTypeField>>> tablePlaceholders = sqlFunctionBindable.getInputTables();
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

        if (isAsync) {
            SqlFunctionBindable.executorService.submit(() -> sqlFunctionBindable.bind(tmpSchema, context));
            return null;
        } else {
            return sqlFunctionBindable.bind(tmpSchema, context);
        }
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return sqlFunctionBindable.getReturnDataFields();
    }

    @Override
    public boolean isParallelizable() {
        return true;
    }

    @Override
    public Set<String> getReadTables() {
        Set<String> readTables = new HashSet<>(sqlFunctionBindable.getReadTables());
        readTables.addAll(inputTables);
        readTables.removeAll(tablePlaceholders);
        return readTables;
    }

    @Override
    public Set<String> getWriteTables() {
        Set<String> writeTables = new HashSet<>(sqlFunctionBindable.getWriteTables());
        writeTables.removeAll(tablePlaceholders);
        return writeTables;
    }
}
