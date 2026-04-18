package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.Executor;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;

import java.util.*;

public class CallSqlFunctionBindable extends BindableInterface {
    private String funName;
    private List<String> inputTables;
    private List<String> tablePlaceholders;
    private SqlFunctionBindable sqlFunctionBindable;
    private boolean isAsync;

    public CallSqlFunctionBindable(String funName, List<String> inputTables, SqlFunctionBindable sqlFunctionBindable, boolean isAsync) {
        if (sqlFunctionBindable.getInputTables().size() != inputTables.size()) {
            throw new RuntimeException("function input table not match when compile call function: " + funName);
        }

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
        checkInputTable(schema);

        if (!(context instanceof ExecuteContextImpl)) {
            throw new RuntimeException("function call context must be ExecuteContextImpl");
        }
        ExecuteContextImpl finalContext = ((ExecuteContextImpl) context).clone();
        finalContext.addFunNameToStack(funName);

        List<Map.Entry<String, List<RelDataTypeField>>> tablePlaceholders = sqlFunctionBindable.getInputTables();
        CalciteSchema tmpSchema = HmsSchema.getHmsCalciteSchema();
        for (int i = 0; i < tablePlaceholders.size(); i++) {
            CalciteSchema.TableEntry inputTableEntry = schema.getTable(inputTables.get(i), false);
            tmpSchema.add(tablePlaceholders.get(i).getKey(), inputTableEntry.getTable());
        }

        if (isAsync) {
            Executor.getExecutorService().submit(() -> sqlFunctionBindable.bind(tmpSchema, finalContext));
            return null;
        } else {
            return sqlFunctionBindable.bind(tmpSchema, finalContext);
        }
    }

    public void checkInputTable(CalciteSchema schema) {
        List<Map.Entry<String, List<RelDataTypeField>>> tablePlaceholders = sqlFunctionBindable.getInputTables();
        for (int i = 0; i < tablePlaceholders.size(); i++) {
            String inputTable = inputTables.get(i);
            CalciteSchema.TableEntry inputTableEntry = schema.getTable(inputTable, false);
            if (inputTableEntry == null) {
                throw new RuntimeException("function input table not found: " + inputTable);
            }
            Table inputTableObj = inputTableEntry.getTable();

            // only support cache table can be input table now
            if (inputTableObj instanceof CacheTable) {
                CacheTable cacheTable = (CacheTable) inputTableObj;
                List<RelDataTypeField> desiredFields = tablePlaceholders.get(i).getValue();
                List<RelDataTypeField> givenFields = cacheTable.getDataFields();
                DataTypeUtils.checkTableSchemaCompatible(desiredFields, givenFields);
            } else {
                throw new RuntimeException("only support cache table as function input table now, " +
                        "but got: " + inputTableObj.getClass().getName() + " for table: " + inputTable);
            }
        }
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return sqlFunctionBindable.getReturnDataFields();
    }

    @Override
    public boolean isParallelizable() {
        return sqlFunctionBindable.isParallelizable();
    }

    @Override
    public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
        return sqlFunctionBindable.isTimeoutAble(schema, context);
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

    public String getDependencySqlFuncName() {
        return sqlFunctionBindable.getFunName();
    }

    public Map<String, String> getAllDependSqlFunctionMap() {
        return sqlFunctionBindable.getAllDependSqlFunctionMap();
    }
}
