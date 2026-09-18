package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

/** Creates function bindables from parsed SQL. Execution remains in the bindable classes. */
final class FunctionBindableFactory {
    private FunctionBindableFactory() {
    }

    static BindableInterface create(
            SqlCallSqlFunction call,
            CalciteSchema schema,
            CompileManager compileManager
    ) throws Exception {
        List<SqlNode> inputs = call.getInputTableList();
        SqlGetVariable functionVariable = call.getFuncNameVariable();
        List<RelDataTypeField> returnFields = resolveReturnFields(call, schema, compileManager);

        if (functionVariable != null) {
            return new FunctionProxyBindable(
                    call, inputs, functionVariable, returnFields, call.isAsync());
        }

        BindableInterface delegate = createByName(
                call.getFuncName().getSimple(), schema, inputs, returnFields, compileManager);
        return new FunctionProxyBindable(call, inputs, delegate, call.isAsync());
    }

    private static List<RelDataTypeField> resolveReturnFields(
            SqlCallSqlFunction call,
            CalciteSchema schema,
            CompileManager compileManager
    ) throws Exception {
        SqlIdentifier likeTable = call.getLikeTableName();
        if (likeTable != null) {
            return SchemaUtils.getDataTypeByLikeTableName(likeTable.getSimple(), schema);
        }

        SqlNode likeFunction = call.getLikeFunctionName();
        if (likeFunction == null) {
            return null;
        }

        String functionName = SchemaUtils.getValueOfStringLiteral(likeFunction);
        SqlFunctionBindable bindable = compileManager.getSqlFunction(functionName);
        if (bindable == null) {
            throw new RuntimeException("like function not found: " + functionName);
        }
        return bindable.getReturnDataFields();
    }

    static BindableInterface createByName(
            String functionName,
            CalciteSchema schema,
            List<SqlNode> inputs,
            List<RelDataTypeField> returnFields,
            CompileManager compileManager
    ) throws Exception {
        Object javaFunction = JavaFunctionUtils.getTableFunction(
                Consts.DEFAULT_SCHEMA_NAME, functionName);
        if (javaFunction != null) {
            return new JavaFunctionBindable(
                    functionName, javaFunction, inputs, returnFields, schema);
        }

        SqlFunctionBindable sqlFunction = compileManager.getSqlFunction(functionName);
        if (sqlFunction == null) {
            throw new Exception("function not find: " + functionName);
        }

        List<String> inputTables = new ArrayList<>(inputs.size());
        for (SqlNode input : inputs) {
            if (!(input instanceof SqlIdentifier identifier)) {
                throw new Exception("function input table must be table name");
            }
            inputTables.add(identifier.getSimple());
        }

        CallSqlFunctionBindable bindable = new CallSqlFunctionBindable(
                functionName, inputTables, sqlFunction);
        bindable.checkInputTable(schema);
        return bindable;
    }
}
