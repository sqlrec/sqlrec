package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
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
        if (returnFields != null) {
            checkSqlReturnFields(functionName, returnFields, sqlFunction.getReturnDataFields());
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

    private static void checkSqlReturnFields(
            String functionName, List<RelDataTypeField> declared, List<RelDataTypeField> actual) {
        try {
            DataTypeUtils.checkTableSchemaSame(declared, actual == null ? List.of() : actual);
            JavaTypeFactoryImpl factory = new JavaTypeFactoryImpl();
            for (int i = 0; i < declared.size(); i++) {
                RelDataType expectedType = declared.get(i).getType();
                RelDataType actualType = actual.get(i).getType();
                // Rows are passed through unchanged: their complete type must fit the
                // compiled contract, including precision, nested types and NULLs.
                if (actualType.isNullable() && !expectedType.isNullable()
                        || !factory.createTypeWithNullability(actualType, expectedType.isNullable()).equals(expectedType)) {
                    throw new IllegalArgumentException("field " + declared.get(i).getName()
                            + " expects " + expectedType.getFullTypeString()
                            + ", got " + actualType.getFullTypeString());
                }
            }
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException("SQL function " + functionName
                    + " return schema does not match LIKE: " + failure.getMessage(), failure);
        }
    }
}
