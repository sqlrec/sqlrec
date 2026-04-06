package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.Const;
import com.sqlrec.utils.SchemaUtils;
import com.sqlrec.utils.JavaFunctionUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FunctionProxyBindable extends BindableInterface {
    private List<SqlNode> inputList;
    private SqlGetVariable funcNameVariable;
    private String likeTableName;
    private List<RelDataTypeField> returnDataFields;
    private boolean isAsync;

    public FunctionProxyBindable(
            List<SqlNode> inputList,
            SqlGetVariable funcNameVariable,
            String likeTableName,
            CalciteSchema schema,
            boolean isAsync
    ) {
        if (StringUtils.isEmpty(likeTableName)) {
            throw new RuntimeException("like table name is empty");
        }
        returnDataFields = JavaFunctionBindable.getDataTypeByLikeTableName(likeTableName, schema);

        this.inputList = inputList;
        this.funcNameVariable = funcNameVariable;
        this.likeTableName = likeTableName;
        this.isAsync = isAsync;
    }

    public static BindableInterface getFunctionBindable(
            SqlCallSqlFunction callSqlFunction,
            CalciteSchema schema,
            CompileManager compileManager
    ) throws Exception {
        List<SqlNode> inputList = callSqlFunction.getInputTableList();
        SqlGetVariable funcNameVariable = callSqlFunction.getFuncNameVariable();
        SqlIdentifier likeTableNameIdentifier = callSqlFunction.getLikeTableName();
        String likeTableName = null;
        if (likeTableNameIdentifier != null) {
            likeTableName = likeTableNameIdentifier.getSimple();
        }

        if (funcNameVariable != null) {
            return new FunctionProxyBindable(
                    inputList, funcNameVariable, likeTableName, schema, callSqlFunction.isAsync()
            );
        }

        String functionName = callSqlFunction.getFuncName().getSimple();
        return getFunctionBindableByName(
                functionName, schema, inputList, likeTableName, callSqlFunction.isAsync(), compileManager
        );
    }

    public static BindableInterface getFunctionBindableByName(
            String functionName,
            CalciteSchema schema,
            List<SqlNode> inputList,
            String likeTableName,
            boolean isAsync,
            CompileManager compileManager
    ) throws Exception {
        // todo check is function name ambiguous
        Object javaFunctionObj = JavaFunctionUtils.getTableFunction(Const.DEFAULT_SCHEMA_NAME, functionName);
        if (javaFunctionObj != null) {
            return new JavaFunctionBindable(
                    functionName, javaFunctionObj, inputList, likeTableName, schema, isAsync
            );
        }

        SqlFunctionBindable sqlFunctionBindable = compileManager.getSqlFunction(functionName);
        if (sqlFunctionBindable != null) {
            List<String> inputTableList = new ArrayList<>();
            for (SqlNode input : inputList) {
                if (input instanceof SqlIdentifier) {
                    inputTableList.add(((SqlIdentifier) input).getSimple());
                } else {
                    throw new Exception("function input table must be table name");
                }
            }
            CallSqlFunctionBindable callSqlFunctionBindable = new CallSqlFunctionBindable(
                    functionName, inputTableList, sqlFunctionBindable, isAsync);
            callSqlFunctionBindable.checkInputTable(schema);
            return callSqlFunctionBindable;
        }

        throw new Exception("function not find: " + functionName);
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        String variableName = SchemaUtils.getValueOfStringLiteral(funcNameVariable.getVariableName());
        String functionName = context.getVariable(variableName);
        if (StringUtils.isEmpty(functionName)) {
            throw new RuntimeException("cant get function name from variable: " + variableName);
        }
        BindableInterface bindableInterface = null;
        try {
            bindableInterface = getFunctionBindableByName(
                    functionName, schema, inputList, likeTableName, isAsync, new CompileManager()
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return bindableInterface.bind(schema, context);
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return returnDataFields;
    }

    @Override
    public boolean isParallelizable() {
        return false;
    }

    @Override
    public Set<String> getReadTables() {
        return Set.of();
    }

    @Override
    public Set<String> getWriteTables() {
        return Set.of();
    }
}
