package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.JavaFunctionUtils;
import com.sqlrec.utils.SchemaUtils;
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
    private List<RelDataTypeField> returnDataFields;
    private boolean isAsync;

    public FunctionProxyBindable(
            List<SqlNode> inputList,
            SqlGetVariable funcNameVariable,
            List<RelDataTypeField> returnDataFields,
            boolean isAsync
    ) {
        if (returnDataFields == null) {
            throw new RuntimeException("return data fields is null");
        }
        
        this.inputList = inputList;
        this.funcNameVariable = funcNameVariable;
        this.returnDataFields = returnDataFields;
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
        SqlNode likeFunctionNameNode = callSqlFunction.getLikeFunctionName();
        
        List<RelDataTypeField> returnDataFields = null;
        if (likeTableNameIdentifier != null) {
            String likeTableName = likeTableNameIdentifier.getSimple();
            returnDataFields = SchemaUtils.getDataTypeByLikeTableName(likeTableName, schema);
        } else if (likeFunctionNameNode != null) {
            String likeFunctionName = SchemaUtils.getValueOfStringLiteral(likeFunctionNameNode);
            SqlFunctionBindable likeFunctionBindable = compileManager.getSqlFunction(likeFunctionName);
            if (likeFunctionBindable == null) {
                throw new RuntimeException("like function not found: " + likeFunctionName);
            }
            returnDataFields = likeFunctionBindable.getReturnDataFields();
        }

        if (funcNameVariable != null) {
            return new FunctionProxyBindable(
                    inputList, funcNameVariable, returnDataFields, callSqlFunction.isAsync()
            );
        }

        String functionName = callSqlFunction.getFuncName().getSimple();
        return getFunctionBindableByName(
                functionName, schema, inputList, returnDataFields, callSqlFunction.isAsync(), compileManager
        );
    }

    public static BindableInterface getFunctionBindableByName(
            String functionName,
            CalciteSchema schema,
            List<SqlNode> inputList,
            List<RelDataTypeField> returnDataFields,
            boolean isAsync,
            CompileManager compileManager
    ) throws Exception {
        Object javaFunctionObj = JavaFunctionUtils.getTableFunction(Consts.DEFAULT_SCHEMA_NAME, functionName);
        if (javaFunctionObj != null) {
            return new JavaFunctionBindable(
                    functionName, javaFunctionObj, inputList, returnDataFields, schema, isAsync
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
                    functionName, schema, inputList, returnDataFields, isAsync, new CompileManager()
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
