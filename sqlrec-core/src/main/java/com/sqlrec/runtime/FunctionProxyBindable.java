package com.sqlrec.runtime;

import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.SchemaUtils;
import com.sqlrec.utils.TableFunctionUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FunctionProxyBindable implements BindableInterface {
    private List<SqlNode> inputList;
    private SqlGetVariable funcNameVariable;
    private String likeTableName;
    private List<RelDataTypeField> returnDataFields;
    private boolean needReturnSchema;
    private boolean isAsync;

    public FunctionProxyBindable(
            List<SqlNode> inputList,
            SqlGetVariable funcNameVariable,
            String likeTableName,
            CalciteSchema schema,
            boolean needReturnSchema,
            boolean isAsync
    ) {
        if (needReturnSchema) {
            if (StringUtils.isEmpty(likeTableName)) {
                throw new RuntimeException("like table name is empty");
            }
            returnDataFields = JavaFunctionBindable.getDataTypeByLikeTableName(likeTableName, schema);
        }

        this.inputList = inputList;
        this.funcNameVariable = funcNameVariable;
        this.likeTableName = likeTableName;
        this.needReturnSchema = needReturnSchema;
        this.isAsync = isAsync;
    }

    public static BindableInterface getFunctionBindable(
            SqlCallSqlFunction callSqlFunction,
            CalciteSchema schema,
            boolean needReturnSchema
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
                    inputList, funcNameVariable, likeTableName, schema, needReturnSchema, callSqlFunction.isAsync()
            );
        }

        String functionName = callSqlFunction.getFuncName().getSimple();
        return getFunctionBindableByName(
                functionName, schema, inputList, likeTableName, needReturnSchema, callSqlFunction.isAsync()
        );
    }

    public static BindableInterface getFunctionBindableByName(
            String functionName,
            CalciteSchema schema,
            List<SqlNode> inputList,
            String likeTableName,
            boolean needReturnSchema,
            boolean isAsync
    ) throws Exception {
        // todo check is function name ambiguous
        Object javaFunctionObj = TableFunctionUtils.getTableFunction(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, functionName);
        if (javaFunctionObj != null) {
            return new JavaFunctionBindable(
                    functionName, javaFunctionObj, inputList, likeTableName, schema, needReturnSchema, isAsync
            );
        }

        SqlFunctionBindable sqlFunctionBindable = CompileManager.compileSqlFunction(functionName);
        if (sqlFunctionBindable != null) {
            List<String> inputTableList = new ArrayList<>();
            for (SqlNode input : inputList) {
                if (input instanceof SqlIdentifier) {
                    inputTableList.add(((SqlIdentifier) input).getSimple());
                } else {
                    throw new Exception("function input table must be table name");
                }
            }
            if (sqlFunctionBindable.getInputTables().size() != inputTableList.size()) {
                throw new Exception("function input table not match");
            }
            return new CallSqlFunctionBindable(functionName, inputTableList, sqlFunctionBindable, isAsync);
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
                    functionName, schema, inputList, likeTableName, needReturnSchema, isAsync
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
