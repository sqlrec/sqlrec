package com.sqlrec.runtime;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.Const;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.util.*;

public class JavaFunctionBindable extends BindableInterface {
    private String functionName;
    private Object tableFunction;
    private List<SqlNode> inputTableList;
    private List<RelDataTypeField> returnDataFields;
    private Method evalMethod;
    private boolean isAsync;

    public JavaFunctionBindable(
            String functionName,
            Object tableFunction,
            List<SqlNode> inputTableList,
            String likeTableName,
            CalciteSchema schema,
            boolean needReturnSchema,
            boolean isAsync
    ) {
        this.functionName = functionName;
        this.tableFunction = tableFunction;
        this.inputTableList = inputTableList;
        this.evalMethod = getEvalMethod(tableFunction);
        this.isAsync = isAsync;

        if (!StringUtils.isEmpty(likeTableName)) {
            returnDataFields = getDataTypeByLikeTableName(likeTableName, schema);
        } else {
            if (needReturnSchema && CacheTable.class.isAssignableFrom(evalMethod.getReturnType())) {
                Object outputTable = callEvalMethod(schema, new ExecuteContextImpl());
                if (outputTable == null) {
                    throw new RuntimeException("table function return null");
                }
                returnDataFields = ((CacheTable) outputTable).getDataFields();
            }
        }
    }

    public Method getEvalMethod(Object tableFunction) {
        Method[] allMethods = tableFunction.getClass().getMethods();
        List<Method> evalMethods = new ArrayList<>();
        for (Method method : allMethods) {
            if (method.getName().equals("eval")) {
                evalMethods.add(method);
            }
        }
        if (evalMethods.size() != 1) {
            throw new RuntimeException("table function must have one eval method");
        }
        return evalMethods.get(0);
    }

    public static List<RelDataTypeField> getDataTypeByLikeTableName(
            String likeTableName,
            CalciteSchema schema) {
        CalciteSchema.TableEntry tableEntry = schema.getTable(likeTableName, false);
        if (tableEntry == null) {
            throw new RuntimeException("like table not found: " + likeTableName);
        }
        Table table = tableEntry.getTable();
        if (!(table instanceof CacheTable)) {
            throw new RuntimeException("like table must be cache table for table function");
        }
        return ((CacheTable) table).getDataFields();
    }

    private Object callEvalMethod(CalciteSchema schema, ExecuteContext context) {
        Class<?>[] paramTypes = evalMethod.getParameterTypes();
        List<Object> paramList = new ArrayList<>();
        int inputParamIndex = 0;

        for (Class<?> paramType : paramTypes) {
            SqlNode input = inputTableList.get(inputParamIndex);
            if (paramType.equals(CacheTable.class)) {
                if (!(input instanceof SqlIdentifier)) {
                    throw new RuntimeException("should use cache table as input for " + inputParamIndex);
                }
                paramList.add(getCacheTable(((SqlIdentifier) inputTableList.get(inputParamIndex)).getSimple(), schema));
                inputParamIndex++;
            } else if (paramType.equals(String.class)) {
                if (input instanceof SqlCharStringLiteral) {
                    paramList.add(SchemaUtils.getValueOfStringLiteral((SqlCharStringLiteral) input));
                } else if (input instanceof SqlGetVariable) {
                    String variableName = SchemaUtils.getValueOfStringLiteral(((SqlGetVariable) input).getVariableName());
                    paramList.add(context.getVariable(variableName));
                } else {
                    throw new RuntimeException("input " + inputParamIndex + " must be char string literal or variable");
                }
                inputParamIndex++;
            } else if (paramType.equals(ExecuteContext.class)) {
                paramList.add(context);
            } else {
                throw new RuntimeException("input " + inputParamIndex + " must be cache table, char string literal or variable");
            }
        }

        if (inputParamIndex != inputTableList.size()) {
            throw new RuntimeException("input parameter count not match");
        }

        try {
            if (isAsync) {
                Const.executorService.submit(() -> evalMethod.invoke(tableFunction, paramList.toArray()));
                return null;
            } else {
                return evalMethod.invoke(tableFunction, paramList.toArray());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CacheTable getCacheTable(String inputTableName, CalciteSchema schema) {
        Table table = Objects.requireNonNull(
                        schema.getTable(inputTableName, false),
                        "input table not found: " + inputTableName)
                .getTable();
        if (!(table instanceof CacheTable)) {
            throw new RuntimeException("input table must be cache table for table function");
        }
        return (CacheTable) table;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        Object outputTable = callEvalMethod(schema, context);
        if (outputTable == null) {
            return null;
        }
        if (outputTable instanceof CacheTable) {
            return ((CacheTable) outputTable).scan(null);
        } else {
            return null;
        }
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return returnDataFields;
    }

    @Override
    public boolean isParallelizable() {
        Class<?>[] paramTypes = evalMethod.getParameterTypes();
        for (Class<?> paramType : paramTypes) {
            if (paramType.equals(ExecuteContext.class)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Set<String> getReadTables() {
        Set<String> readTables = new HashSet<>();
        for (SqlNode input : inputTableList) {
            if (input instanceof SqlIdentifier) {
                readTables.add(((SqlIdentifier) input).getSimple());
            }
        }
        return readTables;
    }

    @Override
    public Set<String> getWriteTables() {
        return Set.of();
    }

    public String getFunName() {
        return functionName;
    }
}
