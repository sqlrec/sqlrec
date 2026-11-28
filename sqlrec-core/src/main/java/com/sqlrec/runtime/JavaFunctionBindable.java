package com.sqlrec.runtime;

import com.google.common.primitives.Primitives;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.runtime.UnionLikeTableFunction;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.JavaFunctionMethodMatcher;
import com.sqlrec.utils.JavaFunctionMethodMatcher.ParameterKind;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JavaFunctionBindable extends BindableInterface {
    private final Object tableFunction;
    private final List<SqlNode> inputTableList;
    private final List<RelDataTypeField> returnDataFields;
    private final Method evalMethod;
    private final Parameter[] parameters;

    public JavaFunctionBindable(
            String functionName,
            Object tableFunction,
            List<SqlNode> inputTableList,
            List<RelDataTypeField> returnDataFields,
            CalciteSchema schema
    ) {
        this.tableFunction = tableFunction;
        this.inputTableList = inputTableList;
        this.evalMethod = JavaFunctionMethodMatcher.selectEvalMethod(functionName, tableFunction.getClass(), inputTableList);
        this.parameters = evalMethod.getParameters();

        List<RelDataTypeField> resolvedReturnDataFields = returnDataFields;
        if (resolvedReturnDataFields == null && CacheTable.class.isAssignableFrom(evalMethod.getReturnType())) {
            if (!SqlRecConfigs.ENABLE_JAVA_FUNCTION_SCHEMA_INFERENCE.getValue()) {
                throw new IllegalArgumentException("Java function " + functionName
                        + " requires an explicit return schema via LIKE table or LIKE FUNCTION when "
                        + "ENABLE_JAVA_FUNCTION_SCHEMA_INFERENCE is false");
            }
            Object outputTable = callEvalMethod(schema, new ExecuteContextImpl(schema));
            if (outputTable == null) {
                throw new RuntimeException("table function return null");
            }
            resolvedReturnDataFields = ((CacheTable) outputTable).getDataFields();
        }
        this.returnDataFields = resolvedReturnDataFields;
    }

    private static CacheTable resolveTableInput(SqlNode input, CalciteSchema schema, int inputIndex) {
        if (!(input instanceof SqlIdentifier identifier)) {
            throw new RuntimeException("should use cache table as input for " + inputIndex);
        }
        return SchemaUtils.getCacheTable(identifier.getSimple(), schema);
    }

    private Object resolveInput(
            Class<?> paramType, int inputIndex, CalciteSchema schema, ExecuteContext context) {
        SqlNode input = inputTableList.get(inputIndex);
        if (paramType.equals(CacheTable.class)) {
            return resolveTableInput(input, schema, inputIndex);
        }
        String value = input instanceof SqlGetVariable variable
                ? RuntimeValueResolver.resolveVariable(variable, context, RuntimeValueResolver.MissingValuePolicy.NULL_ONLY)
                : RuntimeValueResolver.literalText((SqlLiteral) input);
        if (value == null) {
            if (paramType.isPrimitive()) throw new IllegalArgumentException("input " + inputIndex + " cannot be null");
            return null;
        }
        Class<?> target = Primitives.wrap(paramType);
        if (target == String.class) return value;
        if (target == Integer.class) return Integer.valueOf(value);
        if (target == Long.class) return Long.valueOf(value);
        if (target == Double.class) return Double.valueOf(value);
        if (target == Boolean.class) {
            if ("true".equalsIgnoreCase(value)) return true;
            if ("false".equalsIgnoreCase(value)) return false;
            throw new IllegalArgumentException("input " + inputIndex + " must be true or false");
        }
        throw new IllegalArgumentException("unsupported parameter type: " + paramType);
    }

    private Object resolveVarArgs(
            Class<?> componentType, int startIndex, CalciteSchema schema, ExecuteContext context) {
        Object array = java.lang.reflect.Array.newInstance(componentType, inputTableList.size() - startIndex);
        for (int i = startIndex; i < inputTableList.size(); i++) {
            Object value = resolveInput(componentType, i, schema, context);
            java.lang.reflect.Array.set(array, i - startIndex, value);
        }
        return array;
    }

    private Object callEvalMethod(CalciteSchema schema, ExecuteContext context) {
        Object[] arguments = new Object[parameters.length];
        int inputParamIndex = 0;

        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];
            switch (JavaFunctionMethodMatcher.parameterKind(parameter)) {
                case VARARGS -> {
                    arguments[i] = resolveVarArgs(parameter.getType().getComponentType(), inputParamIndex, schema, context);
                    inputParamIndex = inputTableList.size();
                }
                case READONLY_CONTEXT, EXECUTE_CONTEXT -> arguments[i] = context;
                case INPUT -> {
                    if (inputParamIndex >= inputTableList.size()) {
                        throw new RuntimeException("not enough input parameters");
                    }
                    arguments[i] = resolveInput(parameter.getType(), inputParamIndex, schema, context);
                    inputParamIndex++;
                }
            }
        }

        if (inputParamIndex != inputTableList.size()) {
            throw new RuntimeException("input parameter count not match");
        }

        try {
            return evalMethod.invoke(tableFunction, arguments);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        Object outputTable = callEvalMethod(schema, context);
        if (outputTable == null) {
            return null;
        }
        if (!(outputTable instanceof CacheTable cacheTable)) {
            throw new RuntimeException("output table is not a CacheTable: " + outputTable.getClass().getName());
        }
        List<Object[]> rows = RowTransformUtils.adaptRowsToSchema(
                cacheTable.scan(null), returnDataFields, cacheTable.getDataFields());
        return Linq4j.asEnumerable(rows);
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return returnDataFields;
    }

    @Override
    public boolean isParallelizable() {
        for (Parameter parameter : parameters) {
            if (JavaFunctionMethodMatcher.parameterKind(parameter) == ParameterKind.EXECUTE_CONTEXT) {
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

    @Override
    public boolean isUnionSql() {
        return tableFunction instanceof UnionLikeTableFunction;
    }
}
