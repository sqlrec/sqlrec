package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.runtime.UnionLikeTableFunction;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JavaFunctionBindable extends BindableInterface {
    private final String functionName;
    private final Object tableFunction;
    private final List<SqlNode> inputTableList;
    private final List<RelDataTypeField> returnDataFields;
    private final Method evalMethod;
    private final Class<?>[] parameterTypes;
    private final boolean varArgs;

    public JavaFunctionBindable(
            String functionName,
            Object tableFunction,
            List<SqlNode> inputTableList,
            List<RelDataTypeField> returnDataFields,
            CalciteSchema schema
    ) {
        this.functionName = functionName;
        this.tableFunction = tableFunction;
        this.inputTableList = inputTableList;
        this.evalMethod = selectEvalMethod(tableFunction, inputTableList);
        this.parameterTypes = evalMethod.getParameterTypes();
        this.varArgs = evalMethod.isVarArgs();

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

    public static List<Method> getEvalMethods(Object tableFunction) {
        Method[] allMethods = tableFunction.getClass().getMethods();
        List<Method> evalMethods = new ArrayList<>();
        for (Method method : allMethods) {
            if (method.getName().equals("evaluate") && Modifier.isPublic(method.getModifiers())) {
                evalMethods.add(method);
            }
        }
        if (evalMethods.isEmpty()) {
            throw new RuntimeException("table function must have at least one evaluate method");
        }
        return evalMethods;
    }

    private static Method selectEvalMethod(Object tableFunction, List<SqlNode> inputs) {
        List<Method> evalMethods = getEvalMethods(tableFunction);
        List<Method> matchedMethods = new ArrayList<>();

        for (Method method : evalMethods) {
            if (isMethodMatch(method, inputs)) {
                matchedMethods.add(method);
            }
        }

        if (matchedMethods.size() != 1) {
            int inputCount = inputs != null ? inputs.size() : 0;
            throw new RuntimeException("found " + matchedMethods.size() + " evaluate method(s) for " + inputCount + " parameters, expected exactly 1");
        }

        return matchedMethods.get(0);
    }

    private static boolean isMethodMatch(Method method, List<SqlNode> inputs) {
        Class<?>[] paramTypes = method.getParameterTypes();
        boolean isVarArgs = method.isVarArgs();
        int inputCount = inputs != null ? inputs.size() : 0;
        int inputIndex = 0;

        for (int i = 0; i < paramTypes.length; i++) {
            Class<?> paramType = paramTypes[i];

            if (isVarArgs && i == paramTypes.length - 1) {
                Class<?> varArgType = paramType.getComponentType();
                while (inputIndex < inputCount) {
                    if (!isInputTypeMatch(inputs.get(inputIndex), varArgType)) {
                        return false;
                    }
                    inputIndex++;
                }
            } else if (paramType.equals(ExecuteContext.class) || paramType.equals(ReadonlyContext.class)) {
                continue;
            } else {
                if (inputIndex >= inputCount) {
                    return false;
                }
                if (!isInputTypeMatch(inputs.get(inputIndex), paramType)) {
                    return false;
                }
                inputIndex++;
            }
        }

        return inputIndex == inputCount;
    }

    private static boolean isInputTypeMatch(SqlNode input, Class<?> paramType) {
        if (paramType.equals(CacheTable.class)) {
            return input instanceof SqlIdentifier;
        } else if (paramType.equals(String.class)) {
            return input instanceof SqlCharStringLiteral || input instanceof SqlGetVariable;
        }
        return false;
    }

    private static Object resolveStringInput(SqlNode input, ExecuteContext context, int inputIndex) {
        if (input instanceof SqlCharStringLiteral) {
            return SchemaUtils.getValueOfStringLiteral((SqlCharStringLiteral) input);
        } else if (input instanceof SqlGetVariable) {
            return RuntimeValueResolver.resolveVariable((SqlGetVariable) input, context,
                    RuntimeValueResolver.MissingValuePolicy.NULL_ONLY);
        } else {
            throw new RuntimeException("input " + inputIndex + " must be char string literal or variable");
        }
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
        if (paramType.equals(String.class)) {
            return resolveStringInput(input, context, inputIndex);
        }
        throw new RuntimeException("unsupported parameter type: " + paramType);
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
        Object[] arguments = new Object[parameterTypes.length];
        int inputParamIndex = 0;

        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> paramType = parameterTypes[i];

            if (varArgs && i == parameterTypes.length - 1) {
                arguments[i] = resolveVarArgs(paramType.getComponentType(), inputParamIndex, schema, context);
            } else if (paramType.equals(ExecuteContext.class) || paramType.equals(ReadonlyContext.class)) {
                arguments[i] = context;
            } else {
                if (inputParamIndex >= inputTableList.size()) {
                    throw new RuntimeException("not enough input parameters");
                }
                arguments[i] = resolveInput(paramType, inputParamIndex, schema, context);
                inputParamIndex++;
            }
        }

        if (!varArgs && inputParamIndex != inputTableList.size()) {
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
        if (outputTable instanceof CacheTable) {
            CacheTable cacheTable = (CacheTable) outputTable;
            List<Object[]> rows = cacheTable.scan(null).toList();
            rows = RowTransformUtils.adaptRowsToSchema(rows, returnDataFields, cacheTable.getDataFields());
            return Linq4j.asEnumerable(rows);
        } else {
            throw new RuntimeException("output table is not a CacheTable: " + outputTable.getClass().getName());
        }
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return returnDataFields;
    }

    @Override
    public boolean isParallelizable() {
        for (Class<?> paramType : parameterTypes) {
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

    @Override
    public boolean isUnionSql() {
        return tableFunction instanceof UnionLikeTableFunction;
    }

    public Set<String> getDependencyJavaFuncName() {
        return Set.of(functionName);
    }
}
