package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ConfigContext;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.Executor;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
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
            List<RelDataTypeField> returnDataFields,
            CalciteSchema schema,
            boolean isAsync
    ) {
        this.functionName = functionName;
        this.tableFunction = tableFunction;
        this.inputTableList = inputTableList;
        this.evalMethod = selectEvalMethod(tableFunction, inputTableList);
        this.isAsync = isAsync;

        if (returnDataFields != null) {
            this.returnDataFields = returnDataFields;
        } else {
            if (!isAsync && CacheTable.class.isAssignableFrom(evalMethod.getReturnType())) {
                Object outputTable = callEvalMethod(schema, new ExecuteContextImpl());
                if (outputTable == null) {
                    throw new RuntimeException("table function return null");
                }
                this.returnDataFields = ((CacheTable) outputTable).getDataFields();
            }
        }
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
            } else if (paramType.equals(ExecuteContext.class) || paramType.equals(ConfigContext.class)) {
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
            String variableName = SchemaUtils.getValueOfStringLiteral(((SqlGetVariable) input).getVariableName());
            return context.getVariable(variableName);
        } else {
            throw new RuntimeException("input " + inputIndex + " must be char string literal or variable");
        }
    }

    private Object callEvalMethod(CalciteSchema schema, ExecuteContext context) {
        Class<?>[] paramTypes = evalMethod.getParameterTypes();
        List<Object> paramList = new ArrayList<>();
        int inputParamIndex = 0;
        boolean isVarArgs = evalMethod.isVarArgs();

        for (int i = 0; i < paramTypes.length; i++) {
            Class<?> paramType = paramTypes[i];
            
            if (isVarArgs && i == paramTypes.length - 1) {
                Class<?> varArgType = paramType.getComponentType();
                List<Object> varArgs = new ArrayList<>();
                while (inputParamIndex < inputTableList.size()) {
                    SqlNode input = inputTableList.get(inputParamIndex);
                    if (varArgType.equals(String.class)) {
                        varArgs.add(resolveStringInput(input, context, inputParamIndex));
                    } else if (varArgType.equals(CacheTable.class)) {
                        if (!(input instanceof SqlIdentifier)) {
                            throw new RuntimeException("should use cache table as input for " + inputParamIndex);
                        }
                        varArgs.add(SchemaUtils.getCacheTable(((SqlIdentifier) input).getSimple(), schema));
                    } else {
                        throw new RuntimeException("unsupported vararg type: " + varArgType);
                    }
                    inputParamIndex++;
                }
                Object varArgArray = java.lang.reflect.Array.newInstance(varArgType, varArgs.size());
                for (int j = 0; j < varArgs.size(); j++) {
                    java.lang.reflect.Array.set(varArgArray, j, varArgs.get(j));
                }
                paramList.add(varArgArray);
            } else if (paramType.equals(CacheTable.class)) {
                if (inputParamIndex >= inputTableList.size()) {
                    throw new RuntimeException("not enough input parameters");
                }
                SqlNode input = inputTableList.get(inputParamIndex);
                if (!(input instanceof SqlIdentifier)) {
                    throw new RuntimeException("should use cache table as input for " + inputParamIndex);
                }
                paramList.add(SchemaUtils.getCacheTable(((SqlIdentifier) inputTableList.get(inputParamIndex)).getSimple(), schema));
                inputParamIndex++;
            } else if (paramType.equals(String.class)) {
                if (inputParamIndex >= inputTableList.size()) {
                    throw new RuntimeException("not enough input parameters");
                }
                paramList.add(resolveStringInput(inputTableList.get(inputParamIndex), context, inputParamIndex));
                inputParamIndex++;
            } else if (paramType.equals(ExecuteContext.class)) {
                paramList.add(context);
            } else if (paramType.equals(ConfigContext.class)) {
                paramList.add(new ConfigContextImpl());
            } else {
                throw new RuntimeException("unsupported parameter type: " + paramType);
            }
        }

        if (!isVarArgs && inputParamIndex != inputTableList.size()) {
            throw new RuntimeException("input parameter count not match");
        }

        try {
            if (isAsync) {
                Executor.getExecutorService().submit(() -> evalMethod.invoke(tableFunction, paramList.toArray()));
                return null;
            } else {
                return evalMethod.invoke(tableFunction, paramList.toArray());
            }
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

    public String getDependencyJavaFuncName() {
        return functionName;
    }
}
