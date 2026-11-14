package com.sqlrec.utils;

import com.google.common.primitives.Primitives;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.sql.parser.SqlGetVariable;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/** Selects an evaluate overload from the static types of SQLRec CALL arguments. */
public final class JavaFunctionMethodMatcher {
    private JavaFunctionMethodMatcher() {
    }

    public static Method selectEvalMethod(String functionName, Class<?> functionClass, List<SqlNode> inputs) {
        List<Method> evalMethods = getEvalMethods(functionClass);
        List<Method> matchedMethods = new ArrayList<>();

        int bestScore = Integer.MAX_VALUE;
        for (Method method : evalMethods) {
            int score = methodMatchScore(method, inputs);
            if (score >= 0 && score <= bestScore) {
                if (score < bestScore) {
                    matchedMethods.clear();
                    bestScore = score;
                }
                matchedMethods.add(method);
            }
        }

        if (matchedMethods.size() != 1) {
            StringJoiner actual = new StringJoiner(", ", functionName + "(", ")");
            if (inputs != null) inputs.forEach(input -> actual.add(typeName(inputType(input))));
            List<Method> candidates = matchedMethods.isEmpty() ? evalMethods : matchedMethods;
            String signatures = candidates.stream().map(method -> signature(functionName, method))
                    .sorted().collect(Collectors.joining(", "));
            throw new IllegalArgumentException((matchedMethods.isEmpty() ? "No matching overload for " : "Ambiguous overload for ")
                    + actual + "; candidates: " + (signatures.isEmpty() ? "none" : signatures));
        }

        return matchedMethods.get(0);
    }

    private static int methodMatchScore(Method method, List<SqlNode> inputs) {
        Class<?>[] paramTypes = method.getParameterTypes();
        int inputCount = inputs != null ? inputs.size() : 0;
        int inputIndex = 0;
        int score = 0;
        for (int i = 0; i < paramTypes.length; i++) {
            Class<?> paramType = paramTypes[i];
            if (paramType == ExecuteContext.class || paramType == ReadonlyContext.class) {
                continue;
            }
            boolean varArg = method.isVarArgs() && i == paramTypes.length - 1;
            int count = varArg ? inputCount - inputIndex : 1;
            for (int j = 0; j < count; j++) {
                if (inputIndex >= inputCount) return -1;
                int match = inputMatchScore(inputs.get(inputIndex++), varArg ? paramType.getComponentType() : paramType);
                if (match < 0) return -1;
                score += match;
            }
        }
        return inputIndex == inputCount ? score : -1;
    }

    private static int inputMatchScore(SqlNode input, Class<?> paramType) {
        if (paramType == CacheTable.class) {
            return input instanceof SqlIdentifier ? 0 : -1;
        }
        Class<?> inputType = inputType(input);
        Class<?> target = Primitives.wrap(paramType);
        if (target == inputType) return 0;
        // Only widen numeric arguments; strings are never implicitly parsed during overload selection.
        if (inputType == Integer.class && target == Long.class) return 1;
        if ((inputType == Integer.class || inputType == Long.class) && target == Double.class) return 2;
        return -1;
    }

    private static Class<?> inputType(SqlNode input) {
        if (input instanceof SqlIdentifier) return CacheTable.class;
        if (input instanceof SqlGetVariable variable) {
            return variable.hasDefaultValue()
                    ? literalType((SqlLiteral) variable.getDefaultValue()) : String.class;
        }
        return input instanceof SqlLiteral literal ? literalType(literal) : Void.class;
    }

    private static String signature(String functionName, Method method) {
        StringJoiner signature = new StringJoiner(", ", functionName + "(", ")");
        Class<?>[] parameters = method.getParameterTypes();
        for (int i = 0; i < parameters.length; i++) {
            Class<?> type = parameters[i];
            if (type == ExecuteContext.class || type == ReadonlyContext.class) continue;
            boolean varArg = method.isVarArgs() && i == parameters.length - 1;
            signature.add(varArg ? typeName(type.getComponentType()) + "..." : typeName(type));
        }
        return signature.toString();
    }

    private static String typeName(Class<?> type) {
        Class<?> boxed = Primitives.wrap(type);
        if (boxed == Integer.class) return "INT";
        if (boxed == Long.class) return "BIGINT";
        if (boxed == Double.class) return "DOUBLE";
        if (boxed == Boolean.class) return "BOOLEAN";
        if (boxed == String.class) return "VARCHAR";
        if (boxed == CacheTable.class) return "TABLE";
        return boxed == Void.class ? "UNKNOWN" : boxed.getSimpleName();
    }

    private static Class<?> literalType(SqlLiteral literal) {
        if (literal instanceof SqlCharStringLiteral) return String.class;
        if (literal.getValue() instanceof Boolean) return Boolean.class;
        if (literal instanceof SqlNumericLiteral number) {
            if (!number.isInteger()) return Double.class;
            BigDecimal value = number.getValueAs(BigDecimal.class);
            if (value.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0
                    && value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0) return Integer.class;
            if (value.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0
                    && value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0) return Long.class;
            return Double.class;
        }
        return Void.class;
    }

    private static List<Method> getEvalMethods(Class<?> functionClass) {
        Method[] allMethods = functionClass.getMethods();
        List<Method> evalMethods = new ArrayList<>();
        for (Method method : allMethods) {
            if (method.getName().equals("evaluate") && !method.isBridge() && Modifier.isPublic(method.getModifiers())) {
                evalMethods.add(method);
            }
        }
        return evalMethods;
    }
}
