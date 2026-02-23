package com.sqlrec.utils;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCharStringLiteral;

import java.util.Objects;

public class SchemaUtils {
    public static void addFunction(CalciteSchema schema, String functionName, String className) throws Exception {
        schema.plus().add(
                functionName, Objects.requireNonNull(createScalarFunction(className))
        );
    }

    public static ScalarFunction createScalarFunction(String className) throws Exception {
        return createScalarFunction(className, "eval");
    }

    public static ScalarFunction createScalarFunction(String className, String methodName) throws Exception {
        Class<?> clazz = Class.forName(className);
        return ScalarFunctionImpl.create(clazz, methodName);
    }

    public static String getValueOfStringLiteral(SqlCharStringLiteral value) {
        if (value == null) {
            return null;
        }
        String valueStr = value.toString();
        return removeQuotes(valueStr);
    }

    public static String removeQuotes(String value) {
        if (value == null) {
            return null;
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
