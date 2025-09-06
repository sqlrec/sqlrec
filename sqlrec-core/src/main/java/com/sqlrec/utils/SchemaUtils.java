package com.sqlrec.utils;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.util.Objects;

public class SchemaUtils {
    public static void addFunction(CalciteSchema schema, String functionName, String className, String methodName) throws Exception {
        schema.plus().add(
                functionName, Objects.requireNonNull(createScalarFunction(className, methodName))
        );
    }

    public static void addFunction(CalciteSchema schema, String functionName, String className) throws Exception {
        addFunction(schema, functionName, className, "eval");
    }

    public static ScalarFunction createScalarFunction(String className, String methodName) throws Exception {
        Class<?> clazz = Class.forName(className);
        return ScalarFunctionImpl.create(clazz, methodName);
    }

    public static ScalarFunction createScalarFunction(String className) throws Exception {
        Class<?> clazz = Class.forName(className);
        return ScalarFunctionImpl.create(clazz, "eval");
    }
}
