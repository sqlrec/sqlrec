package com.sqlrec.udf;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.ScalarFunction;

import java.util.Objects;

public class UdfManager {
    public static void addFunction(CalciteSchema schema, String functionName, String className) throws Exception {
        schema.plus().add(
                functionName, Objects.requireNonNull(createScalarFunction(className))
        );
    }

    public static ScalarFunction createScalarFunction(String className) throws Exception {
        return createScalarFunction(className, "evaluate");
    }

    public static ScalarFunction createScalarFunction(String className, String methodName) throws Exception {
        Class<?> clazz = Class.forName(className);
        return SqlRecScalarFunctionImpl.create(clazz, methodName);
    }
}
