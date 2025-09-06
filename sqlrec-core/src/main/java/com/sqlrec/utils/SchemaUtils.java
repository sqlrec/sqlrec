package com.sqlrec.utils;

import com.sqlrec.common.schema.TableFunction;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

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
        if (clazz.isAssignableFrom(TableFunction.class)) {
            return null;
        }
        return ScalarFunctionImpl.create(clazz, methodName);
    }
}
