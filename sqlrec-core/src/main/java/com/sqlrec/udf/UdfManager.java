package com.sqlrec.udf;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.ScalarFunction;

import java.util.List;

public class UdfManager {
    public static void addFunction(CalciteSchema schema, String functionName, String className) throws Exception {
        for (ScalarFunction function : createScalarFunctions(className)) {
            schema.plus().add(functionName, function);
        }
    }

    public static List<ScalarFunction> createScalarFunctions(String className) throws Exception {
        Class<?> clazz = Class.forName(className);
        return SqlRecScalarFunctionImpl.createAll(clazz, "evaluate");
    }
}
