package com.sqlrec.udf.scalar;

import com.sqlrec.common.runtime.SqlRecDataContext;
import org.apache.calcite.DataContext;

public class GetFunction {
    public static String evaluate(DataContext context, String key) {
        if (!(context instanceof SqlRecDataContext)) {
            throw new IllegalArgumentException("context must be SqlRecDataContext");
        }
        SqlRecDataContext sqlRecDataContext = (SqlRecDataContext) context;
        return sqlRecDataContext.getVariable(key);
    }

    public static String evaluate(DataContext context, String key, String defaultValue) {
        String value = evaluate(context, key);
        return value != null ? value : defaultValue;
    }

    public static Integer evaluate(DataContext context, String key, Integer defaultValue) {
        String value = evaluate(context, key);
        return value != null ? Integer.valueOf(value) : defaultValue;
    }

    public static Long evaluate(DataContext context, String key, Long defaultValue) {
        String value = evaluate(context, key);
        return value != null ? Long.valueOf(value) : defaultValue;
    }

    public static Double evaluate(DataContext context, String key, Double defaultValue) {
        String value = evaluate(context, key);
        return value != null ? Double.valueOf(value) : defaultValue;
    }

    public static Boolean evaluate(DataContext context, String key, Boolean defaultValue) {
        String value = evaluate(context, key);
        if (value == null) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("Variable " + key + " must be true or false");
    }
}
