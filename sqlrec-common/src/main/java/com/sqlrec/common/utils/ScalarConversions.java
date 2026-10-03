package com.sqlrec.common.utils;

import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Locale;

/** Converts scalar values; row projection and schema compatibility belong to their callers. */
public final class ScalarConversions {
    private ScalarConversions() {
    }

    /** Unknown, complex and temporal types pass through without interpretation. */
    public static Object convert(Object value, String type) {
        if (value == null) {
            return null;
        }
        String name = type.toUpperCase(Locale.ROOT);
        switch (name) {
            case "INT": name = "INTEGER"; break;
            case "NUMERIC": name = "DECIMAL"; break;
            case "STRING":
            case "TEXT": name = "VARCHAR"; break;
            default: break;
        }
        return convert(value, SqlTypeName.get(name));
    }

    public static Object convert(Object value, SqlTypeName target) {
        if (value == null || target == null) {
            return value;
        }
        switch (target) {
            case TINYINT:
                return value instanceof Byte ? value : number(value, target).byteValue();
            case SMALLINT:
                return value instanceof Short ? value : number(value, target).shortValue();
            case INTEGER:
                return value instanceof Integer ? value : number(value, target).intValue();
            case BIGINT:
                return value instanceof Long ? value : number(value, target).longValue();
            case FLOAT:
            case REAL:
                return value instanceof Float ? value : number(value, target).floatValue();
            case DOUBLE:
                return value instanceof Double ? value : number(value, target).doubleValue();
            case DECIMAL:
                return value instanceof BigDecimal ? value : new BigDecimal(value.toString());
            case BOOLEAN:
                return value instanceof Boolean ? value : Boolean.valueOf(value.toString());
            case VARCHAR:
            case CHAR:
                return value instanceof String ? value : value.toString();
            default:
                return value;
        }
    }

    private static Number number(Object value, SqlTypeName target) {
        if (value instanceof Number) {
            return (Number) value;
        }
        if (!(value instanceof String)) {
            throw new IllegalArgumentException(
                    "Cannot convert " + value.getClass().getName() + " to numeric type " + target);
        }
        String text = (String) value;
        try {
            switch (target) {
                case TINYINT: return Byte.valueOf(text);
                case SMALLINT: return Short.valueOf(text);
                case INTEGER: return Integer.valueOf(text);
                case BIGINT: return Long.valueOf(text);
                case FLOAT:
                case REAL: return Float.valueOf(text);
                case DOUBLE: return Double.valueOf(text);
                case DECIMAL: return new BigDecimal(text);
                default: return Double.valueOf(text);
            }
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Cannot parse '" + text + "' as " + target + " value", exception);
        }
    }
}
