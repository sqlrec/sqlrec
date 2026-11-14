package com.sqlrec.frontend.utils;

import java.sql.Types;
import java.util.Locale;
import java.util.regex.Pattern;

/** SQL type conversions used by JDBC column metadata and GetTypeInfo. */
public final class JdbcTypeUtils {
    private static final Pattern TYPE_PRECISION = Pattern.compile("(?i)^[A-Z_]+\\s*\\(\\s*(\\d+)");
    private static final Pattern DECIMAL_SCALE = Pattern.compile("(?i)^DECIMAL\\s*\\(\\s*\\d+\\s*,\\s*(\\d+)");

    private JdbcTypeUtils() {
    }

    public static String typeName(String type) {
        return type.replaceAll("(?i)\\s+NOT\\s+NULL$", "");
    }

    public static boolean isNullable(String type) {
        return !type.toUpperCase(Locale.ROOT).endsWith("NOT NULL");
    }

    public static int jdbcType(String type) {
        return switch (baseType(type)) {
            case "BOOLEAN" -> Types.BOOLEAN;
            case "TINYINT" -> Types.TINYINT;
            case "SMALLINT" -> Types.SMALLINT;
            case "INT", "INTEGER" -> Types.INTEGER;
            case "BIGINT" -> Types.BIGINT;
            case "FLOAT" -> Types.FLOAT;
            case "DOUBLE" -> Types.DOUBLE;
            case "DECIMAL" -> Types.DECIMAL;
            case "CHAR" -> Types.CHAR;
            case "VARCHAR", "STRING" -> Types.VARCHAR;
            case "BINARY", "VARBINARY", "BYTES" -> Types.BINARY;
            case "DATE" -> Types.DATE;
            case "TIME" -> Types.TIME;
            case "TIMESTAMP", "TIMESTAMP_LTZ" -> Types.TIMESTAMP;
            case "ARRAY" -> Types.ARRAY;
            case "ROW", "STRUCT" -> Types.STRUCT;
            default -> Types.JAVA_OBJECT;
        };
    }

    public static int precision(String type) {
        String base = baseType(type);
        if (base.equals("TIME") || base.equals("TIMESTAMP") || base.equals("TIMESTAMP_LTZ")) {
            int fraction = fractionalPrecision(type);
            return (base.equals("TIME") ? 8 : 19) + (fraction == 0 ? 0 : fraction + 1);
        }
        var match = TYPE_PRECISION.matcher(type);
        if (match.find()) {
            return Integer.parseInt(match.group(1));
        }
        return switch (base) {
            case "TINYINT" -> 3;
            case "SMALLINT" -> 5;
            case "INT", "INTEGER" -> 10;
            case "BIGINT" -> 19;
            case "FLOAT" -> 7;
            case "DOUBLE" -> 15;
            case "DECIMAL" -> 38;
            case "DATE" -> 10;
            case "BOOLEAN" -> 1;
            default -> Integer.MAX_VALUE;
        };
    }

    public static Integer scale(String type) {
        return switch (baseType(type)) {
            case "TIME", "TIMESTAMP", "TIMESTAMP_LTZ" -> fractionalPrecision(type);
            case "DECIMAL" -> {
                var match = DECIMAL_SCALE.matcher(type);
                yield match.find() ? Integer.valueOf(match.group(1)) : 0;
            }
            case "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT" -> 0;
            default -> null;
        };
    }

    private static String baseType(String type) {
        return type.strip().toUpperCase(Locale.ROOT).split("[<(\\s]", 2)[0];
    }

    private static int fractionalPrecision(String type) {
        var match = TYPE_PRECISION.matcher(type);
        return match.find() ? Integer.parseInt(match.group(1)) : baseType(type).equals("TIME") ? 0 : 9;
    }

    public static Integer numericRadix(int jdbcType) {
        return switch (jdbcType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                    Types.FLOAT, Types.DOUBLE, Types.DECIMAL -> 10;
            default -> null;
        };
    }
}
