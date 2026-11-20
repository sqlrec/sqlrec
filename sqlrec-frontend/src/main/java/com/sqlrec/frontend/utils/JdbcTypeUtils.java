package com.sqlrec.frontend.utils;

import java.sql.Types;
import java.util.Locale;
import java.util.regex.Pattern;

/** SQL type conversions used by JDBC column metadata and GetTypeInfo. */
public final class JdbcTypeUtils {
    private static final Pattern NOT_NULL_SUFFIX = Pattern.compile("(?i)\\s+NOT\\s+NULL$");
    private static final Pattern TYPE_PRECISION = Pattern.compile("(?i)^[A-Z_]+\\s*\\(\\s*(\\d+)");
    private static final Pattern DECIMAL_SCALE = Pattern.compile("(?i)^DECIMAL\\s*\\(\\s*\\d+\\s*,\\s*(\\d+)");

    private JdbcTypeUtils() {
    }

    public static ColumnType parse(String type) {
        String name = type.strip();
        var notNull = NOT_NULL_SUFFIX.matcher(name);
        boolean nullable = !notNull.find();
        if (!nullable) {
            name = name.substring(0, notNull.start());
        }
        String base = name.toUpperCase(Locale.ROOT).split("[<(\\s]", 2)[0];
        var match = TYPE_PRECISION.matcher(name);
        Integer declaredPrecision = match.find() ? Integer.valueOf(match.group(1)) : null;
        return new ColumnType(name, jdbcType(base), precision(base, declaredPrecision),
                scale(name, base, declaredPrecision), nullable);
    }

    private static int jdbcType(String base) {
        return switch (base) {
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

    private static int precision(String base, Integer declaredPrecision) {
        if (base.equals("TIME") || base.equals("TIMESTAMP") || base.equals("TIMESTAMP_LTZ")) {
            int fraction = fractionalPrecision(base, declaredPrecision);
            return (base.equals("TIME") ? 8 : 19) + (fraction == 0 ? 0 : fraction + 1);
        }
        if (declaredPrecision != null) {
            return declaredPrecision;
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

    private static Integer scale(String name, String base, Integer declaredPrecision) {
        return switch (base) {
            case "TIME", "TIMESTAMP", "TIMESTAMP_LTZ" -> fractionalPrecision(base, declaredPrecision);
            case "DECIMAL" -> {
                var match = DECIMAL_SCALE.matcher(name);
                yield match.find() ? Integer.valueOf(match.group(1)) : 0;
            }
            case "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT" -> 0;
            default -> null;
        };
    }

    private static int fractionalPrecision(String base, Integer declaredPrecision) {
        return declaredPrecision != null ? declaredPrecision : base.equals("TIME") ? 0 : 9;
    }

    public static Integer numericRadix(int jdbcType) {
        return switch (jdbcType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                    Types.FLOAT, Types.DOUBLE, Types.DECIMAL -> 10;
            default -> null;
        };
    }

    public record ColumnType(String name, int jdbcType, int precision, Integer scale, boolean nullable) {
    }
}
