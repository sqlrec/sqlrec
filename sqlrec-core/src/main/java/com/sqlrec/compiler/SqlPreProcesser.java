package com.sqlrec.compiler;

import org.apache.commons.lang3.StringUtils;

public class SqlPreProcesser {
    public static final String USE_DEFAULT = "use default";
    public static final String USE_DEFAULT_FORMATTED = "use `default`";
    public static final String SHOW_TABLES_FROM_DEFAULT = "show tables from default";
    public static final String SHOW_TABLES_FROM_DEFAULT_FORMATTED = "show tables from `default`";
    public static final String SHOW_TABLES_IN_DEFAULT = "show tables in default";
    public static final String SHOW_TABLES_IN_DEFAULT_FORMATTED = "show tables in `default`";

    public static String preProcessSql(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return sql;
        }

        if (StringUtils.deleteWhitespace(sql).equals(StringUtils.deleteWhitespace(USE_DEFAULT))) {
            return USE_DEFAULT_FORMATTED;
        }
        if (StringUtils.deleteWhitespace(sql).equals(StringUtils.deleteWhitespace(SHOW_TABLES_FROM_DEFAULT))) {
            return SHOW_TABLES_FROM_DEFAULT_FORMATTED;
        }
        if (StringUtils.deleteWhitespace(sql).equals(StringUtils.deleteWhitespace(SHOW_TABLES_IN_DEFAULT))) {
            return SHOW_TABLES_IN_DEFAULT_FORMATTED;
        }

        if (isSetStatement(sql)) {
            return transformSetStatement(sql);
        }

        return sql;
    }

    public static boolean isSetStatement(String sql) {
        return sql.trim().toLowerCase().startsWith("set ");
    }

    // transform from set param=test to set 'param'='test'
    public static String transformSetStatement(String sql) {
        if (sql.contains("'")) {
            return sql;
        }
        if (StringUtils.countMatches(sql, "=") != 1) {
            return sql;
        }

        sql = sql.trim();

        // Extract the part after "set "
        String content = sql.substring(4).trim();

        // Find the first occurrence of '='
        int equalIndex = content.indexOf('=');
        String paramName = content.substring(0, equalIndex).trim();
        String paramValue = content.substring(equalIndex + 1).trim();

        // Return the formatted SQL
        return "set '" + paramName + "'='" + paramValue + "'";
    }
}
