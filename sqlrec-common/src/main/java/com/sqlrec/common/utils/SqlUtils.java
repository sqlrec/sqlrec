package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class SqlUtils {

    public static String buildSelectSql(String url, String tableName, List<FieldSchema> fieldSchemas, String whereClause) {
        StringBuilder sql = new StringBuilder("SELECT ");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(" FROM ").append(quoteIdentifier(tableName, url));
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        return sql.toString();
    }

    public static String buildWhereClause(List<RexNode> filters, List<FieldSchema> fieldSchemas) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }
        return FilterUtils.getSqlFilterString(filters, fieldSchemas);
    }

    public static String buildUpsertSql(String url, String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        String lowerUrl = url.toLowerCase();
        if (lowerUrl.startsWith("jdbc:mysql:")) {
            return buildMySqlUpsertSql(tableName, fieldSchemas, primaryKey);
        }
        if (lowerUrl.startsWith("jdbc:h2:")) {
            return buildH2UpsertSql(tableName, fieldSchemas, primaryKey);
        }
        // PostgreSQL
        return buildPostgreSqlUpsertSql(tableName, fieldSchemas, primaryKey);
    }

    public static String buildPostgreSqlUpsertSql(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        // PostgreSQL uses ANSI double-quoted identifiers.
        String url = null;
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(quoteIdentifier(tableName, url)).append(" (");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(") ON CONFLICT (").append(quoteIdentifier(primaryKey, url)).append(") DO UPDATE SET ");
        appendExcludedSet(sql, fieldSchemas, url);
        return sql.toString();
    }

    public static String buildMySqlUpsertSql(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        // MySQL uses backtick-quoted identifiers.
        String url = "jdbc:mysql:";
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(quoteIdentifier(tableName, url)).append(" (");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(") ON DUPLICATE KEY UPDATE ");
        appendValuesSet(sql, fieldSchemas, url);
        return sql.toString();
    }

    public static String buildH2UpsertSql(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        // H2 uses ANSI double-quoted identifiers.
        String url = "jdbc:h2:";
        StringBuilder sql = new StringBuilder("MERGE INTO ");
        sql.append(quoteIdentifier(tableName, url)).append(" KEY (");
        sql.append(quoteIdentifier(primaryKey, url)).append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(")");
        return sql.toString();
    }

    public static String buildDeleteSql(String url, String tableName, String primaryKey) {
        return "DELETE FROM " + quoteIdentifier(tableName, url)
                + " WHERE " + quoteIdentifier(primaryKey, url) + " = ?";
    }

    private static void appendColumnNames(StringBuilder sql, List<FieldSchema> fieldSchemas, String url) {
        for (int i = 0; i < fieldSchemas.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(fieldSchemas.get(i).getName(), url));
        }
    }

    private static void appendPlaceholders(StringBuilder sql, int count) {
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }
    }

    private static void appendExcludedSet(StringBuilder sql, List<FieldSchema> fieldSchemas, String url) {
        for (int i = 0; i < fieldSchemas.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String colName = quoteIdentifier(fieldSchemas.get(i).getName(), url);
            sql.append(colName).append(" = EXCLUDED.").append(colName);
        }
    }

    private static void appendValuesSet(StringBuilder sql, List<FieldSchema> fieldSchemas, String url) {
        for (int i = 0; i < fieldSchemas.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String colName = quoteIdentifier(fieldSchemas.get(i).getName(), url);
            sql.append(colName).append(" = VALUES(").append(colName).append(")");
        }
    }

    /**
     * Quote a SQL identifier (table/column name) according to the dialect inferred from the JDBC url.
     * <p>
     * This prevents identifier injection from untrusted table metadata. Safe identifiers (matching
     * {@code [a-zA-Z_][a-zA-Z0-9_]*}) are returned as-is to preserve the database's default
     * case-folding behavior and avoid breaking existing schemas. Unsafe identifiers (containing
     * special characters, spaces, quotes, etc.) are quoted to prevent SQL injection and syntax
     * errors. MySQL uses backticks; PostgreSQL/H2 and the default use ANSI double quotes. The
     * corresponding quote character is escaped by doubling.
     */
    public static String quoteIdentifier(String identifier, String url) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        if (isSafeIdentifier(identifier)) {
            return identifier;
        }
        boolean mySql = url != null && url.toLowerCase().startsWith("jdbc:mysql:");
        if (mySql) {
            return "`" + identifier.replace("`", "``") + "`";
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Returns true if the identifier consists only of {@code [a-zA-Z_][a-zA-Z0-9_]*} and therefore
     * does not need quoting (it cannot break out of an identifier context or inject SQL).
     */
    private static boolean isSafeIdentifier(String identifier) {
        char first = identifier.charAt(0);
        if (!Character.isLetter(first) && first != '_') {
            return false;
        }
        for (int i = 1; i < identifier.length(); i++) {
            char c = identifier.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') {
                return false;
            }
        }
        return true;
    }
}
