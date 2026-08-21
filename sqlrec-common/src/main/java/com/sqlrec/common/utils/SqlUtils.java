package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

/**
 * The single place where SQL text is assembled. Every public method returns a
 * {@link SqlStatement}, whose SQL text contains only dialect-quoted identifiers
 * (see {@link #quoteIdentifier(String, String)}) and {@code '?'} placeholders; literal
 * values always travel as bind parameters. {@link SqlStatement} can only be constructed
 * inside this package, so code outside it cannot produce SQL with inlined values.
 */
public class SqlUtils {

    /**
     * Build a SELECT for the whole table, optionally with a WHERE clause translated from
     * Calcite filter conditions (see {@link FilterUtils#buildSqlFilter}).
     */
    public static SqlStatement select(String url, String tableName, List<FieldSchema> fieldSchemas, List<RexNode> filters) {
        SqlStatement where = FilterUtils.buildSqlFilter(filters, fieldSchemas, url);
        StringBuilder sql = new StringBuilder("SELECT ");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(" FROM ").append(quoteIdentifier(tableName, url));
        if (!where.getSql().isEmpty()) {
            sql.append(" WHERE ").append(where.getSql());
        }
        return new SqlStatement(sql.toString(), where.getParameters());
    }

    /**
     * Build a SELECT of rows whose primary key is in the given key set.
     * The returned statement carries {@code keyCount} unbound placeholders; the caller
     * must supply the key values via {@link SqlStatement#withParameters(List)}.
     */
    public static SqlStatement selectByPrimaryKey(String url, String tableName, List<FieldSchema> fieldSchemas,
                                                  String primaryKey, int keyCount) {
        if (keyCount <= 0) {
            throw new IllegalArgumentException("keyCount must be positive: " + keyCount);
        }
        StringBuilder sql = new StringBuilder("SELECT ");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(" FROM ").append(quoteIdentifier(tableName, url));
        sql.append(" WHERE ").append(quoteIdentifier(primaryKey, url)).append(" IN (");
        appendPlaceholders(sql, keyCount);
        sql.append(")");
        return new SqlStatement(sql.toString(), Collections.emptyList());
    }

    /**
     * Build an upsert statement (INSERT ... ON CONFLICT/ON DUPLICATE KEY or MERGE INTO,
     * depending on the dialect inferred from the JDBC url). The returned statement is a
     * row template: the caller supplies one value per column via
     * {@link SqlStatement#withParameters(List)}.
     */
    public static SqlStatement upsert(String url, String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        String lowerUrl = url.toLowerCase();
        if (lowerUrl.startsWith("jdbc:mysql:")) {
            return mysqlUpsert(tableName, fieldSchemas, primaryKey);
        }
        if (lowerUrl.startsWith("jdbc:h2:")) {
            return h2Upsert(tableName, fieldSchemas, primaryKey);
        }
        // PostgreSQL
        return postgresUpsert(tableName, fieldSchemas, primaryKey);
    }

    /** PostgreSQL uses ANSI double-quoted identifiers. */
    private static SqlStatement postgresUpsert(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        String url = null;
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(quoteIdentifier(tableName, url)).append(" (");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(") ON CONFLICT (").append(quoteIdentifier(primaryKey, url)).append(") DO UPDATE SET ");
        appendExcludedSet(sql, fieldSchemas, url);
        return new SqlStatement(sql.toString(), Collections.emptyList());
    }

    /** MySQL uses backtick-quoted identifiers. */
    private static SqlStatement mysqlUpsert(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        String url = "jdbc:mysql:";
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(quoteIdentifier(tableName, url)).append(" (");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(") ON DUPLICATE KEY UPDATE ");
        appendValuesSet(sql, fieldSchemas, url);
        return new SqlStatement(sql.toString(), Collections.emptyList());
    }

    /** H2 uses ANSI double-quoted identifiers. */
    private static SqlStatement h2Upsert(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        String url = "jdbc:h2:";
        StringBuilder sql = new StringBuilder("MERGE INTO ");
        sql.append(quoteIdentifier(tableName, url)).append(" KEY (");
        sql.append(quoteIdentifier(primaryKey, url)).append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(")");
        return new SqlStatement(sql.toString(), Collections.emptyList());
    }

    /**
     * Build a delete-by-primary-key statement. The returned statement is a row template:
     * the caller supplies the key value via {@link SqlStatement#withParameters(List)}.
     */
    public static SqlStatement deleteByPrimaryKey(String url, String tableName, String primaryKey) {
        String sql = "DELETE FROM " + quoteIdentifier(tableName, url)
                + " WHERE " + quoteIdentifier(primaryKey, url) + " = ?";
        return new SqlStatement(sql, Collections.emptyList());
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
     * This is the only place raw identifiers may enter SQL text. Safe identifiers (matching
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
