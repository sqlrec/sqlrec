package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class SqlUtils {

    public static String buildSelectSql(String tableName, List<FieldSchema> fieldSchemas, String whereClause) {
        StringBuilder sql = new StringBuilder("SELECT ");
        appendColumnNames(sql, fieldSchemas);
        sql.append(" FROM ").append(tableName);
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
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");
        appendColumnNames(sql, fieldSchemas);
        sql.append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(") ON CONFLICT (").append(primaryKey).append(") DO UPDATE SET ");
        appendExcludedSet(sql, fieldSchemas);
        return sql.toString();
    }

    public static String buildMySqlUpsertSql(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");
        appendColumnNames(sql, fieldSchemas);
        sql.append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(") ON DUPLICATE KEY UPDATE ");
        appendValuesSet(sql, fieldSchemas);
        return sql.toString();
    }

    public static String buildH2UpsertSql(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        StringBuilder sql = new StringBuilder("MERGE INTO ");
        sql.append(tableName).append(" KEY (");
        sql.append(primaryKey).append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(")");
        return sql.toString();
    }

    public static String buildDeleteSql(String tableName, String primaryKey) {
        return "DELETE FROM " + tableName
                + " WHERE " + primaryKey + " = ?";
    }

    private static void appendColumnNames(StringBuilder sql, List<FieldSchema> fieldSchemas) {
        for (int i = 0; i < fieldSchemas.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(fieldSchemas.get(i).getName());
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

    private static void appendExcludedSet(StringBuilder sql, List<FieldSchema> fieldSchemas) {
        for (int i = 0; i < fieldSchemas.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String colName = fieldSchemas.get(i).getName();
            sql.append(colName).append(" = EXCLUDED.").append(colName);
        }
    }

    private static void appendValuesSet(StringBuilder sql, List<FieldSchema> fieldSchemas) {
        for (int i = 0; i < fieldSchemas.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String colName = fieldSchemas.get(i).getName();
            sql.append(colName).append(" = VALUES(").append(colName).append(")");
        }
    }
}
