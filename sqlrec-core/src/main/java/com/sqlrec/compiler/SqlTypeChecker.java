package com.sqlrec.compiler;

import com.sqlrec.schema.SqlRecTable;
import com.sqlrec.sql.parser.*;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

public class SqlTypeChecker {
    public static boolean isFlinkSqlCompilable(SqlNode flinkSqlNode, CalciteSchema schema) {
        if (flinkSqlNode instanceof SqlCallSqlFunction) {
            return true;
        }
        if (flinkSqlNode instanceof SqlCache) {
            return true;
        }

        if (!isCrudSql(flinkSqlNode)) {
            return false;
        }
        return isSqlTableRunable(flinkSqlNode, schema);
    }

    private static boolean isCrudSql(SqlNode flinkSqlNode) {
        if (flinkSqlNode instanceof SqlSelect) {
            return true;
        }
        if (flinkSqlNode instanceof SqlInsert) {
            return true;
        }
        if (flinkSqlNode instanceof SqlUpdate) {
            return true;
        }
        if (flinkSqlNode instanceof SqlDelete) {
            return true;
        }

        return false;
    }

    public static boolean isSqlTableRunable(SqlNode flinkSqlNode, CalciteSchema schema) {
        List<String> tableNames = getTableFromSqlNode(flinkSqlNode);
        if (tableNames.isEmpty()) {
            return false;
        }
        for (String tableName : tableNames) {
            if (tableName.contains(".")) {
                String[] tableNameParts = tableName.split("\\.");
                if (tableNameParts.length != 2) {
                    return false;
                }
                String schemaName = tableNameParts[0];
                String shortTableName = tableNameParts[1];
                CalciteSchema subSchema = schema.getSubSchema(schemaName, false);
                if (subSchema == null) {
                    return false;
                }
                if (!checkIsTableRunable(subSchema, shortTableName)) {
                    return false;
                }
            } else {
                if (!checkIsTableRunable(schema, tableName)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean checkIsTableRunable(CalciteSchema schema, String tableName) {
        CalciteSchema.TableEntry tableEntry = schema.getTable(tableName, false);
        if (tableEntry == null) {
            return false;
        }
        Table table = tableEntry.getTable();
        if (table == null) {
            return false;
        }
        if (table instanceof SqlRecTable) {
            SqlRecTable sqlRecTable = (SqlRecTable) table;
            switch (sqlRecTable.getSqlRecTableType()) {
                case MEMORY:
                case KV:
                case MQ:
                case VECTOR:
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    public static List<String> getTableFromSqlNode(SqlNode flinkSqlNode) {
        List<String> tableNames = new ArrayList<>();
        if (flinkSqlNode == null) {
            return tableNames;
        }

        class TableNameVisitor extends SqlBasicVisitor<Void> {
            @Override
            public Void visit(SqlCall call) {
                tryGetTable(call, tableNames);
                return super.visit(call);
            }
        }

        TableNameVisitor visitor = new TableNameVisitor();
        flinkSqlNode.accept(visitor);

        return tableNames;
    }

    public static void tryGetTable(SqlNode sqlNode, List<String> tableNames) {
        if (sqlNode == null) {
            return;
        }

        if (sqlNode instanceof SqlInsert) {
            SqlInsert insertSql = (SqlInsert) sqlNode;
            tryGetTableNameFromSqlNode(insertSql.getTargetTable(), tableNames);
        } else if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            tryGetTableNameFromSqlNode(sqlSelect.getFrom(), tableNames);
        } else if (sqlNode instanceof SqlUpdate) {
            SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;
            tryGetTableNameFromSqlNode(sqlUpdate.getTargetTable(), tableNames);
        } else if (sqlNode instanceof SqlDelete) {
            SqlDelete sqlDelete = (SqlDelete) sqlNode;
            tryGetTableNameFromSqlNode(sqlDelete.getTargetTable(), tableNames);
        } else if (sqlNode instanceof SqlJoin) {
            SqlJoin sqlKind = (SqlJoin) sqlNode;
            tryGetTableNameFromSqlNode(sqlKind.getLeft(), tableNames);
            tryGetTableNameFromSqlNode(sqlKind.getRight(), tableNames);
        }
    }

    private static void tryGetTableNameFromSqlNode(SqlNode sqlNode, List<String> tableNames) {
        if (sqlNode instanceof SqlIdentifier) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
            String tableName = sqlIdentifier.toString();
            tableNames.add(tableName);
        }
    }
}
