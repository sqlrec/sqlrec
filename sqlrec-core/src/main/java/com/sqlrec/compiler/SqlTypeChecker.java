package com.sqlrec.compiler;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.sql.parser.SqlCache;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.utils.NodeUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SqlTypeChecker {
    private static final Logger log = LoggerFactory.getLogger(SqlTypeChecker.class);

    public static boolean isFlinkSqlCompilable(SqlNode flinkSqlNode, CalciteSchema schema, String defaultSchema) {
        if (flinkSqlNode instanceof SqlCallSqlFunction) {
            return true;
        }
        if (flinkSqlNode instanceof SqlCache) {
            if (((SqlCache) flinkSqlNode).getSelect() != null) {
                return isSqlTableRunnable(((SqlCache) flinkSqlNode).getSelect(), schema, defaultSchema);
            }
            return true;
        }
        if (flinkSqlNode instanceof SqlSet) {
            return true;
        }
        if (!isCrudSql(flinkSqlNode)) {
            return false;
        }
        return isSqlTableRunnable(flinkSqlNode, schema, defaultSchema);
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
        if (flinkSqlNode instanceof SqlOrderBy) {
            return true;
        }

        if (isUnionSql(flinkSqlNode)) {
            return true;
        }

        return false;
    }

    public static boolean isUnionSql(SqlNode flinkSqlNode) {
        if (flinkSqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) flinkSqlNode;
            if (sqlBasicCall.getOperator() instanceof SqlSetOperator) {
                SqlSetOperator sqlSetOperator = (SqlSetOperator) sqlBasicCall.getOperator();
                if (sqlSetOperator.getKind() == SqlKind.UNION) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isSqlContainKvTable(SqlNode flinkSqlNode, CalciteSchema schema, String defaultSchema) {
        List<String> tableNames = NodeUtils.getTableFromSqlNode(flinkSqlNode);
        for (String tableName : tableNames) {
            Table table = getTableObj(schema, defaultSchema, tableName);
            if (table == null) {
                throw new RuntimeException("table " + tableName + " is not found");
            }
            if (table instanceof SqlRecKvTable) {
                return true;
            }
        }
        return false;
    }

    public static boolean isSqlTableRunnable(SqlNode flinkSqlNode, CalciteSchema schema, String defaultSchema) {
        List<String> tableNames = NodeUtils.getTableFromSqlNode(flinkSqlNode);
        for (String tableName : tableNames) {
            Table table = getTableObj(schema, defaultSchema, tableName);
            if (table == null) {
                log.error("table {} is not found", tableName);
                return false;
            }
            if (!(table instanceof SqlRecTable)) {
                return false;
            }
        }
        return true;
    }

    private static Table getTableObj(CalciteSchema schema, String defaultSchema, String tableName) {
        if (tableName.contains(".")) {
            String[] tableNameParts = tableName.split("\\.");
            if (tableNameParts.length != 2) {
                log.error("table name {} is not in format schema.table", tableName);
                return null;
            }
            String schemaName = tableNameParts[0];
            String shortTableName = tableNameParts[1];
            CalciteSchema subSchema = schema.getSubSchema(schemaName, false);
            return SchemaUtils.getTableObj(subSchema, shortTableName);
        } else {
            Table table = SchemaUtils.getTableObj(schema, tableName);
            if (table == null) {
                CalciteSchema subSchema = schema.getSubSchema(defaultSchema, false);
                table = SchemaUtils.getTableObj(subSchema, tableName);
            }
            return table;
        }
    }
}
