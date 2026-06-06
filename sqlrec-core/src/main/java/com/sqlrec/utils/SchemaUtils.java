package com.sqlrec.utils;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SchemaUtils {
    private static final Logger log = LoggerFactory.getLogger(SchemaUtils.class);

    public static String getValueOfStringLiteral(SqlCharStringLiteral value) {
        if (value == null) {
            return null;
        }
        String valueStr = value.toString();
        return removeQuotes(valueStr);
    }

    public static String getValueOfStringLiteral(SqlNode value) {
        if (value == null) {
            return null;
        }
        if (value instanceof SqlCharStringLiteral) {
            return getValueOfStringLiteral((SqlCharStringLiteral) value);
        }
        throw new RuntimeException("value is not a string literal: " + value);
    }

    public static String removeQuotes(String value) {
        if (value == null) {
            return null;
        }
        if ((value.startsWith("'") && value.endsWith("'")) ||
                (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    public static List<FieldSchema> convertFieldList(SqlNodeList fieldList) {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        if (fieldList != null && fieldList.size() > 0) {
            for (SqlNode field : fieldList) {
                FieldSchema fieldSchema = convertField(field);
                if (fieldSchema != null) {
                    fieldSchemas.add(fieldSchema);
                }
            }
        }
        return fieldSchemas;
    }

    public static FieldSchema convertField(SqlNode field) {
        if (field == null) {
            return null;
        }

        if (field instanceof SqlTableColumn.SqlRegularColumn) {
            SqlTableColumn.SqlRegularColumn regularColumn = (SqlTableColumn.SqlRegularColumn) field;
            String name = regularColumn.getName().toString();
            String type = regularColumn.getType().toString();
            return new FieldSchema(name, type);
        } else {
            log.warn("Unsupported field type: {}", field);
            throw new IllegalArgumentException("Unsupported field type: " + field);
        }
    }

    public static Map<String, String> convertPropertyList(SqlNodeList propertyList) {
        Map<String, String> params = new HashMap<>();
        if (propertyList != null && propertyList.size() > 0) {
            for (SqlNode property : propertyList) {
                if (property instanceof SqlTableOption) {
                    SqlTableOption option = (SqlTableOption) property;
                    String key = removeQuotes(option.getKey().toString());
                    String value = removeQuotes(option.getValue().toString());
                    params.put(key, value);
                } else {
                    log.warn("Unsupported property type: {}", property);
                    throw new IllegalArgumentException("Unsupported property type: " + property);
                }
            }
        }
        return params;
    }

    public static SqlNodeList addConfigToPropertyList(SqlNodeList propertyList, String key, String value) {
        SqlTableOption option = new SqlTableOption(
                SqlLiteral.createCharString(key, SqlParserPos.ZERO),
                SqlLiteral.createCharString(value, SqlParserPos.ZERO),
                SqlParserPos.ZERO
        );
        propertyList.add(option);
        return propertyList;
    }

    public static List<RelDataTypeField> getDataTypeByLikeTableName(
            String likeTableName,
            CalciteSchema schema) {
        CalciteSchema.TableEntry tableEntry = schema.getTable(likeTableName, false);
        if (tableEntry == null) {
            throw new RuntimeException("like table not found: " + likeTableName);
        }
        Table table = tableEntry.getTable();
        if (!(table instanceof CacheTable)) {
            throw new RuntimeException("like table must be cache table for table function");
        }
        return ((CacheTable) table).getDataFields();
    }

    public static CacheTable getCacheTable(String inputTableName, CalciteSchema schema) {
        Table table = Objects.requireNonNull(
                schema.getTable(inputTableName, false),
                "input table not found: " + inputTableName
        ).getTable();
        if (!(table instanceof CacheTable)) {
            throw new RuntimeException("input table must be cache table for table function");
        }
        return (CacheTable) table;
    }

    public static CacheTable tryGetCacheTable(String inputTableName, CalciteSchema schema) {
        Table table = getTableObj(schema, inputTableName);
        if (table == null) {
            return null;
        }
        if (!(table instanceof CacheTable)) {
            return null;
        }
        return (CacheTable) table;
    }

    public static Table getTableObj(CalciteSchema schema, String shortTableName) {
        if (schema == null || shortTableName == null) {
            return null;
        }
        CalciteSchema.TableEntry tableEntry = schema.getTable(shortTableName, false);
        if (tableEntry == null) {
            return null;
        }
        return tableEntry.getTable();
    }

    public static Table getTableObj(CalciteSchema schema, String defaultSchema, String tableName) {
        tableName = tableName.replaceAll("`", "");
        if (tableName.contains(".")) {
            String[] tableNameParts = tableName.split("\\.");
            if (tableNameParts.length != 2) {
                log.error("table name {} is not in format schema.table", tableName);
                return null;
            }
            String schemaName = tableNameParts[0];
            String shortTableName = tableNameParts[1];
            CalciteSchema subSchema = schema.getSubSchema(schemaName, false);
            return getTableObj(subSchema, shortTableName);
        } else {
            Table table = getTableObj(schema, tableName);
            if (table == null) {
                CalciteSchema subSchema = schema.getSubSchema(defaultSchema, false);
                table = getTableObj(subSchema, tableName);
            }
            return table;
        }
    }

    public static String getSqlFirstWord(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return "";
        }
        String[] parts = sql.trim().split("\\s+");
        if (parts.length == 0) {
            return "";
        }
        String firstWord = parts[0].replaceAll("[^a-zA-Z0-9_]", "");
        return firstWord.toLowerCase();
    }

    /**
     * Generate CREATE TABLE DDL from an HMS Table object.
     * Reconstructs the full DDL including columns, primary key, and WITH properties.
     */
    public static String generateCreateSqlFromHmsTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
        String tableName = hmsTable.getDbName().equals("default")
                ? hmsTable.getTableName()
                : hmsTable.getDbName() + "." + hmsTable.getTableName();

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (");

        List<org.apache.hadoop.hive.metastore.api.FieldSchema> columns = hmsTable.getSd().getCols();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            org.apache.hadoop.hive.metastore.api.FieldSchema col = columns.get(i);
            sb.append(col.getName()).append(" ").append(col.getType());
        }

        Map<String, String> parameters = hmsTable.getParameters();
        String primaryKey = parameters != null ? parameters.get("flink.schema.primary-key.columns") : null;
        if (primaryKey != null && !primaryKey.isEmpty()) {
            sb.append(", PRIMARY KEY (").append(primaryKey).append(") NOT ENFORCED");
        }

        sb.append(")");

        if (parameters != null && !parameters.isEmpty()) {
            Map<String, String> flinkOptions = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                if (entry.getKey().startsWith("flink.") && !entry.getKey().equals("flink.schema.primary-key.columns")) {
                    flinkOptions.put(entry.getKey().substring(6), entry.getValue());
                }
            }
            if (!flinkOptions.isEmpty()) {
                sb.append(" WITH (");
                int i = 0;
                for (Map.Entry<String, String> entry : flinkOptions.entrySet()) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append("'").append(entry.getKey()).append("' = '").append(entry.getValue()).append("'");
                    i++;
                }
                sb.append(")");
            }
        }

        return sb.toString();
    }

    /**
     * Parse a SqlCreateTable node into an HMS Table object.
     */
    public static org.apache.hadoop.hive.metastore.api.Table parseCreateTableToHmsTable(SqlCreateTable createTable) {
        String database = extractDatabase(createTable.getTableName());
        String tableName = extractTableName(createTable.getTableName());
        List<org.apache.hadoop.hive.metastore.api.FieldSchema> columns = extractColumnsFromCreateTable(createTable);
        Map<String, String> properties = convertPropertyList(createTable.getPropertyList());
        String primaryKey = extractPrimaryKey(createTable);
        if (primaryKey != null) {
            properties.put("schema.primary-key.columns", primaryKey);
        }
        if (columns.isEmpty()) {
            return null;
        }
        return buildHmsTable(database, tableName, columns, properties);
    }

    private static List<org.apache.hadoop.hive.metastore.api.FieldSchema> extractColumnsFromCreateTable(SqlCreateTable createTable) {
        List<org.apache.hadoop.hive.metastore.api.FieldSchema> columns = new ArrayList<>();
        SqlNodeList columnList = createTable.getColumnList();
        if (columnList != null) {
            for (SqlNode field : columnList) {
                if (field instanceof SqlTableColumn.SqlRegularColumn) {
                    SqlTableColumn.SqlRegularColumn col = (SqlTableColumn.SqlRegularColumn) field;
                    String colName = col.getName().getSimple();
                    String typeStr = col.getType().toString();
                    columns.add(new org.apache.hadoop.hive.metastore.api.FieldSchema(colName, typeStr, null));
                }
            }
        }
        return columns;
    }

    private static String extractPrimaryKey(SqlCreateTable createTable) {
        for (SqlTableConstraint constraint : createTable.getFullConstraints()) {
            if (constraint.isPrimaryKey()) {
                return String.join(",", constraint.getColumnNames());
            }
        }
        return null;
    }

    private static org.apache.hadoop.hive.metastore.api.Table buildHmsTable(
            String database, String tableName,
            List<org.apache.hadoop.hive.metastore.api.FieldSchema> columns,
            Map<String, String> properties) {
        org.apache.hadoop.hive.metastore.api.Table table = new org.apache.hadoop.hive.metastore.api.Table();
        table.setDbName(database);
        table.setTableName(tableName);
        org.apache.hadoop.hive.metastore.api.StorageDescriptor sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor();
        sd.setCols(columns);
        table.setSd(sd);
        Map<String, String> parameters = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            parameters.put("flink." + entry.getKey(), entry.getValue());
        }
        table.setParameters(parameters);
        return table;
    }

    private static String extractDatabase(SqlIdentifier identifier) {
        if (identifier.names.size() > 1) {
            return identifier.names.get(0);
        }
        return "default";
    }

    private static String extractTableName(SqlIdentifier identifier) {
        if (identifier.names.size() > 1) {
            return identifier.names.get(identifier.names.size() - 1);
        }
        return identifier.getSimple();
    }
}
