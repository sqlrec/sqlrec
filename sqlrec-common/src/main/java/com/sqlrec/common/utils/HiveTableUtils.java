package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.ConnectorTableMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;

import java.util.*;

public class HiveTableUtils {
    private static final Logger log = LoggerFactory.getLogger(HiveTableUtils.class);

    /** Reads execution metadata for connectors that require a single-column primary key. */
    public static ConnectorTableMetadata getConnectorTableMetadata(
            org.apache.hadoop.hive.metastore.api.Table table) {
        Map<String, String> options = getFlinkTableOptions(table);
        List<FieldSchema> fields = parse(table);
        String primaryKey = getTablePrimaryKey(table);
        return new ConnectorTableMetadata(table.getDbName(), table.getTableName(), options,
                fields, primaryKey, getTablePrimaryKeyIndex(fields, primaryKey));
    }

    public static String getTableConnector(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> tableProperties = tableObj.getParameters();
        if (tableProperties == null) {
            return null;
        }
        return tableProperties.get("flink.connector");
    }

    public static List<FieldSchema> parse(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        List<FieldSchema> fieldSchemaList = new ArrayList<>();
        List<org.apache.hadoop.hive.metastore.api.FieldSchema> fieldSchemas = tableObj.getSd().getCols();
        if (fieldSchemas != null && !fieldSchemas.isEmpty()) {
            for (org.apache.hadoop.hive.metastore.api.FieldSchema fieldSchema : fieldSchemas) {
                fieldSchemaList.add(new FieldSchema(fieldSchema.getName(), fieldSchema.getType()));
            }
        } else {
            Map<String, String> flinkTableColumns = getFlinkTableColumns(tableObj);
            for (Map.Entry<String, String> entry : flinkTableColumns.entrySet()) {
                fieldSchemaList.add(new FieldSchema(entry.getKey(), entry.getValue()));
            }
        }

        if (fieldSchemaList.isEmpty()) {
            throw new IllegalArgumentException("Table " + tableObj.getTableName() + " has no columns");
        }

        return fieldSchemaList;
    }

    public static Map<String, String> getFlinkTableOptions(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> flinkTableOptions = new LinkedHashMap<>();
        Map<String, String> tableProperties = tableObj.getParameters();

        if (tableProperties != null) {
            for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
                if (entry.getKey().startsWith("flink.")) {
                    String key = entry.getKey().substring(6);
                    flinkTableOptions.put(key, entry.getValue());
                }
            }
        }

        if (flinkTableOptions.containsKey("schema.0.name")) {
            Map<String, String> options = new LinkedHashMap<>(
                    CatalogPropertiesUtil.deserializeCatalogTable(flinkTableOptions).getOptions());
            options.remove("is_generic");
            return options;
        }
        flinkTableOptions.keySet().removeIf(key -> key.startsWith("schema.") || key.startsWith("partition.keys.")
                || key.equals("comment") || key.equals("snapshot") || key.equals("is_generic"));
        return flinkTableOptions;
    }

    /** Fields for metadata display, preserving Flink's serialized types and nullability. */
    public static List<FieldSchema> getMetadataFields(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> properties = tableObj.getParameters();
        if (properties != null && properties.containsKey("flink.schema.0.name")) {
            List<FieldSchema> fields = new ArrayList<>();
            for (int i = 0; properties.containsKey("flink.schema." + i + ".name"); i++) {
                String prefix = "flink.schema." + i + ".";
                // Keep nullability and nested field names exactly as serialized by Flink.
                String type = properties.get(prefix + "data-type");
                if (type == null) {
                    throw new IllegalArgumentException("Missing Flink type for " + properties.get(prefix + "name"));
                }
                fields.add(new FieldSchema(properties.get(prefix + "name"), type));
            }
            return fields;
        }
        List<FieldSchema> result = new ArrayList<>();
        if (tableObj.getSd() != null && tableObj.getSd().getCols() != null) {
            tableObj.getSd().getCols().forEach(c -> result.add(new FieldSchema(c.getName(), c.getType())));
        }
        if (tableObj.getPartitionKeys() != null) {
            tableObj.getPartitionKeys().forEach(c -> result.add(new FieldSchema(c.getName(), c.getType())));
        }
        return result;
    }

    public static Map<String, String> getFlinkTableColumns(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> flinkTableColumns = new LinkedHashMap<>();
        Map<String, String> tableProperties = tableObj.getParameters();

        if (tableProperties == null) {
            return flinkTableColumns;
        }
        for (int index = 0; ; index++) {
            String nameKey = "flink.schema." + index + ".name";
            String typeKey = "flink.schema." + index + ".data-type";
            if (!tableProperties.containsKey(nameKey) || !tableProperties.containsKey(typeKey)) {
                break;
            }
            String columnName = tableProperties.get(nameKey);
            String columnType = convertHiveType(tableProperties.get(typeKey));
            flinkTableColumns.put(columnName, columnType);
        }

        return flinkTableColumns;
    }

    //todo cover more type case
    public static String convertHiveType(String hiveType) {
        hiveType = hiveType.toUpperCase();
        if (hiveType.contains("NOT NULL")) {
            return hiveType.replace("NOT NULL", "").trim();
        }
        return hiveType;
    }

    public static String getTablePrimaryKey(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> tableProperties = tableObj.getParameters();
        if (tableProperties == null) {
            throw new IllegalArgumentException("Table " + tableObj.getTableName() + " has no parameters (cannot resolve primary key)");
        }
        String primaryKey = tableProperties.get("flink.schema.primary-key.columns");
        if (StringUtils.isEmpty(primaryKey)) {
            throw new IllegalArgumentException("Table " + tableObj.getTableName() + " has no primary key");
        }
        if (primaryKey.contains(",")) {
            throw new IllegalArgumentException("Table " + tableObj.getTableName() + " primary key must be single column");
        }
        return primaryKey;
    }

    public static int getTablePrimaryKeyIndex(List<FieldSchema> fieldSchemas, String primaryKey) {
        for (int i = 0; i < fieldSchemas.size(); i++) {
            if (fieldSchemas.get(i).getName().equalsIgnoreCase(primaryKey)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Table primary key " + primaryKey + " not found");
    }

    public static long getTableModificationTime(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> tableProperties = tableObj.getParameters();
        if (tableProperties == null) {
            log.warn("Table {} has no parameters", tableObj.getTableName());
            return 0;
        }
        String lastModificationTime = tableProperties.get("transient_lastDdlTime");
        if (StringUtils.isEmpty(lastModificationTime)) {
            log.warn("Table {} has no last modification time", tableObj.getTableName());
            return 0;
        }
        try {
            return Long.parseLong(lastModificationTime) * 1000;
        } catch (NumberFormatException e) {
            log.warn("Table {} last modification time {} is not a valid long", tableObj.getTableName(), lastModificationTime);
            return 0;
        }
    }

    public static Map.Entry<String, String> getDbAndTable(String tableName) {
        int index = tableName.indexOf(".");
        if (index == -1) {
            return new AbstractMap.SimpleEntry<>("default", tableName);
        }
        return new AbstractMap.SimpleEntry<>(tableName.substring(0, index), tableName.substring(index + 1));
    }
}
