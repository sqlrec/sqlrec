package com.sqlrec.common.utils;

import com.sqlrec.common.schema.ConnectorTableMetadata;
import com.sqlrec.common.schema.FieldSchema;
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
        return new ConnectorTableMetadata(table.getDbName(), table.getTableName(), options,
                parse(table), getTablePrimaryKey(table));
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
            Map<String, FieldSchema> fieldsByName = new LinkedHashMap<>();
            for (FieldSchema field : readFlinkFields(tableObj.getParameters(), false)) {
                fieldsByName.put(field.getName(), field);
            }
            fieldSchemaList.addAll(fieldsByName.values());
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
            return readFlinkFields(properties, true);
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

    private static List<FieldSchema> readFlinkFields(Map<String, String> properties, boolean requireTypes) {
        List<FieldSchema> fields = new ArrayList<>();
        if (properties == null) {
            return fields;
        }
        for (int i = 0; ; i++) {
            String prefix = "flink.schema." + i + ".";
            if (!properties.containsKey(prefix + "name")) {
                break;
            }
            String name = properties.get(prefix + "name");
            String typeKey = prefix + "data-type";
            String type = properties.get(typeKey);
            if (requireTypes && type == null) {
                throw new IllegalArgumentException("Missing Flink type for " + name);
            }
            if (!properties.containsKey(typeKey)) {
                break;
            }
            fields.add(new FieldSchema(name, type));
        }
        return fields;
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
