package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HiveTableUtils {
    private static final Logger log = LoggerFactory.getLogger(HiveTableUtils.class);

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
                    flinkTableOptions.put(entry.getKey().substring(6), entry.getValue());
                }
            }
        }

        return flinkTableOptions;
    }

    public static Map<String, String> getFlinkTableColumns(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> flinkTableColumns = new LinkedHashMap<>();
        Map<String, String> tableProperties = tableObj.getParameters();

        if (tableProperties != null) {
            int index = 0;
            while (true) {
                String nameKey = "flink.schema." + index + ".name";
                String typeKey = "flink.schema." + index + ".data-type";
                if (tableProperties.containsKey(nameKey) && tableProperties.containsKey(typeKey)) {
                    String columnName = tableProperties.get(nameKey);
                    String columnType = tableProperties.get(typeKey);
                    columnType = convertHiveType(columnType);
                    flinkTableColumns.put(columnName, columnType);
                    index++;
                } else {
                    break;
                }
            }
        }

        return flinkTableColumns;
    }

    //todo cover more type case
    public static String convertHiveType(String hiveType) {
        hiveType = hiveType.toUpperCase();
        if (hiveType.startsWith("VARCHAR")) {
            return "VARCHAR";
        }
        if (hiveType.contains("NOT NULL")) {
            return hiveType.replace("NOT NULL", "").trim();
        }
        return hiveType;
    }

    public static String getTablePrimaryKey(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> tableProperties = tableObj.getParameters();
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
        String lastModificationTime = tableProperties.get("transient_lastDdlTime");;
        if (StringUtils.isEmpty(lastModificationTime)) {
            log.warn("Table {} has no last modification time", tableObj.getTableName());
            return 0;
        }
        return Long.parseLong(lastModificationTime) * 1000;
    }

    public static Map.Entry<String, String> getDbAndTable(String tableName) {
        int index = tableName.indexOf(".");
        if (index == -1) {
            return new AbstractMap.SimpleEntry<>("default", tableName);
        }
        return new AbstractMap.SimpleEntry<>(tableName.substring(0, index), tableName.substring(index + 1));
    }
}
