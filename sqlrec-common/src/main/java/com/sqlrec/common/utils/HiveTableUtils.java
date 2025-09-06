package com.sqlrec.common.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;

public class HiveTableUtils {
    public static String getTableConnector(org.apache.hadoop.hive.metastore.api.Table tableObj){
        Map<String, String> tableProperties = tableObj.getParameters();
        if (tableProperties == null) {
            return null;
        }
        return tableProperties.get("flink.connector");
    }

    public static List<FieldSchema> parse(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        List<FieldSchema> fieldSchemaList = new ArrayList<>();
        List<org.apache.hadoop.hive.metastore.api.FieldSchema> fieldSchemas = tableObj.getSd().getCols();
        if (fieldSchemas!=null && !fieldSchemas.isEmpty()){
            for (org.apache.hadoop.hive.metastore.api.FieldSchema fieldSchema : fieldSchemas) {
                fieldSchemaList.add(new FieldSchema(fieldSchema.getName(), fieldSchema.getType()));
            }
        } else {
            Map<String, String> flinkTableColumns = getFlinkTableColumns(tableObj);
            for (Map.Entry<String, String> entry : flinkTableColumns.entrySet()) {
                fieldSchemaList.add(new FieldSchema(entry.getKey(), entry.getValue()));
            }
        }
        return fieldSchemaList;
    }

    public static Map<String, String> getFlinkTableOptions(org.apache.hadoop.hive.metastore.api.Table tableObj){
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

    public static Map<String, String> getFlinkTableColumns(org.apache.hadoop.hive.metastore.api.Table tableObj){
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

    public static String convertHiveType(String hiveType) {
        if (hiveType.contains("VARCHAR")) {
            return "VARCHAR";
        }
        return hiveType;
    }
}
