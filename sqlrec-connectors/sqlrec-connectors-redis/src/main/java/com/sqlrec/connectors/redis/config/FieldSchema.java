package com.sqlrec.connectors.redis.config;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.util.ArrayList;
import java.util.List;

public class FieldSchema {
    public String name;
    public String type;

    public FieldSchema(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public static List<FieldSchema> parse(ResolvedSchema tableSchema) {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        for (Column col : tableSchema.getColumns()) {
            fieldSchemas.add(new FieldSchema(col.getName(), col.getDataType().getLogicalType().getTypeRoot().name()));
        }
        return fieldSchemas;
    }

    public static List<FieldSchema> parse(List<org.apache.hadoop.hive.metastore.api.FieldSchema> fieldSchemas) {
        List<FieldSchema> fieldSchemaList = new ArrayList<>();
        for (org.apache.hadoop.hive.metastore.api.FieldSchema fieldSchema : fieldSchemas) {
            fieldSchemaList.add(new FieldSchema(fieldSchema.getName(), fieldSchema.getType()));
        }
        return fieldSchemaList;
    }
}
