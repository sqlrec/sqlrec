package com.sqlrec.common.utils;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class FlinkSchemaUtils {
    public static List<FieldSchema> getFieldSchemas(ResolvedSchema schema) {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        for (Column col : schema.getColumns()) {
            fieldSchemas.add(new FieldSchema(col.getName(), col.getDataType().getLogicalType().getTypeRoot().name()));
        }
        return fieldSchemas;
    }

    public static String getPrimaryKey(ResolvedSchema schema) {
        Optional<UniqueConstraint> uniqueConstraint = schema.getPrimaryKey();
        if (uniqueConstraint.isPresent()) {
            List<String> primaryKeys = uniqueConstraint.get().getColumns();
            if (primaryKeys.size() != 1) {
                throw new IllegalArgumentException("table must have only one primary key");
            }
            return primaryKeys.get(0);
        }
        throw new IllegalArgumentException("table must have primary key");
    }
}
