package com.sqlrec.common.utils;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class HiveTableUtilsTest {
    @Test
    void flinkFallbackPreservesFieldNamesAndNullability() {
        String declaration = "ARRAY<VARCHAR(12) NOT NULL> NOT NULL";
        Table table = new Table();
        table.setSd(new StorageDescriptor());
        table.putToParameters("flink.schema.0.name", "payload");
        table.putToParameters("flink.schema.0.data-type", declaration);

        var field = HiveTableUtils.parse(table).get(0);
        assertEquals(declaration, field.getType());
        assertEquals("payload", field.getName());
        var arrayType = DataTypeUtils.getRelDataType(field.getType());
        assertFalse(arrayType.isNullable());
        assertFalse(arrayType.getComponentType().isNullable());
    }
}
