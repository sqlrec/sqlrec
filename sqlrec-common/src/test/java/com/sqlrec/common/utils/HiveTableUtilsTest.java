package com.sqlrec.common.utils;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void missingFlinkTypeStopsConnectorFallbackButFailsMetadataReading() {
        Table table = new Table();
        table.setSd(new StorageDescriptor());
        table.putToParameters("flink.schema.0.name", "id");
        table.putToParameters("flink.schema.0.data-type", "BIGINT");
        table.putToParameters("flink.schema.1.name", "partial");

        var fields = HiveTableUtils.parse(table);
        assertEquals(1, fields.size());
        assertEquals("id", fields.get(0).getName());
        var error = assertThrows(IllegalArgumentException.class, () -> HiveTableUtils.getMetadataFields(table));
        assertEquals("Missing Flink type for partial", error.getMessage());
    }
}
