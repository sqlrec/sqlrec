package com.sqlrec.common.schema;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.common.utils.HiveTableUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConnectorTableMetadataTest {
    @Test
    void snapshotsMutableFieldsAndOptionsWithoutSharingBackendConfiguration() {
        List<FieldSchema> fields = new ArrayList<>(Collections.singletonList(new FieldSchema("id", "BIGINT")));
        Map<String, String> options = new LinkedHashMap<>(Map.of("connector", "redis"));
        ConnectorTableMetadata metadata = new ConnectorTableMetadata("db", "items", options, fields, "id", 0);
        fields.get(0).setName("changed");
        fields.clear();
        options.clear();
        List<FieldSchema> backendFields = metadata.getFieldSchemas();
        backendFields.get(0).setType("STRING");
        backendFields.clear();

        assertEquals("id", metadata.getFieldSchemas().get(0).getName());
        assertEquals("BIGINT", metadata.getFieldSchemas().get(0).getType());
        assertEquals(Map.of("connector", "redis"), metadata.getOptions());
        assertThrows(UnsupportedOperationException.class, () -> metadata.getOptions().clear());
    }

    @Test
    void hmsReaderPreservesExecutionFieldOrderAndCaseInsensitiveKeyIndex() {
        Table table = hmsTable();
        ConnectorTableMetadata metadata = HiveTableUtils.getConnectorTableMetadata(table);
        assertEquals("recommendation", metadata.getDatabase());
        assertEquals("items", metadata.getTableName());
        assertEquals(Map.of("connector", "jdbc", "url", "jdbc:h2:mem:test"), metadata.getOptions());
        assertEquals("title", metadata.getFieldSchemas().get(0).getName());
        assertEquals("ItemId", metadata.getFieldSchemas().get(1).getName());
        assertEquals("bigint", metadata.getFieldSchemas().get(1).getType());
        assertEquals("itemid", metadata.getPrimaryKey());
        assertEquals(1, metadata.getPrimaryKeyIndex());
    }

    @Test
    void hmsReaderRetainsMissingCompositeAndUnknownPrimaryKeyErrors() {
        Table table = hmsTable();
        table.getParameters().remove("flink.schema.primary-key.columns");
        assertEquals("Table items has no primary key", assertThrows(IllegalArgumentException.class,
                () -> HiveTableUtils.getConnectorTableMetadata(table)).getMessage());
        table.putToParameters("flink.schema.primary-key.columns", "title,itemid");
        assertEquals("Table items primary key must be single column", assertThrows(IllegalArgumentException.class,
                () -> HiveTableUtils.getConnectorTableMetadata(table)).getMessage());
        table.putToParameters("flink.schema.primary-key.columns", "missing");
        assertEquals("Table primary key missing not found", assertThrows(IllegalArgumentException.class,
                () -> HiveTableUtils.getConnectorTableMetadata(table)).getMessage());
    }

    @Test
    void flinkReaderPreservesNestedTypesAndSingleColumnPrimaryKeyRules() {
        List<Column> columns = Arrays.asList(
                Column.physical("tags", DataTypes.ARRAY(DataTypes.STRING())),
                Column.physical("id", DataTypes.BIGINT()));
        ResolvedSchema schema = new ResolvedSchema(columns, Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
        ConnectorTableMetadata metadata = FlinkSchemaUtils.getConnectorTableMetadata(
                "db", "items", Map.of("connector", "redis"), schema);
        assertEquals(FlinkSchemaUtils.getFieldSchemas(schema).get(0).getType(),
                metadata.getFieldSchemas().get(0).getType());
        assertEquals("BIGINT", metadata.getFieldSchemas().get(1).getType());
        assertEquals(1, metadata.getPrimaryKeyIndex());
        assertEquals("id", metadata.getPrimaryKey());

        ResolvedSchema noKey = ResolvedSchema.of(columns.toArray(new Column[0]));
        assertEquals("table must have primary key", assertThrows(IllegalArgumentException.class,
                () -> FlinkSchemaUtils.getConnectorTableMetadata("db", "items", Map.of(), noKey)).getMessage());
        ResolvedSchema composite = new ResolvedSchema(columns, Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Arrays.asList("tags", "id")));
        assertEquals("table must have only one primary key", assertThrows(IllegalArgumentException.class,
                () -> FlinkSchemaUtils.getConnectorTableMetadata("db", "items", Map.of(), composite)).getMessage());
    }

    private static Table hmsTable() {
        Table table = new Table();
        table.setDbName("recommendation");
        table.setTableName("items");
        StorageDescriptor descriptor = new StorageDescriptor();
        descriptor.setCols(Arrays.asList(
                new org.apache.hadoop.hive.metastore.api.FieldSchema("title", "string", ""),
                new org.apache.hadoop.hive.metastore.api.FieldSchema("ItemId", "bigint", "")));
        table.setSd(descriptor);
        table.setParameters(new LinkedHashMap<>(Map.of(
                "flink.connector", "jdbc", "flink.url", "jdbc:h2:mem:test",
                "flink.schema.primary-key.columns", "itemid", "comment", "ignored")));
        return table;
    }
}
