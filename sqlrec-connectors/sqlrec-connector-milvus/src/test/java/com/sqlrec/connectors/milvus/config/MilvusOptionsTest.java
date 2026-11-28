package com.sqlrec.connectors.milvus.config;

import com.sqlrec.common.schema.ConnectorTableMetadata;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MilvusOptionsTest {
    @Test
    void usesMilvusDatabaseOptionAndKeepsMetadataFieldOrder() {
        ConnectorTableMetadata metadata = new ConnectorTableMetadata(
                "sql_catalog_db", "items", Map.of(
                        "url", "http://localhost:19530", "token", "test-token",
                        "database", "milvus_db", "collection", "vectors"),
                List.of(new FieldSchema("embedding", "ARRAY<FLOAT>"), new FieldSchema("ItemId", "BIGINT")),
                "itemid");

        MilvusConfig config = MilvusOptions.fromMetadata(metadata);

        assertEquals("milvus_db", config.database);
        assertEquals("vectors", config.collection);
        assertEquals("embedding", config.fieldSchemas.get(0).getName());
        assertEquals("itemid", config.primaryKey);
        assertEquals(1, config.primaryKeyIndex);
        assertEquals(4096, config.batchSize);
    }

    @Test
    void defaultsToMilvusDefaultDatabaseWhenOptionIsMissing() {
        ConnectorTableMetadata metadata = new ConnectorTableMetadata(
                "sql_catalog_db", "items", Map.of(
                        "url", "http://localhost:19530", "token", "test-token", "collection", "vectors"),
                List.of(new FieldSchema("id", "BIGINT")), "id");

        assertEquals("default", MilvusOptions.fromMetadata(metadata).database);
    }
}
