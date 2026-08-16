package com.sqlrec.connectors.mongodb.handler;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Mock unit tests for the batched write path (bulkWrite): the integration-tagged
 * MongoHandlerTest covers the row-by-row path against a real server; these tests
 * verify what bulkWrite actually sends.
 */
@ExtendWith(MockitoExtension.class)
class MongoHandlerBatchUnitTest {

    @Mock
    MongoClient mockClient;

    @Mock
    MongoDatabase mockDatabase;

    @Mock
    MongoCollection<Document> mockCollection;

    private MongoHandler handler;

    @BeforeEach
    void setUp() {
        MongoConfig config = new MongoConfig();
        config.uri = "mongodb://localhost:27017";
        config.database = "testdb";
        config.collection = "testcollection";
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "string"),
                new FieldSchema("name", "string"));

        handler = new MongoHandler(config);
        handler.setMongoClientForTest(mockClient);
        // lenient: the empty-batch tests never reach getCollection()
        lenient().when(mockClient.getDatabase("testdb")).thenReturn(mockDatabase);
        lenient().when(mockDatabase.getCollection("testcollection")).thenReturn(mockCollection);
    }

    @Test
    void testUpsertBatchSendsReplaceOneWithUpsert() {
        assertTrue(handler.upsertBatch(Arrays.asList(
                new Object[]{"k1", "v1"},
                new Object[]{"k2", "v2"})));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<WriteModel<Document>>> captor =
                ArgumentCaptor.forClass((Class) List.class);
        verify(mockCollection).bulkWrite(captor.capture());

        List<WriteModel<Document>> writes = captor.getValue();
        assertEquals(2, writes.size());
        for (WriteModel<Document> write : writes) {
            assertTrue(write instanceof ReplaceOneModel,
                    "expected ReplaceOneModel but got " + write.getClass().getSimpleName());
            ReplaceOneModel<Document> replace = (ReplaceOneModel<Document>) write;
            assertTrue(replace.getReplaceOptions().isUpsert(),
                    "batched upsert must set upsert(true)");
            assertTrue(replace.getFilter().toBsonDocument().containsKey("id"));
            // replacement document must carry every schema field
            assertTrue(replace.getReplacement().containsKey("id"));
            assertTrue(replace.getReplacement().containsKey("name"));
        }
    }

    @Test
    void testDeleteBatchSendsDeleteOne() {
        assertTrue(handler.deleteBatch(Arrays.asList(
                new Object[]{"k1", "v1"},
                new Object[]{"k2", "v2"})));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<WriteModel<Document>>> captor =
                ArgumentCaptor.forClass((Class) List.class);
        verify(mockCollection).bulkWrite(captor.capture());

        List<WriteModel<Document>> writes = captor.getValue();
        assertEquals(2, writes.size());
        for (WriteModel<Document> write : writes) {
            assertTrue(write instanceof DeleteOneModel,
                    "expected DeleteOneModel but got " + write.getClass().getSimpleName());
            DeleteOneModel<Document> delete = (DeleteOneModel<Document>) write;
            assertTrue(delete.getFilter().toBsonDocument().containsKey("id"));
        }
    }

    @Test
    void testUpsertBatchEmptyIsNoop() {
        assertTrue(handler.upsertBatch(Collections.emptyList()));
        assertTrue(handler.upsertBatch(null));
        verify(mockCollection, never()).bulkWrite(anyList());
    }

    @Test
    void testDeleteBatchEmptyIsNoop() {
        assertTrue(handler.deleteBatch(Collections.emptyList()));
        assertTrue(handler.deleteBatch(null));
        verify(mockCollection, never()).bulkWrite(anyList());
    }
}
