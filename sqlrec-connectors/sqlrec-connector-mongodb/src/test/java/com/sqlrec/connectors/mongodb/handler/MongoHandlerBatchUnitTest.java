package com.sqlrec.connectors.mongodb.handler;

import com.mongodb.MongoTimeoutException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.mongodb.calcite.MongoCalciteTable;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import org.bson.Document;
import org.bson.conversions.Bson;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
    private MongoConfig config;

    @BeforeEach
    void setUp() {
        config = new MongoConfig();
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
    @SuppressWarnings("unchecked")
    void updateMatchesOldKeysWithoutReadingOrUpserting() {
        when(mockCollection.bulkWrite(anyList()))
                .thenReturn(BulkWriteResult.acknowledged(0, 2, 0, 1, List.of(), List.of()));

        var rows = new MongoCalciteTable.MongoCollection(new MongoCalciteTable(config), handler);
        assertTrue(rows.replaceAll(
                List.of(new Object[]{"k1", "one"}, new Object[]{"k2", "two"}, new Object[]{"missing", "three"}),
                List.of(new Object[]{"new1", "changed"}, new Object[]{"k2", "two"}, new Object[]{"new3", "three"})));
        assertEquals(2, rows.size());

        verify(mockCollection, never()).find(any(Bson.class));
        ArgumentCaptor<List<WriteModel<Document>>> captor = ArgumentCaptor.forClass((Class) List.class);
        verify(mockCollection).bulkWrite(captor.capture());
        List<WriteModel<Document>> writes = captor.getValue();
        assertEquals(3, writes.size());
        assertUpdate(writes.get(0), "k1", "new1", "changed");
        assertUpdate(writes.get(1), "k2", "k2", "two");
        assertUpdate(writes.get(2), "missing", "new3", "three");
    }

    @Test
    void emptyUpdateDoesNotSendWrites() {
        assertEquals(0, handler.updateBatch(List.of(), List.of()));
        verify(mockCollection, never()).bulkWrite(anyList());
    }

    @Test
    void updateWithNoMatchesReturnsZero() {
        when(mockCollection.bulkWrite(anyList()))
                .thenReturn(BulkWriteResult.acknowledged(0, 0, 0, 0, List.of(), List.of()));
        var rows = new MongoCalciteTable.MongoCollection(new MongoCalciteTable(config), handler);

        assertFalse(rows.replaceAll(Collections.singletonList(new Object[]{"missing", "value"}),
                Collections.singletonList(new Object[]{"new-key", "value"})));
        assertEquals(0, rows.size());
    }

    @Test
    void unacknowledgedUpdateReportsSubmittedRows() {
        when(mockCollection.bulkWrite(anyList())).thenReturn(BulkWriteResult.unacknowledged());
        assertEquals(2, handler.updateBatch(
                List.of(new Object[]{"old1", "one"}, new Object[]{"old2", "two"}),
                List.of(new Object[]{"new1", "one"}, new Object[]{"new2", "two"})));
    }

    @Test
    void updateRejectsChangingNativeIdAnywhereInTheBatch() {
        config.primaryKeyIndex = 1;
        config.fieldSchemas = List.of(new FieldSchema("_id", "VARCHAR"), new FieldSchema("id", "VARCHAR"));

        var failure = assertThrows(UnsupportedOperationException.class, () -> handler.updateBatch(
                List.of(new Object[]{"native1", "old1"}, new Object[]{"native2", "old2"}),
                List.of(new Object[]{"native1", "new1"}, new Object[]{"changed", "new2"})));
        assertTrue(failure.getMessage().contains("cannot change _id"));
        verify(mockCollection, never()).bulkWrite(anyList());
    }

    @Test
    @SuppressWarnings("unchecked")
    void updateWithNativeIdUsesItDirectly() {
        config.primaryKey = "_id";
        config.fieldSchemas = List.of(new FieldSchema("_id", "VARCHAR"), new FieldSchema("name", "VARCHAR"));
        when(mockCollection.bulkWrite(anyList()))
                .thenReturn(BulkWriteResult.acknowledged(0, 1, 0, 1, List.of(), List.of()));

        assertEquals(1, handler.updateBatch(
                List.<Object[]>of(new Object[]{"key", "old"}),
                List.<Object[]>of(new Object[]{"key", "new"})));

        verify(mockCollection, never()).find(any(Bson.class));
        ArgumentCaptor<List<WriteModel<Document>>> captor = ArgumentCaptor.forClass((Class) List.class);
        verify(mockCollection).bulkWrite(captor.capture());
        var write = (UpdateOneModel<Document>) captor.getValue().get(0);
        assertEquals("key", write.getFilter().toBsonDocument().getString("_id").getValue());
        assertEquals(new Document("name", "new").toBsonDocument(),
                write.getUpdate().toBsonDocument().getDocument("$set"));
    }

    private static void assertUpdate(WriteModel<Document> write, String oldKey, String newKey, String name) {
        UpdateOneModel<Document> update = (UpdateOneModel<Document>) write;
        assertFalse(update.getOptions().isUpsert());
        assertEquals(new Document("id", oldKey).toBsonDocument(), update.getFilter().toBsonDocument());
        var values = update.getUpdate().toBsonDocument().getDocument("$set");
        assertEquals(new Document("id", newKey).append("name", name).toBsonDocument(), values);
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

    @Test
    void testDriverExceptionIsNotRetriedByHandler() {
        when(mockCollection.bulkWrite(anyList()))
                .thenThrow(new MongoTimeoutException("timed out"));

        assertThrows(MongoTimeoutException.class, () -> handler.upsertBatch(
                Collections.singletonList(new Object[]{"k1", "v1"})));

        verify(mockCollection).bulkWrite(anyList());
    }
}
