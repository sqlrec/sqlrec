package com.sqlrec.connectors.mongodb.handler;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class MongoHandlerTest {

    private static final String MONGO_URI = "mongodb://sqlrec:abc123456@"+ SqlRecConfigs.DEFAULT_TEST_IP.getValue()+":30281";
    private static final String DATABASE = "sqlrec_test";
    private static final String COLLECTION = "test_collection";

    private MongoConfig mongoConfig;
    private MongoHandler mongoHandler;
    private MongoClient mongoClient;

    @BeforeEach
    void setUp() {
        mongoConfig = new MongoConfig();
        mongoConfig.uri = MONGO_URI;
        mongoConfig.database = DATABASE;
        mongoConfig.collection = COLLECTION;
        mongoConfig.primaryKey = "id";
        mongoConfig.primaryKeyIndex = 0;
        mongoConfig.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("age", "INTEGER")
        );

        // Insert initial test data directly via MongoDB driver
        mongoClient = MongoClients.create(MONGO_URI);
        MongoDatabase db = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> coll = db.getCollection(COLLECTION);
        coll.drop();
        coll.insertMany(Arrays.asList(
                new Document("id", 1).append("name", "alice").append("age", 20),
                new Document("id", 2).append("name", "bob").append("age", 25),
                new Document("id", 3).append("name", "charlie").append("age", 30)
        ));

        mongoHandler = new MongoHandler(mongoConfig);
    }

    @AfterEach
    void tearDown() {
        // Clean up the collection after each test
        try {
            MongoDatabase db = mongoClient.getDatabase(DATABASE);
            db.getCollection(COLLECTION).drop();
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    @Test
    void testMongoHandlerCreation() {
        MongoHandler handler = new MongoHandler(mongoConfig);
        assertNotNull(handler);
    }

    // ==================== Scan (Query) ====================

    @Test
    void testScan() {
        List<Object[]> rows = mongoHandler.scan(null);
        assertEquals(3, rows.size());
        // verify row structure: [id, name, age]
        boolean foundAlice = false;
        for (Object[] row : rows) {
            if (row[1] != null && row[1].toString().equals("alice")) {
                foundAlice = true;
                assertEquals(1, ((Number) row[0]).intValue());
                assertEquals(20, ((Number) row[2]).intValue());
                break;
            }
        }
        assertTrue(foundAlice, "Should find alice in scan results");
    }

    @Test
    void testScanWithEmptyFilters() {
        List<Object[]> rows = mongoHandler.scan(Collections.emptyList());
        assertEquals(3, rows.size());
    }

    // ==================== GetByPrimaryKey (Query) ====================

    @Test
    void testGetByPrimaryKey() {
        Map<Object, List<Object[]>> result = mongoHandler.getByPrimaryKey(Collections.singleton(2));
        assertEquals(1, result.size());
        List<Object[]> rows = result.get(2);
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(2, ((Number) rows.get(0)[0]).intValue());
        assertEquals("bob", rows.get(0)[1].toString());
        assertEquals(25, ((Number) rows.get(0)[2]).intValue());
    }

    @Test
    void testGetByPrimaryKeyMultipleKeys() {
        Set<Object> keys = new HashSet<>(Arrays.asList(1, 3));
        Map<Object, List<Object[]>> result = mongoHandler.getByPrimaryKey(keys);
        assertEquals(2, result.size());
        assertNotNull(result.get(1));
        assertNotNull(result.get(3));
        assertEquals("alice", result.get(1).get(0)[1].toString());
        assertEquals("charlie", result.get(3).get(0)[1].toString());
    }

    @Test
    void testGetByPrimaryKeyEmptyKeySet() {
        Map<Object, List<Object[]>> result = mongoHandler.getByPrimaryKey(Collections.emptySet());
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetByPrimaryKeyNullKeySet() {
        Map<Object, List<Object[]>> result = mongoHandler.getByPrimaryKey(null);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetByPrimaryKeyNonExistentKey() {
        Map<Object, List<Object[]>> result = mongoHandler.getByPrimaryKey(Collections.singleton(999));
        assertTrue(result.isEmpty());
    }

    // ==================== Upsert (Insert) ====================

    @Test
    void testUpsertInsert() {
        Object[] newRow = {4, "dave", 35};
        boolean result = mongoHandler.upsert(newRow);
        assertTrue(result);

        // verify inserted
        List<Object[]> rows = mongoHandler.scan(null);
        assertEquals(4, rows.size());
        Map<Object, List<Object[]>> found = mongoHandler.getByPrimaryKey(Collections.singleton(4));
        assertEquals("dave", found.get(4).get(0)[1].toString());
    }

    // ==================== Upsert (Update) ====================

    @Test
    void testUpsertUpdate() {
        // upsert with existing primary key should update
        Object[] updatedRow = {2, "bob_updated", 40};
        boolean result = mongoHandler.upsert(updatedRow);
        assertTrue(result);

        // verify updated
        Map<Object, List<Object[]>> found = mongoHandler.getByPrimaryKey(Collections.singleton(2));
        assertEquals(1, found.get(2).size());
        assertEquals("bob_updated", found.get(2).get(0)[1].toString());
        assertEquals(40, ((Number) found.get(2).get(0)[2]).intValue());

        // verify total count unchanged
        List<Object[]> rows = mongoHandler.scan(null);
        assertEquals(3, rows.size());
    }

    // ==================== Delete ====================

    @Test
    void testDelete() {
        Object[] rowToDelete = new Object[]{2, "bob", 25};
        boolean result = mongoHandler.delete(rowToDelete);
        assertTrue(result);

        // verify deleted
        List<Object[]> rows = mongoHandler.scan(null);
        assertEquals(2, rows.size());
        Map<Object, List<Object[]>> found = mongoHandler.getByPrimaryKey(Collections.singleton(2));
        assertTrue(found.isEmpty());
    }

    @Test
    void testDeleteNonExistentRow() {
        // deleting a non-existent row should not throw
        Object[] rowToDelete = new Object[]{999, "nobody", 0};
        boolean result = mongoHandler.delete(rowToDelete);
        assertTrue(result);

        List<Object[]> rows = mongoHandler.scan(null);
        assertEquals(3, rows.size());
    }

    // ==================== Combined CRUD ====================

    @Test
    void testInsertAndScan() {
        // upsert multiple rows
        mongoHandler.upsert(new Object[]{10, "user10", 10});
        mongoHandler.upsert(new Object[]{20, "user20", 20});
        mongoHandler.upsert(new Object[]{30, "user30", 30});

        List<Object[]> rows = mongoHandler.scan(null);
        assertEquals(6, rows.size());
    }

    @Test
    void testFullCrudLifecycle() {
        // 1. Insert
        mongoHandler.upsert(new Object[]{100, "lifecycle_user", 50});
        Map<Object, List<Object[]>> found = mongoHandler.getByPrimaryKey(Collections.singleton(100));
        assertEquals("lifecycle_user", found.get(100).get(0)[1].toString());

        // 2. Update
        mongoHandler.upsert(new Object[]{100, "updated_user", 55});
        found = mongoHandler.getByPrimaryKey(Collections.singleton(100));
        assertEquals("updated_user", found.get(100).get(0)[1].toString());
        assertEquals(55, ((Number) found.get(100).get(0)[2]).intValue());

        // 3. Delete
        mongoHandler.delete(new Object[]{100, "updated_user", 55});
        found = mongoHandler.getByPrimaryKey(Collections.singleton(100));
        assertTrue(found.isEmpty());

        // 4. Verify total count back to initial
        List<Object[]> rows = mongoHandler.scan(null);
        assertEquals(3, rows.size());
    }

    // ==================== Float / Double / Bigint Types ====================

    @Test
    void testFloatDoubleBigintTypes() {
        String collection = "test_numeric_types";
        MongoConfig config = new MongoConfig();
        config.uri = MONGO_URI;
        config.database = DATABASE;
        config.collection = collection;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("score_float", "FLOAT"),
                new FieldSchema("score_double", "DOUBLE"),
                new FieldSchema("big_id", "BIGINT")
        );

        MongoDatabase db = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.drop();
        coll.insertMany(Arrays.asList(
                new Document("id", 1).append("score_float", 3.14).append("score_double", 2.718281828).append("big_id", 100000000000L),
                new Document("id", 2).append("score_float", 0.0).append("score_double", 0.0).append("big_id", 0L)
        ));

        MongoHandler handler = new MongoHandler(config);

        // Scan and verify values
        List<Object[]> rows = handler.scan(null);
        assertEquals(2, rows.size());

        Object[] row1 = rows.stream().filter(r -> ((Number) r[0]).intValue() == 1).findFirst().orElse(null);
        assertNotNull(row1);
        assertEquals(3.14, ((Number) row1[1]).doubleValue(), 0.001);
        assertEquals(2.718281828, ((Number) row1[2]).doubleValue(), 0.0000001);
        assertEquals(100000000000L, ((Number) row1[3]).longValue());

        // Upsert with new row
        Object[] newRow = {3, 99.5, 123.456, 999999999999L};
        assertTrue(handler.upsert(newRow));

        Map<Object, List<Object[]>> found = handler.getByPrimaryKey(Collections.singleton(3));
        assertEquals(1, found.size());
        Object[] foundRow = found.get(3).get(0);
        assertEquals(99.5, ((Number) foundRow[1]).doubleValue(), 0.001);
        assertEquals(123.456, ((Number) foundRow[2]).doubleValue(), 0.001);
        assertEquals(999999999999L, ((Number) foundRow[3]).longValue());

        // Cleanup
        coll.drop();
    }

    // ==================== Array Types ====================

    @Test
    void testArrayOfInteger() {
        String collection = "test_array_int";
        MongoConfig config = new MongoConfig();
        config.uri = MONGO_URI;
        config.database = DATABASE;
        config.collection = collection;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("tags", "ARRAY<INTEGER>")
        );

        MongoDatabase db = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.drop();
        coll.insertMany(Arrays.asList(
                new Document("id", 1).append("tags", Arrays.asList(10, 20, 30)),
                new Document("id", 2).append("tags", Arrays.asList())
        ));

        MongoHandler handler = new MongoHandler(config);
        List<Object[]> rows = handler.scan(null);
        assertEquals(2, rows.size());

        Object[] row1 = rows.stream().filter(r -> ((Number) r[0]).intValue() == 1).findFirst().orElse(null);
        assertNotNull(row1);
        assertTrue(row1[1] instanceof List, "tags should be a List");
        @SuppressWarnings("unchecked")
        List<Object> tags = (List<Object>) row1[1];
        assertEquals(3, tags.size());
        assertEquals(10, ((Number) tags.get(0)).intValue());
        assertEquals(20, ((Number) tags.get(1)).intValue());
        assertEquals(30, ((Number) tags.get(2)).intValue());

        // Empty array
        Object[] row2 = rows.stream().filter(r -> ((Number) r[0]).intValue() == 2).findFirst().orElse(null);
        assertNotNull(row2);
        @SuppressWarnings("unchecked")
        List<Object> tags2 = (List<Object>) row2[1];
        assertTrue(tags2.isEmpty());

        // Upsert with array
        Object[] newRow = {3, Arrays.asList(100, 200)};
        assertTrue(handler.upsert(newRow));
        Map<Object, List<Object[]>> found = handler.getByPrimaryKey(Collections.singleton(3));
        @SuppressWarnings("unchecked")
        List<Object> upsertedTags = (List<Object>) found.get(3).get(0)[1];
        assertEquals(2, upsertedTags.size());
        assertEquals(100, ((Number) upsertedTags.get(0)).intValue());
        assertEquals(200, ((Number) upsertedTags.get(1)).intValue());

        coll.drop();
    }

    @Test
    void testArrayOfFloat() {
        String collection = "test_array_float";
        MongoConfig config = new MongoConfig();
        config.uri = MONGO_URI;
        config.database = DATABASE;
        config.collection = collection;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("embedding", "ARRAY<FLOAT>")
        );

        MongoDatabase db = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.drop();
        coll.insertOne(new Document("id", 1).append("embedding", Arrays.asList(0.1, 0.2, 0.3)));

        MongoHandler handler = new MongoHandler(config);
        List<Object[]> rows = handler.scan(null);
        assertEquals(1, rows.size());

        @SuppressWarnings("unchecked")
        List<Object> embedding = (List<Object>) rows.get(0)[1];
        assertEquals(3, embedding.size());
        assertEquals(0.1, ((Number) embedding.get(0)).doubleValue(), 0.001);
        assertEquals(0.2, ((Number) embedding.get(1)).doubleValue(), 0.001);
        assertEquals(0.3, ((Number) embedding.get(2)).doubleValue(), 0.001);

        coll.drop();
    }

    @Test
    void testArrayOfDouble() {
        String collection = "test_array_double";
        MongoConfig config = new MongoConfig();
        config.uri = MONGO_URI;
        config.database = DATABASE;
        config.collection = collection;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("scores", "ARRAY<DOUBLE>")
        );

        MongoDatabase db = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.drop();
        coll.insertOne(new Document("id", 1).append("scores", Arrays.asList(1.5, 2.5, 3.5)));

        MongoHandler handler = new MongoHandler(config);
        List<Object[]> rows = handler.scan(null);
        assertEquals(1, rows.size());

        @SuppressWarnings("unchecked")
        List<Object> scores = (List<Object>) rows.get(0)[1];
        assertEquals(3, scores.size());
        assertEquals(1.5, ((Number) scores.get(0)).doubleValue(), 0.001);
        assertEquals(2.5, ((Number) scores.get(1)).doubleValue(), 0.001);
        assertEquals(3.5, ((Number) scores.get(2)).doubleValue(), 0.001);

        coll.drop();
    }

    @Test
    void testArrayOfBigint() {
        String collection = "test_array_bigint";
        MongoConfig config = new MongoConfig();
        config.uri = MONGO_URI;
        config.database = DATABASE;
        config.collection = collection;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("big_ids", "ARRAY<BIGINT>")
        );

        MongoDatabase db = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.drop();
        coll.insertOne(new Document("id", 1).append("big_ids", Arrays.asList(100000000001L, 100000000002L)));

        MongoHandler handler = new MongoHandler(config);
        List<Object[]> rows = handler.scan(null);
        assertEquals(1, rows.size());

        @SuppressWarnings("unchecked")
        List<Object> bigIds = (List<Object>) rows.get(0)[1];
        assertEquals(2, bigIds.size());
        assertEquals(100000000001L, ((Number) bigIds.get(0)).longValue());
        assertEquals(100000000002L, ((Number) bigIds.get(1)).longValue());

        coll.drop();
    }

    @Test
    void testArrayOfVarchar() {
        String collection = "test_array_varchar";
        MongoConfig config = new MongoConfig();
        config.uri = MONGO_URI;
        config.database = DATABASE;
        config.collection = collection;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("labels", "ARRAY<VARCHAR>")
        );

        MongoDatabase db = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.drop();
        coll.insertOne(new Document("id", 1).append("labels", Arrays.asList("tag1", "tag2", "tag3")));

        MongoHandler handler = new MongoHandler(config);
        List<Object[]> rows = handler.scan(null);
        assertEquals(1, rows.size());

        @SuppressWarnings("unchecked")
        List<Object> labels = (List<Object>) rows.get(0)[1];
        assertEquals(3, labels.size());
        assertEquals(String.class, labels.get(0).getClass(), "Array element should be String");
        assertEquals("tag1", labels.get(0));
        assertEquals("tag2", labels.get(1));
        assertEquals("tag3", labels.get(2));

        coll.drop();
    }

    @Test
    void testArrayOfString() {
        String collection = "test_array_string";
        MongoConfig config = new MongoConfig();
        config.uri = MONGO_URI;
        config.database = DATABASE;
        config.collection = collection;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("labels", "ARRAY<STRING>")
        );

        MongoDatabase db = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.drop();
        coll.insertOne(new Document("id", 1).append("labels", Arrays.asList("hello", "world")));

        MongoHandler handler = new MongoHandler(config);
        List<Object[]> rows = handler.scan(null);
        assertEquals(1, rows.size());

        @SuppressWarnings("unchecked")
        List<Object> labels = (List<Object>) rows.get(0)[1];
        assertEquals(2, labels.size());
        assertEquals(String.class, labels.get(0).getClass(), "Array element should be String");
        assertEquals("hello", labels.get(0));
        assertEquals("world", labels.get(1));

        coll.drop();
    }

    @Test
    void testMixedScalarAndArrayTypes() {
        String collection = "test_mixed_types";
        MongoConfig config = new MongoConfig();
        config.uri = MONGO_URI;
        config.database = DATABASE;
        config.collection = collection;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("score", "DOUBLE"),
                new FieldSchema("big_id", "BIGINT"),
                new FieldSchema("embedding", "ARRAY<FLOAT>")
        );

        MongoDatabase db = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> coll = db.getCollection(collection);
        coll.drop();
        coll.insertOne(new Document("id", 1)
                .append("name", "test_user")
                .append("score", 95.5)
                .append("big_id", 123456789012L)
                .append("embedding", Arrays.asList(0.1, 0.2, 0.3)));

        MongoHandler handler = new MongoHandler(config);
        List<Object[]> rows = handler.scan(null);
        assertEquals(1, rows.size());

        Object[] row = rows.get(0);
        assertEquals(1, ((Number) row[0]).intValue());
        assertEquals("test_user", row[1].toString());
        assertEquals(95.5, ((Number) row[2]).doubleValue(), 0.001);
        assertEquals(123456789012L, ((Number) row[3]).longValue());

        @SuppressWarnings("unchecked")
        List<Object> embedding = (List<Object>) row[4];
        assertEquals(3, embedding.size());
        assertEquals(0.1, ((Number) embedding.get(0)).doubleValue(), 0.001);

        // Upsert and verify
        Object[] updatedRow = {1, "updated_user", 100.0, 999999999999L, Arrays.asList(0.9, 0.8)};
        assertTrue(handler.upsert(updatedRow));

        Map<Object, List<Object[]>> found = handler.getByPrimaryKey(Collections.singleton(1));
        Object[] foundRow = found.get(1).get(0);
        assertEquals("updated_user", foundRow[1].toString());
        assertEquals(100.0, ((Number) foundRow[2]).doubleValue(), 0.001);
        assertEquals(999999999999L, ((Number) foundRow[3]).longValue());

        @SuppressWarnings("unchecked")
        List<Object> updatedEmbedding = (List<Object>) foundRow[4];
        assertEquals(2, updatedEmbedding.size());
        assertEquals(0.9, ((Number) updatedEmbedding.get(0)).doubleValue(), 0.001);

        coll.drop();
    }
}
