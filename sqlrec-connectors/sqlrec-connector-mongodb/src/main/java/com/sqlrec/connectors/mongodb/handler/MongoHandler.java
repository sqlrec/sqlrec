package com.sqlrec.connectors.mongodb.handler;

import com.mongodb.client.*;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import com.sqlrec.connectors.mongodb.utils.MongoClientUtils;
import org.apache.calcite.rex.RexNode;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.Serializable;
import java.util.*;

public class MongoHandler implements Serializable {
    private static final long serialVersionUID = 1L;

    private final MongoConfig mongoConfig;
    private final MongoFilterTranslator filterTranslator;

    public MongoHandler(MongoConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
        this.filterTranslator = new MongoFilterTranslator(mongoConfig);
    }

    public List<Object[]> scan(List<RexNode> filters) {
        MongoCollection<Document> collection = getCollection();
        Bson query = filterTranslator.buildQuery(filters);
        List<Object[]> rows = new ArrayList<>();
        try (MongoCursor<Document> cursor = collection.find(query).iterator()) {
            while (cursor.hasNext()) {
                rows.add(documentToRow(cursor.next()));
            }
        }
        return rows;
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyMap();
        }

        MongoCollection<Document> collection = getCollection();
        String primaryKey = mongoConfig.primaryKey;
        List<Object> keyList = new ArrayList<>(keySet);

        Map<Object, List<Object[]>> result = new HashMap<>();
        try (MongoCursor<Document> cursor = collection.find(Filters.in(primaryKey, keyList)).iterator()) {
            while (cursor.hasNext()) {
                Object[] row = documentToRow(cursor.next());
                Object key = row[mongoConfig.primaryKeyIndex];
                result.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }
        return result;
    }

    public boolean upsert(Object[] data) {
        MongoCollection<Document> collection = getCollection();
        Document doc = rowToDocument(data);
        Object primaryKeyValue = data[mongoConfig.primaryKeyIndex];
        collection.replaceOne(
                Filters.eq(mongoConfig.primaryKey, primaryKeyValue),
                doc,
                new ReplaceOptions().upsert(true)
        );
        return true;
    }

    public boolean delete(Object[] data) {
        MongoCollection<Document> collection = getCollection();
        Object primaryKeyValue = data[mongoConfig.primaryKeyIndex];
        collection.deleteOne(Filters.eq(mongoConfig.primaryKey, primaryKeyValue));
        return true;
    }

    /**
     * Batched upsert via bulkWrite: one round trip for the whole batch instead of one
     * per row. Ordered (default) execution keeps row-by-row semantics: rows before a
     * failing row are committed and execution stops at the first failure.
     */
    public boolean upsertBatch(Collection<? extends Object[]> records) {
        if (records == null || records.isEmpty()) {
            return true;
        }
        MongoCollection<Document> collection = getCollection();
        List<WriteModel<Document>> writes = new ArrayList<>(records.size());
        for (Object[] data : records) {
            writes.add(new ReplaceOneModel<>(
                    Filters.eq(mongoConfig.primaryKey, data[mongoConfig.primaryKeyIndex]),
                    rowToDocument(data),
                    new ReplaceOptions().upsert(true)));
        }
        collection.bulkWrite(writes);
        return true;
    }

    public boolean deleteBatch(Collection<? extends Object[]> records) {
        if (records == null || records.isEmpty()) {
            return true;
        }
        MongoCollection<Document> collection = getCollection();
        List<WriteModel<Document>> writes = new ArrayList<>(records.size());
        for (Object[] data : records) {
            writes.add(new DeleteOneModel<>(
                    Filters.eq(mongoConfig.primaryKey, data[mongoConfig.primaryKeyIndex])));
        }
        collection.bulkWrite(writes);
        return true;
    }


    private Object[] documentToRow(Document doc) {
        List<FieldSchema> schemas = mongoConfig.fieldSchemas;
        Object[] row = new Object[schemas.size()];
        for (int i = 0; i < schemas.size(); i++) {
            row[i] = doc.get(schemas.get(i).getName());
        }
        return row;
    }

    private Document rowToDocument(Object[] row) {
        List<FieldSchema> schemas = mongoConfig.fieldSchemas;
        Document doc = new Document();
        for (int i = 0; i < schemas.size(); i++) {
            doc.put(schemas.get(i).getName(), row[i]);
        }
        return doc;
    }

    private MongoCollection<Document> getCollection() {
        MongoClient client = testMongoClient != null ? testMongoClient : MongoClientUtils.getOrCreate(mongoConfig.uri);
        MongoDatabase database = client.getDatabase(mongoConfig.database);
        return database.getCollection(mongoConfig.collection);
    }

    /** Test-only mock client, takes precedence over the shared client cache. */
    private MongoClient testMongoClient;

    /** Test-only: inject a mock client so getCollection() skips real connection setup. */
    void setMongoClientForTest(MongoClient client) {
        this.testMongoClient = client;
    }

    /**
     * Close and discard the cached MongoClient for {@code uri} so the next call
     * re-creates it. Intended to be called after a connection-level failure (MongoDB
     * restart, network blip, credential rotation) so that subsequent calls recover
     * instead of failing forever on the broken client.
     */
    public static synchronized void invalidateClient(String uri) {
        MongoClientUtils.invalidateClient(uri);
    }

    /**
     * Close every cached MongoClient and clear the cache. Registered as a JVM shutdown
     * hook so connection pools and background monitoring threads are released on exit.
     */
    public static synchronized void closeAllMongoClients() {
        MongoClientUtils.closeAll();
    }

}
