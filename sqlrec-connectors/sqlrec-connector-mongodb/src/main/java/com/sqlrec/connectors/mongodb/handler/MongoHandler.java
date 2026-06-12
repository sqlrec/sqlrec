package com.sqlrec.connectors.mongodb.handler;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MongoHandler {
    private static final Logger logger = LoggerFactory.getLogger(MongoHandler.class);
    private static final Map<String, MongoClient> mongoClients = new ConcurrentHashMap<>();

    private final MongoConfig mongoConfig;

    public MongoHandler(MongoConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
    }

    public List<Object[]> scan(List<RexNode> filters) {
        MongoCollection<Document> collection = getCollection();
        Bson query = buildQuery(filters);
        List<Object[]> rows = new ArrayList<>();
        for (Document doc : collection.find(query)) {
            rows.add(documentToRow(doc));
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
        Iterable<Document> docs = collection.find(Filters.in(primaryKey, keyList));

        Map<Object, List<Object[]>> result = new HashMap<>();
        for (Document doc : docs) {
            Object[] row = documentToRow(doc);
            Object key = row[mongoConfig.primaryKeyIndex];
            result.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
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

    private Bson buildQuery(List<RexNode> filters) {
        if (filters == null || filters.isEmpty()) {
            return new Document();
        }
        List<Bson> bsonFilters = filters.stream()
                .map(filter -> buildBsonFilter(filter))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (bsonFilters.isEmpty()) {
            return new Document();
        }
        if (bsonFilters.size() == 1) {
            return bsonFilters.get(0);
        }
        return Filters.and(bsonFilters);
    }

    private Bson buildBsonFilter(RexNode filter) {
        if (filter.isA(SqlKind.AND)) {
            RexCall call = (RexCall) filter;
            List<Bson> operands = call.getOperands().stream()
                    .map(this::buildBsonFilter)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            return operands.isEmpty() ? null : Filters.and(operands);
        }
        if (filter.isA(SqlKind.OR)) {
            RexCall call = (RexCall) filter;
            List<Bson> operands = call.getOperands().stream()
                    .map(this::buildBsonFilter)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            return operands.isEmpty() ? null : Filters.or(operands);
        }
        if (filter.isA(SqlKind.IS_NULL)) {
            RexCall call = (RexCall) filter;
            String fieldName = getFieldName(call.getOperands().get(0));
            if (fieldName != null) {
                return Filters.eq(fieldName, null);
            }
            return null;
        }
        if (filter.isA(SqlKind.IS_NOT_NULL)) {
            RexCall call = (RexCall) filter;
            String fieldName = getFieldName(call.getOperands().get(0));
            if (fieldName != null) {
                return Filters.ne(fieldName, null);
            }
            return null;
        }
        if (filter.isA(SqlKind.EQUALS)) {
            return buildComparisonFilter((RexCall) filter, (field, value) -> Filters.eq(field, value));
        }
        if (filter.isA(SqlKind.NOT_EQUALS)) {
            return buildComparisonFilter((RexCall) filter, (field, value) -> Filters.ne(field, value));
        }
        if (filter.isA(SqlKind.GREATER_THAN)) {
            return buildComparisonFilter((RexCall) filter, (field, value) -> Filters.gt(field, value));
        }
        if (filter.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {
            return buildComparisonFilter((RexCall) filter, (field, value) -> Filters.gte(field, value));
        }
        if (filter.isA(SqlKind.LESS_THAN)) {
            return buildComparisonFilter((RexCall) filter, (field, value) -> Filters.lt(field, value));
        }
        if (filter.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
            return buildComparisonFilter((RexCall) filter, (field, value) -> Filters.lte(field, value));
        }
        logger.warn("Unsupported filter kind: {}, filter will be handled by Calcite", filter.getKind());
        return null;
    }

    private Bson buildComparisonFilter(RexCall call, java.util.function.BiFunction<String, Object, Bson> filterFactory) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        String fieldName = getFieldName(left);
        Object value = extractValue(right);
        if (fieldName != null && value != null) {
            return filterFactory.apply(fieldName, value);
        }

        // Try reversed: literal on left, field on right
        fieldName = getFieldName(right);
        value = extractValue(left);
        if (fieldName != null && value != null) {
            SqlKind kind = call.getKind();
            if (kind == SqlKind.GREATER_THAN) {
                return Filters.lt(fieldName, value);
            } else if (kind == SqlKind.LESS_THAN) {
                return Filters.gt(fieldName, value);
            } else if (kind == SqlKind.GREATER_THAN_OR_EQUAL) {
                return Filters.lte(fieldName, value);
            } else if (kind == SqlKind.LESS_THAN_OR_EQUAL) {
                return Filters.gte(fieldName, value);
            }
            return filterFactory.apply(fieldName, value);
        }
        logger.warn("Cannot convert filter to MongoDB query: {}", call);
        return null;
    }

    private String getFieldName(RexNode node) {
        if (node instanceof RexInputRef) {
            int index = ((RexInputRef) node).getIndex();
            return mongoConfig.fieldSchemas.get(index).getName();
        }
        return null;
    }

    private Object extractValue(RexNode node) {
        if (node instanceof RexLiteral) {
            return ((RexLiteral) node).getValue2();
        }
        return null;
    }

    private Object[] documentToRow(Document doc) {
        Object[] row = new Object[mongoConfig.fieldSchemas.size()];
        for (int i = 0; i < mongoConfig.fieldSchemas.size(); i++) {
            FieldSchema fieldSchema = mongoConfig.fieldSchemas.get(i);
            row[i] = doc.get(fieldSchema.getName());
        }
        return row;
    }

    private Document rowToDocument(Object[] row) {
        Document doc = new Document();
        for (int i = 0; i < mongoConfig.fieldSchemas.size(); i++) {
            FieldSchema fieldSchema = mongoConfig.fieldSchemas.get(i);
            doc.put(fieldSchema.getName(), row[i]);
        }
        return doc;
    }

    private MongoCollection<Document> getCollection() {
        MongoClient client = getOrCreateMongoClient();
        MongoDatabase database = client.getDatabase(mongoConfig.database);
        return database.getCollection(mongoConfig.collection);
    }

    private MongoClient getOrCreateMongoClient() {
        String key = mongoConfig.uri;
        MongoClient client = mongoClients.get(key);
        if (client != null) {
            return client;
        }
        synchronized (mongoClients) {
            client = mongoClients.get(key);
            if (client == null) {
                client = MongoClients.create(mongoConfig.uri);
                mongoClients.put(key, client);
            }
            return client;
        }
    }
}
