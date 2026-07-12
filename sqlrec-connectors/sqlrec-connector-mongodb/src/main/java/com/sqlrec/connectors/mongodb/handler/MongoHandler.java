package com.sqlrec.connectors.mongodb.handler;

import com.mongodb.client.*;
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

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MongoHandler implements Serializable {
    private static final long serialVersionUID = 1L;
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

    private static final Map<SqlKind, java.util.function.BiFunction<String, Object, Bson>> COMPARISON_OPS = new EnumMap<>(SqlKind.class);
    private static final Map<SqlKind, SqlKind> REVERSED_COMPARISON = new EnumMap<>(SqlKind.class);

    static {
        COMPARISON_OPS.put(SqlKind.EQUALS, Filters::eq);
        COMPARISON_OPS.put(SqlKind.NOT_EQUALS, Filters::ne);
        COMPARISON_OPS.put(SqlKind.GREATER_THAN, Filters::gt);
        COMPARISON_OPS.put(SqlKind.GREATER_THAN_OR_EQUAL, Filters::gte);
        COMPARISON_OPS.put(SqlKind.LESS_THAN, Filters::lt);
        COMPARISON_OPS.put(SqlKind.LESS_THAN_OR_EQUAL, Filters::lte);

        REVERSED_COMPARISON.put(SqlKind.GREATER_THAN, SqlKind.LESS_THAN);
        REVERSED_COMPARISON.put(SqlKind.LESS_THAN, SqlKind.GREATER_THAN);
        REVERSED_COMPARISON.put(SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL);
        REVERSED_COMPARISON.put(SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN_OR_EQUAL);
    }

    private Bson buildBsonFilter(RexNode filter) {
        if (filter.isA(SqlKind.AND) || filter.isA(SqlKind.OR)) {
            RexCall call = (RexCall) filter;
            List<Bson> operands = call.getOperands().stream()
                    .map(this::buildBsonFilter)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            if (operands.isEmpty()) {
                return null;
            }
            return filter.isA(SqlKind.AND) ? Filters.and(operands) : Filters.or(operands);
        }
        if (filter.isA(SqlKind.IS_NULL) || filter.isA(SqlKind.IS_NOT_NULL)) {
            RexCall call = (RexCall) filter;
            String fieldName = getFieldName(call.getOperands().get(0));
            if (fieldName == null) {
                return null;
            }
            return filter.isA(SqlKind.IS_NULL) ? Filters.eq(fieldName, null) : Filters.ne(fieldName, null);
        }
        java.util.function.BiFunction<String, Object, Bson> op = COMPARISON_OPS.get(filter.getKind());
        if (op != null) {
            return buildComparisonFilter((RexCall) filter, op);
        }
        logger.warn("Unsupported filter kind: {}, filter will be handled by Calcite", filter.getKind());
        return null;
    }

    private Bson buildComparisonFilter(RexCall call, java.util.function.BiFunction<String, Object, Bson> filterFactory) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        String fieldName = getFieldName(left);
        RexLiteral literal = getLiteral(right);
        if (fieldName != null && literal != null) {
            return filterFactory.apply(fieldName, literal.getValue2());
        }

        // Try reversed: literal on left, field on right
        fieldName = getFieldName(right);
        literal = getLiteral(left);
        if (fieldName != null && literal != null) {
            SqlKind reversed = REVERSED_COMPARISON.getOrDefault(call.getKind(), call.getKind());
            java.util.function.BiFunction<String, Object, Bson> reversedOp = COMPARISON_OPS.get(reversed);
            return (reversedOp != null ? reversedOp : filterFactory).apply(fieldName, literal.getValue2());
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

    private RexLiteral getLiteral(RexNode node) {
        return node instanceof RexLiteral ? (RexLiteral) node : null;
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
        MongoClient client = getOrCreateMongoClient();
        MongoDatabase database = client.getDatabase(mongoConfig.database);
        return database.getCollection(mongoConfig.collection);
    }

    private MongoClient getOrCreateMongoClient() {
        return mongoClients.computeIfAbsent(mongoConfig.uri, MongoClients::create);
    }
}
