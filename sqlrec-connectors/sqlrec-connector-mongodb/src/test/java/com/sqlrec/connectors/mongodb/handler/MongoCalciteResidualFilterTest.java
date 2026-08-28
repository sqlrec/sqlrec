package com.sqlrec.connectors.mongodb.handler;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.connectors.mongodb.calcite.MongoCalciteTable;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end coverage for SQLRec's custom FilterableTableScan residual-filter contract. */
@Tag("integration")
class MongoCalciteResidualFilterTest {

    private static final String MONGO_URI = "mongodb://sqlrec:abc123456@"
            + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30029";
    private static final String DATABASE = "sqlrec_test";
    private static final String COLLECTION = "test_calcite_residual_filter";

    private MongoClient mongoClient;
    private MongoCalciteTable table;

    @BeforeEach
    void setUp() {
        MongoConfig config = new MongoConfig();
        config.uri = MONGO_URI;
        config.database = DATABASE;
        config.collection = COLLECTION;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"));
        table = new MongoCalciteTable(config);

        mongoClient = MongoClients.create(MONGO_URI);
        MongoCollection<Document> collection = mongoClient
                .getDatabase(DATABASE)
                .getCollection(COLLECTION);
        collection.drop();
        collection.insertMany(Arrays.asList(
                new Document("id", 1).append("name", "Bob"),
                new Document("id", 2).append("name", "Alice"),
                // This row reaches Calcite because the mixed OR cannot be safely narrowed in
                // MongoDB, but it must be rejected by Calcite's residual filter.
                new Document("id", 3).append("name", "Bob")));
    }

    @AfterEach
    void tearDown() {
        try {
            if (mongoClient != null) {
                MongoDatabase database = mongoClient.getDatabase(DATABASE);
                database.getCollection(COLLECTION).drop();
            }
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
            MongoHandler.closeAllMongoClients();
        }
    }

    @Test
    void unsupportedMixedOrIsAppliedAsCalciteResidualFilter() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("users", table);
            }
        });

        BindableInterface bindable = NormalSqlCompiler.getNormalSqlBindable(
                "SELECT id FROM users "
                        + "WHERE id = 1 OR name SIMILAR TO 'A%' "
                        + "ORDER BY id",
                schema,
                Consts.DEFAULT_SCHEMA_NAME);

        List<Integer> ids = new ArrayList<>();
        Enumerable<Object[]> result = bindable.bind(schema, new ExecuteContextImpl());
        for (Object[] row : result) {
            ids.add(((Number) row[0]).intValue());
        }

        assertTrue(bindable.getPhysicalPlan().contains("FilterableTableScan"));
        assertTrue(bindable.getPhysicalPlan().contains("EnumerableCalc"));
        // id=2 proves the unsupported OR branch was not lost during MongoDB pushdown;
        // absence of id=3 proves Calcite evaluated the original residual predicate.
        assertEquals(Arrays.asList(1, 2), ids);
    }
}
