package com.sqlrec.connectors.mongodb.handler;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.mongodb.calcite.MongoCalciteTable;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** End-to-end coverage for Calcite's FilterableTable residual-filter contract. */
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
        List<Integer> ids = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            calciteConnection.getRootSchema().add("users", table);
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT \"id\" FROM \"users\" "
                                 + "WHERE \"id\" = 1 OR \"name\" SIMILAR TO 'A%' "
                                 + "ORDER BY \"id\"")) {
                while (resultSet.next()) {
                    ids.add(resultSet.getInt(1));
                }
            }
        }

        // id=2 proves the unsupported OR branch was not lost during MongoDB pushdown;
        // absence of id=3 proves Calcite evaluated the original residual predicate.
        assertEquals(Arrays.asList(1, 2), ids);
    }
}
