package com.sqlrec.connectors.mongodb.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MongoOptionsTest {

    private Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("uri", "mongodb://localhost:27017");
        options.put("database", "testdb");
        options.put("collection", "users");
        return options;
    }

    @Test
    void testConnectorIdentifier() {
        assertEquals("mongodb", MongoOptions.CONNECTOR_IDENTIFIER);
    }

    @Test
    void testGetMongoConfigWithRequiredOptions() {
        Map<String, String> options = baseOptions();

        MongoConfig config = MongoOptions.getMongoConfig(options);

        assertEquals("mongodb://localhost:27017", config.uri);
        assertEquals("testdb", config.database);
        assertEquals("users", config.collection);
    }

    @Test
    void testGetMongoConfigWithCacheOptions() {
        Map<String, String> options = baseOptions();
        options.put("max-cache-size", "5000");
        options.put("cache-ttl", "600");

        MongoConfig config = MongoOptions.getMongoConfig(options);

        assertEquals(5000, config.maxCacheSize);
        assertEquals(600, config.cacheTtl);
    }

    @Test
    void testGetMongoConfigMissingUriThrows() {
        Map<String, String> options = new HashMap<>();
        options.put("database", "testdb");
        options.put("collection", "users");

        assertThrows(IllegalArgumentException.class, () -> MongoOptions.getMongoConfig(options));
    }

    @Test
    void testGetMongoConfigMissingDatabaseThrows() {
        Map<String, String> options = new HashMap<>();
        options.put("uri", "mongodb://localhost:27017");
        options.put("collection", "users");

        assertThrows(IllegalArgumentException.class, () -> MongoOptions.getMongoConfig(options));
    }

    @Test
    void testGetMongoConfigMissingCollectionThrows() {
        Map<String, String> options = new HashMap<>();
        options.put("uri", "mongodb://localhost:27017");
        options.put("database", "testdb");

        assertThrows(IllegalArgumentException.class, () -> MongoOptions.getMongoConfig(options));
    }

    @Test
    void testDefaultValues() {
        Map<String, String> options = baseOptions();

        MongoConfig config = MongoOptions.getMongoConfig(options);

        assertEquals(100000, config.maxCacheSize);
        assertEquals(30, config.cacheTtl);
    }
}
