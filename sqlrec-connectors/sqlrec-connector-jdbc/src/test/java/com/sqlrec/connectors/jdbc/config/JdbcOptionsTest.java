package com.sqlrec.connectors.jdbc.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JdbcOptionsTest {

    private Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:postgresql://localhost:5432/mydb");
        options.put("table-name", "users");
        return options;
    }

    @Test
    void testConnectorIdentifier() {
        assertEquals("jdbc", JdbcOptions.CONNECTOR_IDENTIFIER);
    }

    @Test
    void testGetJdbcConfigWithRequiredOptions() {
        Map<String, String> options = baseOptions();
        options.put("username", "postgres");
        options.put("password", "123456");

        JdbcConfig config = JdbcOptions.getJdbcConfig(options);

        assertEquals("jdbc:postgresql://localhost:5432/mydb", config.url);
        assertEquals("users", config.tableName);
        assertEquals("postgres", config.username);
        assertEquals("123456", config.password);
    }

    @Test
    void testGetJdbcConfigWithDriver() {
        Map<String, String> options = baseOptions();
        options.put("driver", "org.postgresql.Driver");

        JdbcConfig config = JdbcOptions.getJdbcConfig(options);

        assertEquals("org.postgresql.Driver", config.driver);
    }

    @Test
    void testGetJdbcConfigWithSchema() {
        Map<String, String> options = baseOptions();
        options.put("schema", "public");

        JdbcConfig config = JdbcOptions.getJdbcConfig(options);

        assertEquals("public", config.schema);
    }

    @Test
    void testGetJdbcConfigWithCacheOptions() {
        Map<String, String> options = baseOptions();
        options.put("max-cache-size", "5000");
        options.put("cache-ttl", "600");

        JdbcConfig config = JdbcOptions.getJdbcConfig(options);

        assertEquals(5000, config.maxCacheSize);
        assertEquals(600, config.cacheTtl);
    }

    @Test
    void testGetJdbcConfigWithConnectionPoolOptions() {
        Map<String, String> options = baseOptions();
        options.put("connection.pool.size", "20");
        options.put("connection.pool.min-idle", "5");
        options.put("connection.pool.idle-timeout", "600");
        options.put("connection.pool.max-lifetime", "1800");
        options.put("connection.pool.connection-timeout", "30");
        options.put("connection.pool.validation-timeout", "5");
        options.put("connection.pool.keepalive-time", "60");
        options.put("connection.pool.pool-name", "my-pool");

        JdbcConfig config = JdbcOptions.getJdbcConfig(options);

        assertEquals(20, config.connectionPoolSize);
        assertEquals(5, config.connectionPoolMinIdle);
        assertEquals(600L, config.connectionPoolIdleTimeout);
        assertEquals(1800L, config.connectionPoolMaxLifetime);
        assertEquals(30L, config.connectionPoolConnectionTimeout);
        assertEquals(5L, config.connectionPoolValidationTimeout);
        assertEquals(60L, config.connectionPoolKeepaliveTime);
        assertEquals("my-pool", config.connectionPoolName);
    }

    @Test
    void testGetJdbcConfigWithJdbcProperties() {
        Map<String, String> options = baseOptions();
        options.put("jdbc.properties.useSSL", "false");
        options.put("jdbc.properties.characterEncoding", "utf8");

        JdbcConfig config = JdbcOptions.getJdbcConfig(options);

        assertNotNull(config.jdbcProperties);
        assertEquals("false", config.jdbcProperties.get("useSSL"));
        assertEquals("utf8", config.jdbcProperties.get("characterEncoding"));
        assertEquals(2, config.jdbcProperties.size());
    }

    @Test
    void testGetJdbcConfigMissingUrlThrows() {
        Map<String, String> options = new HashMap<>();
        options.put("table-name", "users");

        assertThrows(IllegalArgumentException.class, () -> JdbcOptions.getJdbcConfig(options));
    }

    @Test
    void testGetJdbcConfigMissingTableNameThrows() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:postgresql://localhost:5432/mydb");

        assertThrows(IllegalArgumentException.class, () -> JdbcOptions.getJdbcConfig(options));
    }

    @Test
    void testDefaultValues() {
        Map<String, String> options = baseOptions();

        JdbcConfig config = JdbcOptions.getJdbcConfig(options);

        // default values - strings default to empty
        assertEquals("", config.username);
        assertEquals("", config.password);
        assertEquals("", config.driver);
        assertEquals("", config.schema);
        // cache defaults - aligned with Redis connector
        assertEquals(100000, config.maxCacheSize);
        assertEquals(30, config.cacheTtl);
        // connection pool defaults
        assertEquals(0, config.connectionPoolSize);
        assertEquals(0, config.connectionPoolMinIdle);
        assertEquals(0L, config.connectionPoolIdleTimeout);
        assertEquals(0L, config.connectionPoolMaxLifetime);
        assertEquals(0L, config.connectionPoolConnectionTimeout);
        assertEquals(0L, config.connectionPoolValidationTimeout);
        assertEquals(0L, config.connectionPoolKeepaliveTime);
        assertEquals("", config.connectionPoolName);
    }
}
