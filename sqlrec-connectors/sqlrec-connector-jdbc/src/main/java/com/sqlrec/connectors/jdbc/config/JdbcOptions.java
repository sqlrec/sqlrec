package com.sqlrec.connectors.jdbc.config;

import com.sqlrec.common.config.ConfigOption;

import java.util.Map;

public class JdbcOptions {
    public static final String CONNECTOR_IDENTIFIER = "jdbc";

    // ==================== Basic Connection ====================

    public static final ConfigOption<String> URL = new ConfigOption<>(
            "url",
            null,
            "JDBC database connection URL, e.g. jdbc:postgresql://host:port/db",
            null,
            String.class
    );

    public static final ConfigOption<String> TABLE_NAME = new ConfigOption<>(
            "table-name",
            null,
            "JDBC table name to connect",
            null,
            String.class
    );

    public static final ConfigOption<String> USERNAME = new ConfigOption<>(
            "username",
            "",
            "JDBC connection username",
            null,
            String.class
    );

    public static final ConfigOption<String> PASSWORD = new ConfigOption<>(
            "password",
            "",
            "JDBC connection password",
            null,
            String.class
    );

    public static final ConfigOption<String> DRIVER = new ConfigOption<>(
            "driver",
            "",
            "JDBC driver class name, e.g. org.postgresql.Driver",
            null,
            String.class
    );

    public static final ConfigOption<String> SCHEMA = new ConfigOption<>(
            "schema",
            "",
            "JDBC schema name (e.g. PostgreSQL schema)",
            null,
            String.class
    );

    // ==================== Cache ====================

    public static final ConfigOption<Integer> MAX_CACHE_SIZE = new ConfigOption<>(
            "max-cache-size",
            100000,
            "max cache size for local cache",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> CACHE_TTL = new ConfigOption<>(
            "cache-ttl",
            30,
            "ttl for local cache(seconds), 0 means no cache",
            null,
            Integer.class
    );

    // ==================== Connection Pool ====================

    public static final ConfigOption<Integer> CONNECTION_POOL_SIZE = new ConfigOption<>(
            "connection.pool.size",
            0,
            "Connection pool max size (HikariCP maximumPoolSize), 0 means use default",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> CONNECTION_POOL_MIN_IDLE = new ConfigOption<>(
            "connection.pool.min-idle",
            0,
            "Connection pool min idle connections, 0 means use default",
            null,
            Integer.class
    );

    public static final ConfigOption<Long> CONNECTION_POOL_IDLE_TIMEOUT = new ConfigOption<>(
            "connection.pool.idle-timeout",
            0L,
            "Connection pool idle timeout in seconds, 0 means use default",
            null,
            Long.class
    );

    public static final ConfigOption<Long> CONNECTION_POOL_MAX_LIFETIME = new ConfigOption<>(
            "connection.pool.max-lifetime",
            0L,
            "Connection pool max lifetime in seconds, 0 means use default",
            null,
            Long.class
    );

    public static final ConfigOption<Long> CONNECTION_POOL_CONNECTION_TIMEOUT = new ConfigOption<>(
            "connection.pool.connection-timeout",
            0L,
            "Connection pool connection timeout in seconds, 0 means use default",
            null,
            Long.class
    );

    public static final ConfigOption<Long> CONNECTION_POOL_VALIDATION_TIMEOUT = new ConfigOption<>(
            "connection.pool.validation-timeout",
            0L,
            "Connection pool validation timeout in seconds, 0 means use default",
            null,
            Long.class
    );

    public static final ConfigOption<Long> CONNECTION_POOL_KEEPALIVE_TIME = new ConfigOption<>(
            "connection.pool.keepalive-time",
            0L,
            "Connection pool keepalive time in seconds, 0 means use default",
            null,
            Long.class
    );

    public static final ConfigOption<String> CONNECTION_POOL_NAME = new ConfigOption<>(
            "connection.pool.pool-name",
            "",
            "Connection pool name",
            null,
            String.class
    );

    public static JdbcConfig getJdbcConfig(Map<String, String> options) {
        JdbcConfig config = new JdbcConfig();
        config.url = URL.getValue(options);
        config.tableName = TABLE_NAME.getValue(options);
        config.username = USERNAME.getValue(options);
        config.password = PASSWORD.getValue(options);
        config.driver = DRIVER.getValue(options);
        config.schema = SCHEMA.getValue(options);

        config.maxCacheSize = MAX_CACHE_SIZE.getValue(options);
        config.cacheTtl = CACHE_TTL.getValue(options);

        config.connectionPoolSize = CONNECTION_POOL_SIZE.getValue(options);
        config.connectionPoolMinIdle = CONNECTION_POOL_MIN_IDLE.getValue(options);
        config.connectionPoolIdleTimeout = CONNECTION_POOL_IDLE_TIMEOUT.getValue(options);
        config.connectionPoolMaxLifetime = CONNECTION_POOL_MAX_LIFETIME.getValue(options);
        config.connectionPoolConnectionTimeout = CONNECTION_POOL_CONNECTION_TIMEOUT.getValue(options);
        config.connectionPoolValidationTimeout = CONNECTION_POOL_VALIDATION_TIMEOUT.getValue(options);
        config.connectionPoolKeepaliveTime = CONNECTION_POOL_KEEPALIVE_TIME.getValue(options);
        config.connectionPoolName = CONNECTION_POOL_NAME.getValue(options);

        // extract jdbc.properties.* from options
        config.jdbcProperties = new java.util.HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (entry.getKey().startsWith("jdbc.properties.")) {
                config.jdbcProperties.put(
                        entry.getKey().substring("jdbc.properties.".length()),
                        entry.getValue()
                );
            }
        }

        return config;
    }
}
