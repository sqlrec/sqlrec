package com.sqlrec.connectors.jdbc.utils;

import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Shared connection pools, keyed by connection and pool settings. */
public final class JdbcDataSourceUtils {
    private static final Logger logger = LoggerFactory.getLogger(JdbcDataSourceUtils.class);
    private static final Map<List<Object>, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(JdbcDataSourceUtils::closeAll, "JdbcHandler-shutdown"));
    }

    private JdbcDataSourceUtils() {
    }

    public static HikariDataSource getOrCreate(JdbcConfig jdbcConfig) {
        List<Object> key = dataSourceKey(jdbcConfig);
        HikariDataSource ds = dataSources.get(key);
        if (ds != null) {
            return ds;
        }
        synchronized (dataSources) {
            ds = dataSources.get(key);
            if (ds == null) {
                ds = createDataSource(jdbcConfig);
                dataSources.put(key, ds);
            }
            return ds;
        }
    }

    /**
     * Cache key for a pooled DataSource. Handlers with different connection or pool
     * settings must not accidentally share a pool.
     */
    static List<Object> dataSourceKey(JdbcConfig jdbcConfig) {
        Map<String, String> properties = jdbcConfig.jdbcProperties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(jdbcConfig.jdbcProperties));
        return Collections.unmodifiableList(Arrays.asList(
                jdbcConfig.url,
                jdbcConfig.username,
                jdbcConfig.password,
                jdbcConfig.driver,
                jdbcConfig.schema,
                jdbcConfig.connectionPoolSize,
                jdbcConfig.connectionPoolMinIdle,
                jdbcConfig.connectionPoolIdleTimeout,
                jdbcConfig.connectionPoolMaxLifetime,
                jdbcConfig.connectionPoolConnectionTimeout,
                jdbcConfig.connectionPoolValidationTimeout,
                jdbcConfig.connectionPoolKeepaliveTime,
                jdbcConfig.connectionPoolName,
                properties
        ));
    }

    /**
     * Close every cached HikariDataSource and clear the cache. Registered as a JVM
     * shutdown hook so pooled DB connections are released on process exit.
     */
    public static synchronized void closeAll() {
        for (Map.Entry<List<Object>, HikariDataSource> entry : dataSources.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                // The map key contains the password because credentials identify a pool;
                // never include it in logs.
                logger.warn("Failed to close JDBC connection pool on shutdown: {}", e.getMessage());
            }
        }
        dataSources.clear();
        logger.info("Closed all JDBC connection pools on shutdown");
    }

    private static HikariDataSource createDataSource(JdbcConfig jdbcConfig) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcConfig.url);
        if (jdbcConfig.username != null && !jdbcConfig.username.isEmpty()) {
            hikariConfig.setUsername(jdbcConfig.username);
        }
        if (jdbcConfig.password != null && !jdbcConfig.password.isEmpty()) {
            hikariConfig.setPassword(jdbcConfig.password);
        }
        if (jdbcConfig.driver != null && !jdbcConfig.driver.isEmpty()) {
            hikariConfig.setDriverClassName(jdbcConfig.driver);
        }
        if (jdbcConfig.connectionPoolSize != null && jdbcConfig.connectionPoolSize > 0) {
            hikariConfig.setMaximumPoolSize(jdbcConfig.connectionPoolSize);
        }
        if (jdbcConfig.connectionPoolMinIdle != null && jdbcConfig.connectionPoolMinIdle > 0) {
            hikariConfig.setMinimumIdle(jdbcConfig.connectionPoolMinIdle);
        }
        if (jdbcConfig.connectionPoolIdleTimeout != null && jdbcConfig.connectionPoolIdleTimeout > 0) {
            hikariConfig.setIdleTimeout(jdbcConfig.connectionPoolIdleTimeout * 1000);
        }
        if (jdbcConfig.connectionPoolMaxLifetime != null && jdbcConfig.connectionPoolMaxLifetime > 0) {
            hikariConfig.setMaxLifetime(jdbcConfig.connectionPoolMaxLifetime * 1000);
        }
        if (jdbcConfig.connectionPoolConnectionTimeout != null && jdbcConfig.connectionPoolConnectionTimeout > 0) {
            hikariConfig.setConnectionTimeout(jdbcConfig.connectionPoolConnectionTimeout * 1000);
        }
        if (jdbcConfig.connectionPoolValidationTimeout != null && jdbcConfig.connectionPoolValidationTimeout > 0) {
            hikariConfig.setValidationTimeout(jdbcConfig.connectionPoolValidationTimeout * 1000);
        }
        if (jdbcConfig.connectionPoolKeepaliveTime != null && jdbcConfig.connectionPoolKeepaliveTime > 0) {
            hikariConfig.setKeepaliveTime(jdbcConfig.connectionPoolKeepaliveTime * 1000);
        }
        if (jdbcConfig.connectionPoolName != null && !jdbcConfig.connectionPoolName.isEmpty()) {
            hikariConfig.setPoolName(jdbcConfig.connectionPoolName);
        }

        // set jdbc custom properties
        if (jdbcConfig.jdbcProperties != null) {
            for (Map.Entry<String, String> entry : jdbcConfig.jdbcProperties.entrySet()) {
                hikariConfig.addDataSourceProperty(entry.getKey(), entry.getValue());
            }
        }

        // set schema if specified (for PostgreSQL etc.)
        if (jdbcConfig.schema != null) {
            hikariConfig.setSchema(jdbcConfig.schema);
        }

        return new HikariDataSource(hikariConfig);
    }
}
