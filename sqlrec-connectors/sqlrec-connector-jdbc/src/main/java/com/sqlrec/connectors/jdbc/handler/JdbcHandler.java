package com.sqlrec.connectors.jdbc.handler;

import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.sqlrec.connectors.jdbc.sql.JdbcSqlBuilder;
import com.sqlrec.connectors.jdbc.sql.JdbcStatement;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class JdbcHandler {
    private static final Logger logger = LoggerFactory.getLogger(JdbcHandler.class);
    private static final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    static {
        // Ensure every pooled HikariDataSource is closed on JVM exit so that
        // underlying DB connections are not leaked when the process terminates.
        Runtime.getRuntime().addShutdownHook(new Thread(JdbcHandler::closeAllDataSources, "JdbcHandler-shutdown"));
    }

    private final JdbcConfig jdbcConfig;

    public JdbcHandler(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
    }

    public List<Object[]> scan(List<RexNode> filters) {
        JdbcStatement statement = JdbcSqlBuilder.select(
                jdbcConfig.url, jdbcConfig.tableName, jdbcConfig.fieldSchemas, filters);
        return query(statement, "Failed to scan table " + jdbcConfig.tableName);
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyMap();
        }

        JdbcStatement statement = JdbcSqlBuilder.selectByPrimaryKey(
                        jdbcConfig.url, jdbcConfig.tableName, jdbcConfig.fieldSchemas,
                        jdbcConfig.primaryKey, keySet.size())
                .withParameters(new ArrayList<>(keySet));
        List<Object[]> rows = query(statement,
                "Failed to query by primary key from table " + jdbcConfig.tableName);

        Map<Object, List<Object[]>> result = new HashMap<>();
        for (Object[] row : rows) {
            result.computeIfAbsent(row[jdbcConfig.primaryKeyIndex], k -> new ArrayList<>()).add(row);
        }
        return result;
    }

    public boolean upsert(Object[] data) {
        JdbcStatement statement = upsertTemplate().withParameters(Arrays.asList(data));
        update(statement, "Failed to upsert into table " + jdbcConfig.tableName);
        return true;
    }

    public boolean delete(Object[] data) {
        JdbcStatement statement = deleteTemplate()
                .withParameters(Collections.singletonList(data[jdbcConfig.primaryKeyIndex]));
        update(statement, "Failed to delete from table " + jdbcConfig.tableName);
        return true;
    }

    /**
     * Batched upsert: one prepared statement + addBatch/executeBatch instead of one
     * round trip per row. On MySQL, pairing this with
     * {@code rewriteBatchedStatements=true} (via {@code jdbcProperties}) rewrites the
     * batch into multi-values inserts for another order-of-magnitude gain.
     */
    public boolean upsertBatch(Collection<? extends Object[]> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return true;
        }
        return executeBatch(
                dataList,
                upsertTemplate(),
                Arrays::asList,
                "Failed to batch upsert into table " + jdbcConfig.tableName
        );
    }

    public boolean deleteBatch(Collection<? extends Object[]> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return true;
        }
        return executeBatch(
                dataList,
                deleteTemplate(),
                data -> Collections.singletonList(data[jdbcConfig.primaryKeyIndex]),
                "Failed to batch delete from table " + jdbcConfig.tableName
        );
    }

    private boolean executeBatch(
            Collection<? extends Object[]> rows,
            JdbcStatement template,
            Function<Object[], List<?>> parameterMapper,
            String errorMessage
    ) {
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement(template.getSql())) {
            for (Object[] row : rows) {
                template.withParameters(parameterMapper.apply(row)).addToBatch(statement);
            }
            statement.executeBatch();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(errorMessage, e);
        }
    }

    private JdbcStatement upsertTemplate() {
        return JdbcSqlBuilder.upsert(jdbcConfig.url, jdbcConfig.tableName, jdbcConfig.fieldSchemas, jdbcConfig.primaryKey);
    }

    private JdbcStatement deleteTemplate() {
        return JdbcSqlBuilder.deleteByPrimaryKey(jdbcConfig.url, jdbcConfig.tableName, jdbcConfig.primaryKey);
    }

    /**
     * Execute a query statement and materialize all rows.
     */
    private List<Object[]> query(JdbcStatement statement, String errorMessage) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(statement.getSql())) {
            statement.bindTo(stmt);
            try (ResultSet rs = stmt.executeQuery()) {
                return parseResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(errorMessage, e);
        }
    }

    /**
     * Execute an update statement (upsert/delete).
     */
    private void update(JdbcStatement statement, String errorMessage) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(statement.getSql())) {
            statement.bindTo(stmt);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(errorMessage, e);
        }
    }

    private List<Object[]> parseResultSet(ResultSet rs) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        List<Object[]> rows = new ArrayList<>();
        while (rs.next()) {
            Object[] row = new Object[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                row[i - 1] = rs.getObject(i);
            }
            rows.add(row);
        }
        return rows;
    }

    private Connection getConnection() throws SQLException {
        Connection conn = getOrCreateDataSource().getConnection();
        conn.setAutoCommit(true);
        return conn;
    }

    private HikariDataSource getOrCreateDataSource() {
        String key = dataSourceKey();
        HikariDataSource ds = dataSources.get(key);
        if (ds != null) {
            return ds;
        }
        synchronized (dataSources) {
            ds = dataSources.get(key);
            if (ds == null) {
                ds = createDataSource();
                dataSources.put(key, ds);
            }
            return ds;
        }
    }

    /**
     * Cache key for a pooled DataSource. Handlers with different connection or pool
     * settings must not accidentally share a pool.
     */
    private String dataSourceKey() {
        Map<String, String> properties = jdbcConfig.jdbcProperties == null
                ? Collections.emptyMap()
                : new TreeMap<>(jdbcConfig.jdbcProperties);
        return Arrays.asList(
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
        ).toString();
    }

    /**
     * Close every cached HikariDataSource and clear the cache. Registered as a JVM
     * shutdown hook so pooled DB connections are released on process exit.
     */
    public static synchronized void closeAllDataSources() {
        for (Map.Entry<String, HikariDataSource> entry : dataSources.entrySet()) {
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

    private HikariDataSource createDataSource() {
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
