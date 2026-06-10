package com.sqlrec.connectors.jdbc.handler;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.SqlUtils;
import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class JdbcHandler {
    private static final Logger logger = LoggerFactory.getLogger(JdbcHandler.class);
    private static final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    private final JdbcConfig jdbcConfig;

    public JdbcHandler(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
    }

    public List<Object[]> scan(List<RexNode> filters) {
        String whereClause = SqlUtils.buildWhereClause(filters, jdbcConfig.fieldSchemas);
        String sql = SqlUtils.buildSelectSql(jdbcConfig.tableName, jdbcConfig.fieldSchemas, whereClause);
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            return parseResultSet(rs);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to scan table " + jdbcConfig.tableName, e);
        } finally {
            closeQuietly(rs);
            closeQuietly(stmt);
            closeQuietly(conn);
        }
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyMap();
        }

        String primaryKey = jdbcConfig.primaryKey;
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < keySet.size(); i++) {
            if (i > 0) {
                placeholders.append(",");
            }
            placeholders.append("?");
        }

        String whereClause = primaryKey + " IN (" + placeholders + ")";
        String sql = SqlUtils.buildSelectSql(jdbcConfig.tableName, jdbcConfig.fieldSchemas, whereClause);
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.prepareStatement(sql);
            int idx = 1;
            for (Object key : keySet) {
                stmt.setObject(idx++, key);
            }
            rs = stmt.executeQuery();
            List<Object[]> rows = parseResultSet(rs);

            Map<Object, List<Object[]>> result = new HashMap<>();
            for (Object[] row : rows) {
                Object key = row[jdbcConfig.primaryKeyIndex];
                result.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query by primary key from table " + jdbcConfig.tableName, e);
        } finally {
            closeQuietly(rs);
            closeQuietly(stmt);
            closeQuietly(conn);
        }
    }

    public boolean upsert(Object[] data) {
        String sql = SqlUtils.buildUpsertSql(jdbcConfig.url, jdbcConfig.tableName, jdbcConfig.fieldSchemas, jdbcConfig.primaryKey);
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.prepareStatement(sql);
            setStatementParameters(stmt, data);
            stmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to upsert into table " + jdbcConfig.tableName, e);
        } finally {
            closeQuietly(stmt);
            closeQuietly(conn);
        }
    }

    public boolean delete(Object[] data) {
        String sql = SqlUtils.buildDeleteSql(jdbcConfig.tableName, jdbcConfig.primaryKey);
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setObject(1, data[jdbcConfig.primaryKeyIndex]);
            stmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete from table " + jdbcConfig.tableName, e);
        } finally {
            closeQuietly(stmt);
            closeQuietly(conn);
        }
    }

    private void setStatementParameters(PreparedStatement stmt, Object[] data) throws SQLException {
        for (int i = 0; i < data.length; i++) {
            stmt.setObject(i + 1, data[i]);
        }
    }

    private List<Object[]> parseResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
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
        HikariDataSource ds = getOrCreateDataSource();
        Connection conn = ds.getConnection();
        conn.setAutoCommit(true);
        return conn;
    }

    private HikariDataSource getOrCreateDataSource() {
        String key = getDataSourceKey();
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

    private String getDataSourceKey() {
        return jdbcConfig.url + "|" + jdbcConfig.username;
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

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                logger.warn("Failed to close resource", e);
            }
        }
    }
}
