package com.sqlrec.connectors.jdbc.handler;

import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.sqlrec.connectors.jdbc.sql.JdbcSqlBuilder;
import com.sqlrec.connectors.jdbc.sql.JdbcStatement;
import com.sqlrec.connectors.jdbc.utils.JdbcDataSourceUtils;
import org.apache.calcite.rex.RexNode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

public class JdbcHandler {
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
                template.addToBatch(statement, parameterMapper.apply(row));
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
        Connection conn = JdbcDataSourceUtils.getOrCreate(jdbcConfig).getConnection();
        conn.setAutoCommit(true);
        return conn;
    }

    /** Closes all shared connection pools and clears the cache. */
    public static synchronized void closeAllDataSources() {
        JdbcDataSourceUtils.closeAll();
    }
}
