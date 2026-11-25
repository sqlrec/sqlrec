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
import java.sql.Statement;
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
        executeBatch(
                dataList,
                upsertTemplate(),
                Arrays::asList,
                "Failed to batch upsert into table " + jdbcConfig.tableName
        );
        return true;
    }

    public boolean deleteBatch(Collection<? extends Object[]> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return true;
        }
        executeBatch(
                dataList,
                deleteTemplate(),
                (Object[] data) -> Collections.singletonList(data[jdbcConfig.primaryKeyIndex]),
                "Failed to batch delete from table " + jdbcConfig.tableName
        );
        return true;
    }

    public int updateBatch(List<Object[]> oldRows, List<Object[]> newRows) {
        if (oldRows.isEmpty()) {
            return 0;
        }
        JdbcStatement template = JdbcSqlBuilder.updateByPrimaryKey(
                jdbcConfig.url, jdbcConfig.tableName, jdbcConfig.fieldSchemas, jdbcConfig.primaryKey);
        List<List<Object>> parameters = new ArrayList<>(oldRows.size());
        for (int i = 0; i < oldRows.size(); i++) {
            List<Object> values = new ArrayList<>(Arrays.asList(newRows.get(i)));
            values.add(oldRows.get(i)[jdbcConfig.primaryKeyIndex]);
            parameters.add(values);
        }
        int[] counts = executeBatch(parameters, template, values -> values,
                "Failed to batch update table " + jdbcConfig.tableName);
        return Arrays.stream(counts).map(count -> count == Statement.SUCCESS_NO_INFO ? 1 : count).sum();
    }

    private <T> int[] executeBatch(
            Collection<? extends T> rows,
            JdbcStatement template,
            Function<T, List<?>> parameterMapper,
            String errorMessage
    ) {
        try (Connection connection = getConnection()) {
            connection.setAutoCommit(false);
            try {
                int[] counts;
                try (PreparedStatement statement = connection.prepareStatement(template.getSql())) {
                    for (T row : rows) {
                        template.addToBatch(statement, parameterMapper.apply(row));
                    }
                    counts = statement.executeBatch();
                }
                connection.commit();
                return counts;
            } catch (SQLException | RuntimeException failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
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
        return JdbcDataSourceUtils.getOrCreate(jdbcConfig).getConnection();
    }
}
