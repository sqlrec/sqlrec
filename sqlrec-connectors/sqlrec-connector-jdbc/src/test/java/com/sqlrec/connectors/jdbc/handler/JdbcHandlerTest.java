package com.sqlrec.connectors.jdbc.handler;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class JdbcHandlerTest {

    private static final String JDBC_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
    private static final String DRIVER = "org.h2.Driver";
    private static final String TABLE_NAME = "test_table";

    private JdbcConfig jdbcConfig;
    private JdbcHandler jdbcHandler;

    @BeforeEach
    void setUp() throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'alice', 20)");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 'bob', 25)");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (3, 'charlie', 30)");
        }

        jdbcConfig = new JdbcConfig();
        jdbcConfig.url = JDBC_URL;
        jdbcConfig.driver = DRIVER;
        jdbcConfig.tableName = TABLE_NAME;
        jdbcConfig.primaryKey = "id";
        jdbcConfig.primaryKeyIndex = 0;
        jdbcConfig.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("age", "INTEGER")
        );

        jdbcHandler = new JdbcHandler(jdbcConfig);
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS \"" + TABLE_NAME + "\"");
        }
    }

    @Test
    void testScan() {
        List<Object[]> rows = jdbcHandler.scan(Collections.emptyList());
        assertEquals(3, rows.size());
        // verify row structure: [id, name, age]
        boolean foundAlice = false;
        for (Object[] row : rows) {
            if (row[1] != null && row[1].toString().equals("alice")) {
                foundAlice = true;
                assertEquals(1, ((Number) row[0]).intValue());
                assertEquals(20, ((Number) row[2]).intValue());
                break;
            }
        }
        assertTrue(foundAlice, "Should find alice in scan results");
    }

    @Test
    void testGetByPrimaryKey() {
        Map<Object, List<Object[]>> result = jdbcHandler.getByPrimaryKey(Collections.singleton(2));
        assertEquals(1, result.size());
        List<Object[]> rows = result.get(2);
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(2, ((Number) rows.get(0)[0]).intValue());
        assertEquals("bob", rows.get(0)[1].toString());
        assertEquals(25, ((Number) rows.get(0)[2]).intValue());
    }

    @Test
    void testGetByPrimaryKeyMultipleKeys() {
        Set<Object> keys = new HashSet<>(Arrays.asList(1, 3));
        Map<Object, List<Object[]>> result = jdbcHandler.getByPrimaryKey(keys);
        assertEquals(2, result.size());
        assertNotNull(result.get(1));
        assertNotNull(result.get(3));
        assertEquals("alice", result.get(1).get(0)[1].toString());
        assertEquals("charlie", result.get(3).get(0)[1].toString());
    }

    @Test
    void testGetByPrimaryKeyEmptyKeySet() {
        Map<Object, List<Object[]>> result = jdbcHandler.getByPrimaryKey(Collections.emptySet());
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetByPrimaryKeyNonExistentKey() {
        Map<Object, List<Object[]>> result = jdbcHandler.getByPrimaryKey(Collections.singleton(999));
        assertTrue(result.isEmpty());
    }

    @Test
    void testUpsertInsert() {
        Object[] newRow = {4, "dave", 35};
        boolean result = jdbcHandler.upsert(newRow);
        assertTrue(result);

        // verify inserted
        List<Object[]> rows = jdbcHandler.scan(Collections.emptyList());
        assertEquals(4, rows.size());
        Map<Object, List<Object[]>> found = jdbcHandler.getByPrimaryKey(Collections.singleton(4));
        assertEquals("dave", found.get(4).get(0)[1].toString());
    }

    @Test
    void testUpsertUpdate() {
        // insert first
        Object[] row = {4, "dave", 35};
        jdbcHandler.upsert(row);

        // upsert with same primary key should update
        Object[] updatedRow = {4, "dave_updated", 40};
        boolean result = jdbcHandler.upsert(updatedRow);
        assertTrue(result);

        // verify updated
        Map<Object, List<Object[]>> found = jdbcHandler.getByPrimaryKey(Collections.singleton(4));
        assertEquals(1, found.get(4).size());
        assertEquals("dave_updated", found.get(4).get(0)[1].toString());
        assertEquals(40, ((Number) found.get(4).get(0)[2]).intValue());

        // verify total count unchanged
        List<Object[]> rows = jdbcHandler.scan(Collections.emptyList());
        assertEquals(4, rows.size());
    }

    @Test
    void testDelete() {
        Object[] rowToDelete = new Object[]{2, "bob", 25};
        boolean result = jdbcHandler.delete(rowToDelete);
        assertTrue(result);

        // verify deleted
        List<Object[]> rows = jdbcHandler.scan(Collections.emptyList());
        assertEquals(2, rows.size());
        Map<Object, List<Object[]>> found = jdbcHandler.getByPrimaryKey(Collections.singleton(2));
        assertTrue(found.isEmpty());
    }

    @Test
    void testInsertAndScan() {
        // upsert multiple rows
        jdbcHandler.upsert(new Object[]{10, "user10", 10});
        jdbcHandler.upsert(new Object[]{20, "user20", 20});
        jdbcHandler.upsert(new Object[]{30, "user30", 30});

        List<Object[]> rows = jdbcHandler.scan(Collections.emptyList());
        assertEquals(6, rows.size());
    }

    @Test
    void testDeleteNonExistentRow() {
        // deleting a non-existent row should not throw
        Object[] rowToDelete = new Object[]{999, "nobody", 0};
        boolean result = jdbcHandler.delete(rowToDelete);
        assertTrue(result);

        List<Object[]> rows = jdbcHandler.scan(Collections.emptyList());
        assertEquals(3, rows.size());
    }

    @Test
    void testWithConnectionPoolConfig() {
        jdbcConfig.connectionPoolSize = 5;
        jdbcConfig.connectionPoolMinIdle = 1;
        jdbcConfig.connectionPoolName = "test-pool";

        JdbcHandler handlerWithPool = new JdbcHandler(jdbcConfig);
        List<Object[]> rows = handlerWithPool.scan(Collections.emptyList());
        assertEquals(3, rows.size());
    }
}
