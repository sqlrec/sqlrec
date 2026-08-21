package com.sqlrec.connectors.jdbc.handler;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
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
        // null filters behave like no filters
        assertEquals(3, jdbcHandler.scan(null).size());
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
    void testScanWithFilter() {
        List<Object[]> rows = jdbcHandler.scan(Collections.singletonList(makeStringEqualsFilter("bob")));
        assertEquals(1, rows.size());
        assertEquals("bob", rows.get(0)[1].toString());
        assertEquals(25, ((Number) rows.get(0)[2]).intValue());
    }

    @Test
    void testScanWithNumericFilter() {
        List<Object[]> rows = jdbcHandler.scan(Collections.singletonList(makeNumericEqualsFilter(25)));
        assertEquals(1, rows.size());
        assertEquals("bob", rows.get(0)[1].toString());
    }

    @Test
    void testScanWithFilterValueContainingSingleQuote() {
        // A filter value containing a quote must match exactly and must not break out
        // of the SQL statement (values are bound as PreparedStatement parameters).
        jdbcHandler.upsert(new Object[]{4, "O'Brien", 40});

        List<Object[]> rows = jdbcHandler.scan(Collections.singletonList(makeStringEqualsFilter("O'Brien")));
        assertEquals(1, rows.size());
        assertEquals("O'Brien", rows.get(0)[1].toString());
    }

    @Test
    void testScanWithFilterInjectionAttempts() {
        // Classic SQL-injection payloads must be treated as plain data: they match no
        // row and must not modify or drop the table.
        jdbcHandler.upsert(new Object[]{4, "alice", 40});
        String[] payloads = {
                "x' OR '1'='1",
                "'; DROP TABLE test_table; --",
                "\\'; DROP TABLE test_table; --"
        };
        for (String payload : payloads) {
            List<Object[]> rows = jdbcHandler.scan(Collections.singletonList(makeStringEqualsFilter(payload)));
            assertTrue(rows.isEmpty(), "Injection payload must not match any row: " + payload);
        }
        // no payload must have executed: table still has all 4 rows
        assertEquals(4, jdbcHandler.scan(Collections.emptyList()).size());
    }

    @Test
    void testScanWithFilterInjectionPayloadStoredAsData() {
        // A payload-like value can legitimately be stored as data; scanning for it must
        // return exactly that row, proving the value is matched literally (and its
        // embedded statement is never executed).
        String payload = "\\'; DROP TABLE test_table; --";
        jdbcHandler.upsert(new Object[]{4, payload, 40});

        List<Object[]> rows = jdbcHandler.scan(Collections.singletonList(makeStringEqualsFilter(payload)));
        assertEquals(1, rows.size());
        assertEquals(4, ((Number) rows.get(0)[0]).intValue());

        // the DROP TABLE inside the value must not have executed
        assertEquals(4, jdbcHandler.scan(Collections.emptyList()).size());
    }

    private RexNode makeStringEqualsFilter(String value) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RexInputRef nameRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, nameRef, literal);
    }

    private RexNode makeNumericEqualsFilter(int value) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RexInputRef ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(value));
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ageRef, literal);
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
    void testGetByPrimaryKeyNullKeySet() {
        assertTrue(jdbcHandler.getByPrimaryKey(null).isEmpty());
    }

    @Test
    void testScanNonExistentTable() {
        jdbcConfig.tableName = "no_such_table";
        try {
            JdbcHandler handler = new JdbcHandler(jdbcConfig);
            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> handler.scan(Collections.emptyList()));
            assertTrue(ex.getMessage().contains("no_such_table"));
            // the underlying database error must be preserved as the cause
            assertTrue(ex.getCause() instanceof SQLException);
        } finally {
            jdbcConfig.tableName = TABLE_NAME;
        }
    }

    @Test
    void testUpsertColumnCountMismatchFailsFast() {
        // a row shorter than the declared schema has no unambiguous binding; the driver
        // must reject it and the failure must surface as a wrapped SQLException
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> jdbcHandler.upsert(new Object[]{1, "only-two-columns"}));
        assertTrue(ex.getCause() instanceof SQLException);

        // the table must be unchanged
        assertEquals(3, jdbcHandler.scan(Collections.emptyList()).size());
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
    void testUpsertBatch() {
        List<Object[]> rows = Arrays.asList(
                new Object[]{4, "dave", 35},
                new Object[]{5, "eve", 28},
                // same primary key as an existing row -> update, not insert
                new Object[]{1, "alice_updated", 21}
        );
        assertTrue(jdbcHandler.upsertBatch(rows));

        // 2 new rows + 1 update on an existing row -> total 5
        assertEquals(5, jdbcHandler.scan(Collections.emptyList()).size());
        Map<Object, List<Object[]>> updated = jdbcHandler.getByPrimaryKey(Collections.singleton(1));
        assertEquals("alice_updated", updated.get(1).get(0)[1].toString());
        Map<Object, List<Object[]>> inserted = jdbcHandler.getByPrimaryKey(Collections.singleton(5));
        assertEquals("eve", inserted.get(5).get(0)[1].toString());
    }

    @Test
    void testUpsertBatchEmpty() {
        assertTrue(jdbcHandler.upsertBatch(Collections.emptyList()));
        assertEquals(3, jdbcHandler.scan(Collections.emptyList()).size());
    }

    @Test
    void testDeleteBatch() {
        List<Object[]> rows = Arrays.asList(
                new Object[]{1, "alice", 20},
                new Object[]{2, "bob", 25}
        );
        assertTrue(jdbcHandler.deleteBatch(rows));

        assertEquals(1, jdbcHandler.scan(Collections.emptyList()).size());
        assertTrue(jdbcHandler.getByPrimaryKey(new HashSet<>(Arrays.asList(1, 2))).isEmpty());
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
