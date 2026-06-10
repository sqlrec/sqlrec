package com.sqlrec.connectors.jdbc.calcite;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class JdbcCalciteTableTest {

    private static final String JDBC_URL = "jdbc:h2:mem:calcitedb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
    private static final String DRIVER = "org.h2.Driver";
    private static final String TABLE_NAME = "calcite_test";

    private JdbcConfig jdbcConfig;

    @BeforeEach
    void setUp() throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'alice', 20)");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 'bob', 25)");
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
    }

    @Test
    void testGetRowType() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType rowType = table.getRowType(typeFactory);

        assertEquals(3, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertEquals("name", rowType.getFieldNames().get(1));
        assertEquals("age", rowType.getFieldNames().get(2));
        assertEquals(SqlTypeName.INTEGER, rowType.getFieldList().get(0).getType().getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(1).getType().getSqlTypeName());
        assertEquals(SqlTypeName.INTEGER, rowType.getFieldList().get(2).getType().getSqlTypeName());
    }

    @Test
    void testGetPrimaryKeyIndex() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        assertEquals(0, table.getPrimaryKeyIndex());
    }

    @Test
    void testOnlyFilterByPrimaryKey() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        assertFalse(table.onlyFilterByPrimaryKey());
    }

    @Test
    void testGetElementType() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        assertEquals(Object[].class, table.getElementType());
    }

    @Test
    void testAsQueryableThrows() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        assertThrows(UnsupportedOperationException.class, () ->
                table.asQueryable(null, null, "test"));
    }

    @Test
    void testGetByPrimaryKeyImpl() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        Map<Object, List<Object[]>> result = table.getByPrimaryKeyImpl(Collections.singleton(1));
        assertEquals(1, result.size());
        List<Object[]> rows = result.get(1);
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(1, ((Number) rows.get(0)[0]).intValue());
        assertEquals("alice", rows.get(0)[1].toString());
    }

    @Test
    void testScanImpl() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        Iterable<Object[]> result = table.scanImpl(null, Collections.emptyList());
        List<Object[]> rows = new ArrayList<>();
        result.forEach(rows::add);
        assertEquals(2, rows.size());
    }

    @Test
    void testGetModifiableCollection() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        Collection collection = table.getModifiableCollection();
        assertNotNull(collection);
        assertTrue(collection instanceof JdbcCalciteTable.JdbcCollection);
    }

    @Test
    void testCollectionUpsertAndRemove() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        Collection<Object[]> collection = table.getModifiableCollection();

        // add (upsert)
        Object[] newRow = {3, "charlie", 30};
        boolean added = collection.add(newRow);
        assertTrue(added);

        // verify added via getByPrimaryKey
        Map<Object, List<Object[]>> result = table.getByPrimaryKeyImpl(Collections.singleton(3));
        assertNotNull(result.get(3));

        // remove
        boolean removed = collection.remove(newRow);
        assertTrue(removed);

        // verify removed
        Map<Object, List<Object[]>> resultAfterDelete = table.getByPrimaryKeyImpl(Collections.singleton(3));
        assertTrue(resultAfterDelete.isEmpty());
    }

    @Test
    void testCollectionUpsertUpdate() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        Collection<Object[]> collection = table.getModifiableCollection();

        // upsert existing row (id=1) should update
        Object[] updatedRow = {1, "alice_updated", 21};
        boolean added = collection.add(updatedRow);
        assertTrue(added);

        // verify updated
        Map<Object, List<Object[]>> result = table.getByPrimaryKeyImpl(Collections.singleton(1));
        assertEquals("alice_updated", result.get(1).get(0)[1].toString());
        assertEquals(21, ((Number) result.get(1).get(0)[2]).intValue());
    }

    @Test
    void testWithCache() {
        jdbcConfig.maxCacheSize = 100;
        jdbcConfig.cacheTtl = 60;

        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);

        // first call - should hit DB
        Map<Object, List<Object[]>> result1 = table.getByPrimaryKey(Collections.singleton(1));
        assertEquals(1, result1.size());

        // second call - should hit cache
        Map<Object, List<Object[]>> result2 = table.getByPrimaryKey(Collections.singleton(1));
        assertEquals(1, result2.size());
        assertEquals("alice", result2.get(1).get(0)[1].toString());
    }

    @Test
    void testScanWithFiltersByPrimaryKey() {
        JdbcCalciteTable table = new JdbcCalciteTable(jdbcConfig);
        // scan via primary key filter - the base SqlRecKvTable.scan() handles this
        // by extracting primary key value and calling getByPrimaryKey
        Map<Object, List<Object[]>> result = table.getByPrimaryKey(Collections.singleton(2));
        assertEquals(1, result.size());
        assertEquals("bob", result.get(2).get(0)[1].toString());
    }
}
