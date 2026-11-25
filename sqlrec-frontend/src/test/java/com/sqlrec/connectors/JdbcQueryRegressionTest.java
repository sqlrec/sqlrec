package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.connectors.jdbc.calcite.JdbcCalciteTable;
import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.sqlrec.connectors.jdbc.utils.JdbcDataSourceUtils;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcQueryRegressionTest {
    @Test
    void insertsUpdatesAndDeletesSingleColumnRows() throws Exception {
        JdbcConfig config = itemsConfig("single_column_dml");
        config.fieldSchemas = List.of(new FieldSchema("id", "INTEGER"));
        try (var connection = DriverManager.getConnection(config.url);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE items (id INT PRIMARY KEY)");
            CalciteSchema schema = itemsSchema(config);

            assertQuery(schema, "INSERT INTO items VALUES (1), (2)", List.of(2L));
            assertQuery(schema, "INSERT INTO items SELECT id + 2 FROM items WHERE id = 1", List.of(1L));
            assertQuery(schema, "SELECT id FROM items ORDER BY id", List.of(1, 2, 3));
            assertQuery(schema, "UPDATE items SET id = id + 10 WHERE id < 3", List.of(2L));
            assertQuery(schema, "SELECT id FROM items ORDER BY id", List.of(3, 11, 12));
            assertQuery(schema, "DELETE FROM items WHERE id = 11", List.of(1L));
            assertQuery(schema, "UPDATE items SET id = 99 WHERE id = 11", List.of(0L));
            assertQuery(schema, "DELETE FROM items WHERE id = 11", List.of(0L));
            assertQuery(schema, "SELECT id FROM items ORDER BY id", List.of(3, 12));
        } finally {
            JdbcDataSourceUtils.closeAll();
        }
    }

    @Test
    void preservesSqlSemanticsThroughPlanningAndJdbcFiltering() throws Exception {
        JdbcConfig config = new JdbcConfig();
        config.url = "jdbc:h2:mem:query_regression";
        config.tableName = "items";
        config.primaryKey = "name";
        config.primaryKeyIndex = 1;
        config.fieldSchemas = List.of(new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"), new FieldSchema("active", "BOOLEAN"));

        try (var connection = DriverManager.getConnection(config.url);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE items (id INT, name VARCHAR PRIMARY KEY, active BOOLEAN)");
            statement.execute("INSERT INTO items VALUES (1,'Alice',TRUE),(2,'Bob',FALSE),"
                    + "(3,'Carol',TRUE),(NULL,'NullId',NULL)");
            CalciteSchema schema = CalciteSchema.createRootSchema(false);
            schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema()).add("items", new JdbcCalciteTable(config));

            assertQuery(schema, "SELECT COUNT(id) FROM items", List.of(3L));
            Map<String, List<String>> cases = Map.ofEntries(
                    Map.entry("id IS NULL", List.of("NullId")),
                    Map.entry("id=1 OR id IS NULL", List.of("Alice", "NullId")),
                    Map.entry("id BETWEEN 1 AND 2", List.of("Alice", "Bob")),
                    Map.entry("id NOT IN (1,2)", List.of("Carol")),
                    Map.entry("name IN ('Alice','Bob')", List.of("Alice", "Bob")),
                    Map.entry("active", List.of("Alice", "Carol")),
                    Map.entry("NOT active", List.of("Bob")),
                    Map.entry("active IS NOT TRUE", List.of("Bob", "NullId")),
                    Map.entry("CHAR_LENGTH(name)>3", List.of("Alice", "Carol", "NullId")),
                    Map.entry("name='Alice' OR CHAR_LENGTH(name)>3", List.of("Alice", "Carol", "NullId")),
                    Map.entry("NOT (name='Alice' OR CHAR_LENGTH(name)>3)", List.of("Bob")),
                    Map.entry("(id BETWEEN 1 AND 2) OR id IS NULL", List.of("Alice", "Bob", "NullId")));
            for (var entry : cases.entrySet()) {
                assertQuery(schema, "SELECT name FROM items WHERE " + entry.getKey() + " ORDER BY name", entry.getValue());
            }
        } finally {
            JdbcDataSourceUtils.closeAll();
        }
    }

    @Test
    void failedBatchInsertRollsBackEarlierRows() throws Exception {
        JdbcConfig config = itemsConfig("batch_rollback");
        try (var connection = DriverManager.getConnection(config.url);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE items (id INT PRIMARY KEY, category INT UNIQUE)");
            statement.execute("INSERT INTO items VALUES (1,10),(2,20)");
            CalciteSchema schema = itemsSchema(config);

            assertThrows(RuntimeException.class,
                    () -> execute(schema, "INSERT INTO items VALUES (3,30),(4,20)"));
            assertQuery(schema, "SELECT id FROM items ORDER BY id", List.of(1, 2));

            assertQuery(schema, "INSERT INTO items VALUES (3,30),(4,40)", List.of(2L));
            assertQuery(schema, "SELECT id FROM items ORDER BY id", List.of(1, 2, 3, 4));
        } finally {
            JdbcDataSourceUtils.closeAll();
        }
    }

    @Test
    void updateChangesPrimaryKeyInPlaceAndKeepsBatchAtomic() throws Exception {
        JdbcConfig config = itemsConfig("update_primary_key");
        try (var connection = DriverManager.getConnection(config.url);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE items (id INT PRIMARY KEY, category INT)");
            statement.execute("INSERT INTO items VALUES (1,10),(2,20),(4,40)");
            statement.execute("CREATE TABLE refs (item_id INT REFERENCES items(id) ON UPDATE CASCADE)");
            statement.execute("INSERT INTO refs VALUES (2)");
            CalciteSchema schema = itemsSchema(config);

            assertThrows(RuntimeException.class,
                    () -> execute(schema, "UPDATE items SET id = id + 2 WHERE id <= 2"));
            assertQuery(schema, "SELECT id FROM items ORDER BY id", List.of(1, 2, 4));

            assertQuery(schema, "UPDATE items SET id = 100, category = 21 WHERE id = 2", List.of(1L));
            assertQuery(schema, "SELECT id FROM items WHERE id = 2", List.of());
            assertQuery(schema, "SELECT category FROM items WHERE id = 100", List.of(21));
            try (var refs = statement.executeQuery("SELECT item_id FROM refs")) {
                assertTrue(refs.next());
                assertEquals(100, refs.getInt(1));
            }
            assertQuery(schema, "UPDATE items SET category = 30 WHERE id = 1", List.of(1L));
            assertQuery(schema, "SELECT category FROM items WHERE id = 1", List.of(30));
        } finally {
            JdbcDataSourceUtils.closeAll();
        }
    }

    @Test
    void updateMovesMultipleStringKeysAndInvalidatesCachedRows() throws Exception {
        JdbcConfig config = itemsConfig("update_cached_keys");
        config.primaryKey = "name";
        config.primaryKeyIndex = 1;
        config.fieldSchemas = List.of(new FieldSchema("category", "INTEGER"), new FieldSchema("name", "VARCHAR"));
        config.maxCacheSize = 10;
        config.cacheTtl = 60;

        try (var connection = DriverManager.getConnection(config.url);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE items (category INT, name VARCHAR PRIMARY KEY, extra VARCHAR)");
            statement.execute("INSERT INTO items VALUES (10,'Alice','keep'),(20,'Bob','also keep'),(30,'Carol',NULL)");
            CalciteSchema schema = CalciteSchema.createRootSchema(false);
            var table = new JdbcCalciteTable(config);
            schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema()).add("items", table);
            Set<Object> oldKeys = Set.of("Alice", "Bob", "Carol");
            table.getByPrimaryKey(oldKeys);
            assertEquals(oldKeys, table.getCachedByPrimaryKey(oldKeys).keySet());

            assertQuery(schema, "UPDATE items SET name = name || '-new', category = category + 1 WHERE category < 30",
                    List.of(2L));
            assertEquals(Set.of("Carol"), table.getCachedByPrimaryKey(oldKeys).keySet());
            assertQuery(schema, "SELECT name FROM items ORDER BY name", List.of("Alice-new", "Bob-new", "Carol"));
            assertQuery(schema, "SELECT category FROM items WHERE name IN ('Alice-new','Bob-new') ORDER BY name",
                    List.of(11, 21));
            assertQuery(schema, "UPDATE items SET name = 'unused' WHERE name = 'Alice'", List.of(0L));

            try (var rows = statement.executeQuery("SELECT extra FROM items WHERE name = 'Alice-new'")) {
                assertTrue(rows.next());
                assertEquals("keep", rows.getString(1));
            }
        } finally {
            JdbcDataSourceUtils.closeAll();
        }
    }

    private static JdbcConfig itemsConfig(String database) {
        JdbcConfig config = new JdbcConfig();
        config.url = "jdbc:h2:mem:" + database;
        config.tableName = "items";
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = List.of(new FieldSchema("id", "INTEGER"), new FieldSchema("category", "INTEGER"));
        return config;
    }

    private static CalciteSchema itemsSchema(JdbcConfig config) {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema()).add("items", new JdbcCalciteTable(config));
        return schema;
    }

    private static void assertQuery(CalciteSchema schema, String sql, List<?> expected) throws Exception {
        assertEquals(expected, execute(schema, sql).stream().map(row -> row[0]).toList(), sql);
    }

    private static List<Object[]> execute(CalciteSchema schema, String sql) throws Exception {
        var bindable = NormalSqlCompiler.getNormalSqlBindable(sql, schema, Consts.DEFAULT_SCHEMA_NAME);
        return bindable.bind(schema, new ExecuteContextImpl(schema)).toList();
    }
}
