package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.connectors.jdbc.calcite.JdbcCalciteTable;
import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.sqlrec.connectors.jdbc.handler.JdbcHandler;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcQueryRegressionTest {
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
            JdbcHandler.closeAllDataSources();
        }
    }

    private void assertQuery(CalciteSchema schema, String sql, List<?> expected) throws Exception {
        var bindable = NormalSqlCompiler.getNormalSqlBindable(sql, schema, Consts.DEFAULT_SCHEMA_NAME);
        var rows = bindable.bind(schema, new ExecuteContextImpl(schema)).toList();
        assertEquals(expected, rows.stream().map(row -> row[0]).toList(), sql);
    }
}
