package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlUtilsTest {

    private final List<FieldSchema> fieldSchemas = Arrays.asList(
            new FieldSchema("id", "INTEGER"),
            new FieldSchema("name", "VARCHAR"),
            new FieldSchema("age", "INTEGER")
    );

    private static final String PG_URL = "jdbc:postgresql://localhost:5432/test";
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/test";
    private static final String H2_URL = "jdbc:h2:mem:test";

    private RexBuilder rexBuilder;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    void setUp() {
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
    }

    @Test
    void testSelectWithoutFilters() {
        SqlStatement statement = SqlUtils.select(PG_URL, "users", fieldSchemas, null);
        assertEquals("SELECT id, name, age FROM users", statement.getSql());
        assertTrue(statement.getParameters().isEmpty());
    }

    @Test
    void testSelectWithEmptyFilters() {
        SqlStatement statement = SqlUtils.select(PG_URL, "users", fieldSchemas, Collections.emptyList());
        assertEquals("SELECT id, name, age FROM users", statement.getSql());
        assertTrue(statement.getParameters().isEmpty());
    }

    @Test
    void testSelectWithFiltersCombinesWhereFragmentAndParameters() {
        RexInputRef nameRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, nameRef, literal);

        SqlStatement statement = SqlUtils.select(PG_URL, "users", fieldSchemas, Collections.singletonList(filter));
        // values travel as parameters, never inlined into the SQL text
        assertEquals("SELECT id, name, age FROM users WHERE name = ?", statement.getSql());
        assertEquals(Collections.singletonList("Bob"), statement.getParameters());
    }

    @Test
    void testSelectByPrimaryKey() {
        SqlStatement template = SqlUtils.selectByPrimaryKey(PG_URL, "users", fieldSchemas, "id", 3);
        assertEquals("SELECT id, name, age FROM users WHERE id IN (?, ?, ?)", template.getSql());
        assertEquals(0, template.getParameterCount());

        List<Object> keys = Arrays.asList(1, 2, 3);
        SqlStatement bound = template.withParameters(keys);
        assertEquals(template.getSql(), bound.getSql());
        assertEquals(keys, bound.getParameters());
    }

    @Test
    void testSelectByPrimaryKeyRejectsNonPositiveKeyCount() {
        assertThrows(IllegalArgumentException.class,
                () -> SqlUtils.selectByPrimaryKey(PG_URL, "users", fieldSchemas, "id", 0));
        assertThrows(IllegalArgumentException.class,
                () -> SqlUtils.selectByPrimaryKey(PG_URL, "users", fieldSchemas, "id", -1));
    }

    @Test
    void testUpsertPostgreSql() {
        SqlStatement statement = SqlUtils.upsert(PG_URL, "users", fieldSchemas, "id");
        assertEquals(
                "INSERT INTO users (id, name, age) VALUES (?, ?, ?) " +
                        "ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name, age = EXCLUDED.age",
                statement.getSql());
        // row template: parameters are supplied per row via withParameters
        assertEquals(0, statement.getParameterCount());
    }

    @Test
    void testUpsertMySql() {
        SqlStatement statement = SqlUtils.upsert(MYSQL_URL, "users", fieldSchemas, "id");
        assertEquals(
                "INSERT INTO users (id, name, age) VALUES (?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE id = VALUES(id), name = VALUES(name), age = VALUES(age)",
                statement.getSql());
    }

    @Test
    void testUpsertH2() {
        SqlStatement statement = SqlUtils.upsert(H2_URL, "users", fieldSchemas, "id");
        assertEquals("MERGE INTO users KEY (id) VALUES (?, ?, ?)", statement.getSql());
    }

    @Test
    void testDeleteByPrimaryKey() {
        SqlStatement template = SqlUtils.deleteByPrimaryKey(PG_URL, "users", "id");
        assertEquals("DELETE FROM users WHERE id = ?", template.getSql());

        SqlStatement bound = template.withParameters(Collections.singletonList(1));
        assertEquals(Collections.singletonList(1), bound.getParameters());
    }

    @Test
    void testUpsertQuotesUnsafeTableAndColumnNames() {
        // injection payloads in table/column names must end up quoted, not executed
        List<FieldSchema> evilSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name\"; DROP TABLE users; --", "VARCHAR")
        );

        SqlStatement pg = SqlUtils.upsert(PG_URL, "users; DROP TABLE users; --", evilSchemas, "id");
        assertEquals(
                "INSERT INTO \"users; DROP TABLE users; --\" (id, \"name\"\"; DROP TABLE users; --\") VALUES (?, ?) "
                        + "ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, "
                        + "\"name\"\"; DROP TABLE users; --\" = EXCLUDED.\"name\"\"; DROP TABLE users; --\"",
                pg.getSql());
        assertEquals(0, pg.getParameterCount());

        SqlStatement mysql = SqlUtils.upsert(MYSQL_URL, "users`x", evilSchemas, "id");
        assertTrue(mysql.getSql().startsWith("INSERT INTO `users``x` (id, `name\"; DROP TABLE users; --`)"));
        assertTrue(mysql.getSql().contains("ON DUPLICATE KEY UPDATE"));
    }

    @Test
    void testDeleteByPrimaryKeyQuotesUnsafeIdentifiers() {
        SqlStatement statement = SqlUtils.deleteByPrimaryKey(PG_URL, "users; DROP TABLE users; --", "id` OR 1=1; --");
        assertEquals("DELETE FROM \"users; DROP TABLE users; --\" WHERE \"id` OR 1=1; --\" = ?",
                statement.getSql());
        assertEquals(0, statement.getParameterCount());
    }

    @Test
    void testSelectByPrimaryKeyQuotesUnsafePrimaryKey() {
        SqlStatement statement = SqlUtils.selectByPrimaryKey(
                MYSQL_URL, "users", fieldSchemas, "id`; DELETE FROM users; --", 2);
        // backtick inside the payload is escaped by doubling
        assertEquals("SELECT id, name, age FROM users WHERE `id``; DELETE FROM users; --` IN (?, ?)",
                statement.getSql());
    }

    @Test
    void testWithParametersDoesNotMutateOriginal() {
        SqlStatement template = SqlUtils.deleteByPrimaryKey(PG_URL, "users", "id");
        List<Object> first = Collections.singletonList(1);
        List<Object> second = Collections.singletonList(2);

        SqlStatement firstBound = template.withParameters(first);
        SqlStatement secondBound = template.withParameters(second);

        assertEquals(first, firstBound.getParameters());
        assertEquals(second, secondBound.getParameters());
        // the template itself stays parameter-less and can be reused
        assertEquals(0, template.getParameterCount());
    }

    @Test
    void testParametersAreUnmodifiable() {
        SqlStatement statement = SqlUtils.deleteByPrimaryKey(PG_URL, "users", "id")
                .withParameters(Collections.singletonList(1));
        assertThrows(UnsupportedOperationException.class,
                () -> statement.getParameters().add(2));
    }

    @Test
    void testSelectQuotingWithInjectionAttemptInTableName() {
        SqlStatement statement = SqlUtils.select(PG_URL, "users; DROP TABLE x; --", fieldSchemas, null);
        assertEquals("SELECT id, name, age FROM \"users; DROP TABLE x; --\"", statement.getSql());
    }

    @Test
    void testQuoteIdentifierEscapesInjectionAttempt() {
        // A malicious identifier containing a double quote must be escaped so it cannot break out
        // of the quoted identifier and inject SQL.
        String quoted = SqlUtils.quoteIdentifier("col\"; DROP TABLE users; --", PG_URL);
        assertEquals("\"col\"\"; DROP TABLE users; --\"", quoted);
    }

    @Test
    void testQuoteIdentifierMysqlUsesBackticks() {
        String quoted = SqlUtils.quoteIdentifier("col`x", MYSQL_URL);
        assertEquals("`col``x`", quoted);
    }

    @Test
    void testQuoteIdentifierSafeIdentifierNotQuoted() {
        // Safe identifiers (alphanumeric + underscore) are returned as-is to preserve the
        // database's default case-folding behavior.
        assertEquals("users", SqlUtils.quoteIdentifier("users", PG_URL));
        assertEquals("id", SqlUtils.quoteIdentifier("id", PG_URL));
        assertEquals("_user_1", SqlUtils.quoteIdentifier("_user_1", PG_URL));
    }

    @Test
    void testQuoteIdentifierUnsafeIdentifierQuoted() {
        // Identifiers with special characters are quoted to prevent injection.
        assertEquals("\"user table\"", SqlUtils.quoteIdentifier("user table", PG_URL));
        assertEquals("\"user-name\"", SqlUtils.quoteIdentifier("user-name", PG_URL));
        assertEquals("\"1col\"", SqlUtils.quoteIdentifier("1col", PG_URL));
    }

    @Test
    void testQuoteIdentifierEmptyReturnsEmpty() {
        // empty identifiers pass through unchanged (a misconfiguration that will surface
        // as a SQL syntax error on the database side)
        assertEquals("", SqlUtils.quoteIdentifier("", PG_URL));
    }
}
