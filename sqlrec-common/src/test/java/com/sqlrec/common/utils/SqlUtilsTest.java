package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
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

    @Test
    void testBuildSelectSqlWithoutWhere() {
        String sql = SqlUtils.buildSelectSql(PG_URL, "users", fieldSchemas, null);
        assertEquals("SELECT id, name, age FROM users", sql);
    }

    @Test
    void testBuildSelectSqlWithWhere() {
        String sql = SqlUtils.buildSelectSql(PG_URL, "users", fieldSchemas, "id = 1");
        assertEquals("SELECT id, name, age FROM users WHERE id = 1", sql);
    }

    @Test
    void testBuildSelectSqlWithEmptyWhere() {
        String sql = SqlUtils.buildSelectSql(PG_URL, "users", fieldSchemas, "");
        assertEquals("SELECT id, name, age FROM users", sql);
    }

    @Test
    void testBuildWhereClauseNullFilters() {
        assertNull(SqlUtils.buildWhereClause(null, fieldSchemas));
    }

    @Test
    void testBuildWhereClauseEmptyFilters() {
        assertNull(SqlUtils.buildWhereClause(Collections.emptyList(), fieldSchemas));
    }

    @Test
    void testBuildPostgreSqlUpsertSql() {
        String sql = SqlUtils.buildPostgreSqlUpsertSql("users", fieldSchemas, "id");
        assertEquals(
                "INSERT INTO users (id, name, age) VALUES (?, ?, ?) " +
                        "ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name, age = EXCLUDED.age",
                sql);
    }

    @Test
    void testBuildMySqlUpsertSql() {
        String sql = SqlUtils.buildMySqlUpsertSql("users", fieldSchemas, "id");
        assertEquals(
                "INSERT INTO users (id, name, age) VALUES (?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE id = VALUES(id), name = VALUES(name), age = VALUES(age)",
                sql);
    }

    @Test
    void testBuildH2UpsertSql() {
        String sql = SqlUtils.buildH2UpsertSql("users", fieldSchemas, "id");
        assertEquals(
                "MERGE INTO users KEY (id) VALUES (?, ?, ?)",
                sql);
    }

    @Test
    void testBuildUpsertSqlPostgreSql() {
        String sql = SqlUtils.buildUpsertSql("jdbc:postgresql://localhost:5432/test", "users", fieldSchemas, "id");
        assertTrue(sql.contains("ON CONFLICT"));
    }

    @Test
    void testBuildUpsertSqlMySql() {
        String sql = SqlUtils.buildUpsertSql("jdbc:mysql://localhost:3306/test", "users", fieldSchemas, "id");
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    @Test
    void testBuildUpsertSqlH2() {
        String sql = SqlUtils.buildUpsertSql("jdbc:h2:mem:test", "users", fieldSchemas, "id");
        assertTrue(sql.contains("MERGE INTO"));
    }

    @Test
    void testBuildDeleteSql() {
        String sql = SqlUtils.buildDeleteSql(PG_URL, "users", "id");
        assertEquals("DELETE FROM users WHERE id = ?", sql);
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
        String quoted = SqlUtils.quoteIdentifier("col`x", "jdbc:mysql://localhost:3306/test");
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
}
