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

    @Test
    void testBuildSelectSqlWithoutWhere() {
        String sql = SqlUtils.buildSelectSql("users", fieldSchemas, null);
        assertEquals("SELECT id, name, age FROM users", sql);
    }

    @Test
    void testBuildSelectSqlWithWhere() {
        String sql = SqlUtils.buildSelectSql("users", fieldSchemas, "id = 1");
        assertEquals("SELECT id, name, age FROM users WHERE id = 1", sql);
    }

    @Test
    void testBuildSelectSqlWithEmptyWhere() {
        String sql = SqlUtils.buildSelectSql("users", fieldSchemas, "");
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
        String sql = SqlUtils.buildDeleteSql("users", "id");
        assertEquals("DELETE FROM users WHERE id = ?", sql);
    }
}
