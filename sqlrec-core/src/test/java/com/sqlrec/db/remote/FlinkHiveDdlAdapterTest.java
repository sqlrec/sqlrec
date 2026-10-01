package com.sqlrec.db.remote;

import com.sqlrec.compiler.CompileManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class FlinkHiveDdlAdapterTest {
    @Test
    void requiresCurrentDatabaseOnlyForUnqualifiedDdlReferences() throws Exception {
        for (String sql : new String[]{"DROP TABLE t", "CREATE FUNCTION f AS 'example.Udf'",
                "CREATE TABLE t (id INT)", "ALTER TABLE t SET ('path'='x')",
                "CREATE TABLE other.copy LIKE source"}) {
            assertTrue(FlinkHiveDdlAdapter.requiresCurrentDatabase(CompileManager.parseSql(sql)), sql);
        }
        for (String sql : new String[]{"CREATE DATABASE db", "DROP DATABASE db",
                "DROP TABLE other.t", "CREATE FUNCTION other.f AS 'example.Udf'",
                "CREATE TABLE other.t (id INT)", "CREATE TABLE hive.other.copy LIKE hive.other.source",
                "ALTER TABLE other.t SET ('path'='x')", "ALTER TABLE other.t RENAME TO t2"}) {
            assertFalse(FlinkHiveDdlAdapter.requiresCurrentDatabase(CompileManager.parseSql(sql)), sql);
        }
    }

    @Test
    void rejectsTemporaryAndJobProducingDefinitionsBeforeOpeningHms() throws Exception {
        for (String sql : new String[]{"CREATE TEMPORARY TABLE t (id INT)",
                "CREATE TEMPORARY FUNCTION f AS 'example.Udf'", "CREATE TABLE t AS SELECT 1",
                "SELECT 1", "CREATE VIEW v AS SELECT 1", "USE CATALOG hive",
                "CREATE TABLE other.defaultdb.t (id INT)", "ALTER TABLE other.defaultdb.t SET ('path'='x')",
                "CREATE FUNCTION other.defaultdb.f AS 'example.Udf'", "CREATE DATABASE other.db"}) {
            assertThrows(UnsupportedOperationException.class,
                    () -> FlinkHiveDdlAdapter.requirePersistentDdl(CompileManager.parseSql(sql)), sql);
        }
        assertDoesNotThrow(() -> FlinkHiveDdlAdapter.requirePersistentDdl(
                CompileManager.parseSql("CREATE TABLE t (id INT)")));
        assertThrows(UnsupportedOperationException.class,
                () -> FlinkHiveDdlAdapter.objectPath(new String[]{"unconfigured", "default", "t"}, "default"));
    }

    @Test
    void rejectsComputedColumnsAndWatermarksBeforeOpeningHms() throws Exception {
        for (String sql : new String[]{"CREATE TABLE t (id INT, extra AS id + 1)",
                "CREATE TABLE t (ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)",
                "ALTER TABLE t ADD extra AS id + 1", "ALTER TABLE t ADD WATERMARK FOR ts AS ts"}) {
            assertThrows(UnsupportedOperationException.class,
                    () -> FlinkHiveDdlAdapter.executeDdl(sql, "default"), sql);
        }
    }
}
