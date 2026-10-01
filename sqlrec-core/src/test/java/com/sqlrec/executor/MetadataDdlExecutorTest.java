package com.sqlrec.executor;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.schema.CacheManager;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

class MetadataDdlExecutorTest {
    @Test
    void invalidatesCachesAfterSuccessfulDdl() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        try (var caches = mockStatic(CacheManager.class)) {
            MetadataDdlExecutor.execute(metadata, "DROP TABLE t", "default");
            verify(metadata).executeMetadataDdl("DROP TABLE t", "default");
            caches.verify(CacheManager::invalidateAll);
        }
    }

    @Test
    void validationFailureInvalidatesWithoutRetryingAndPreservesTheError() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        IllegalArgumentException failure = new IllegalArgumentException("table exists");
        doThrow(failure)
                .when(metadata).executeMetadataDdl("CREATE TABLE t (id INT)", "default");
        try (var caches = mockStatic(CacheManager.class)) {
            assertSame(failure, assertThrows(IllegalArgumentException.class,
                    () -> MetadataDdlExecutor.execute(metadata, "CREATE TABLE t (id INT)", "default")));
            verify(metadata).executeMetadataDdl("CREATE TABLE t (id INT)", "default");
            caches.verify(CacheManager::invalidateAll);
        }
    }

    @Test
    void uncertainWriteOutcomeInvalidatesWithoutRetryingTheWrite() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        TTransportException cause = new TTransportException("response lost");
        IllegalStateException failure = new IllegalStateException("METADATA_WRITE_OUTCOME_UNKNOWN", cause);
        doThrow(failure)
                .when(metadata).executeMetadataDdl("DROP TABLE t", "default");
        try (var caches = mockStatic(CacheManager.class)) {
            IllegalStateException actual = assertThrows(IllegalStateException.class,
                    () -> MetadataDdlExecutor.execute(metadata, "DROP TABLE t", "default"));
            assertSame(failure, actual);
            assertSame(cause, actual.getCause());
            verify(metadata).executeMetadataDdl("DROP TABLE t", "default");
            caches.verify(CacheManager::invalidateAll);
        }
    }

    @Test
    void routesPersistentMetadataWithoutRoutingComputationsOrSqlrecFunctionBodies() throws Exception {
        for (String sql : new String[]{
                "CREATE DATABASE IF NOT EXISTS review_db",
                "ALTER DATABASE review_db SET ('comment'='review')",
                "DROP DATABASE IF EXISTS review_db CASCADE",
                "CREATE TABLE t (id BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ('connector'='redis')",
                "ALTER TABLE t SET ('cache-ttl'='0')",
                "DROP TABLE t",
                "CREATE FUNCTION f AS 'example.ReviewUdf' LANGUAGE JAVA USING JAR 'file:///tmp/review.jar'",
                "ALTER FUNCTION f AS 'example.ReviewUdfV2'",
                "DROP FUNCTION f"}) {
            assertTrue(MetadataDdlExecutor.handles(CompileManager.parseSql(sql)), sql);
        }
        assertFalse(MetadataDdlExecutor.handles(CompileManager.parseSql("CREATE TABLE t AS SELECT 1 AS id")));
        assertFalse(MetadataDdlExecutor.handles(CompileManager.parseSql("CREATE SQL FUNCTION review_function")));
        assertFalse(MetadataDdlExecutor.handles(CompileManager.parseSql("USE `default`")));
        assertFalse(MetadataDdlExecutor.handles(CompileManager.parseSql("SELECT 1")));
        for (String sql : new String[]{
                "CREATE TEMPORARY TABLE temp_t (id BIGINT) WITH ('connector'='datagen')",
                "DROP TEMPORARY TABLE temp_t",
                "CREATE TEMPORARY FUNCTION temp_f AS 'example.ReviewUdf'",
                "ALTER TEMPORARY FUNCTION temp_f AS 'example.ReviewUdfV2'",
                "DROP TEMPORARY FUNCTION temp_f",
                "CREATE TEMPORARY SYSTEM FUNCTION system_f AS 'example.ReviewUdf'",
                "CREATE VIEW v AS SELECT 1 AS id",
                "DROP VIEW v"}) {
            assertFalse(MetadataDdlExecutor.handles(CompileManager.parseSql(sql)), sql);
        }
    }
}
