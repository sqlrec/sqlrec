package com.sqlrec.executor;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.remote.FlinkHiveDdlAdapter;
import com.sqlrec.schema.CacheManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlExecutorLocalMetadataTest {
    @BeforeEach
    void setUp() {
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
    }

    @AfterEach
    void tearDown() {
        CalciteSchemaFactory.setGlobalSchema(null);
    }

    @Test
    void invalidatesCachesAfterSuccessfulMetadataDdl() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        try (var factory = mockStatic(MetadataAccessFactory.class);
             var caches = mockStatic(CacheManager.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            var result = new SqlExecutor().executeSqlAsync("DROP TABLE t");
            assertEquals("metadata DDL completed", result.getEnumerable().toList().get(0)[0]);
            verify(metadata).executeMetadataDdl(isA(SqlDropTable.class), eq("default"));
            caches.verify(CacheManager::invalidateAll);
        }
    }

    @Test
    void validationFailureInvalidatesWithoutRetryingAndPreservesTheError() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var failure = new IllegalArgumentException("table exists");
        doThrow(failure).when(metadata).executeMetadataDdl(any(SqlNode.class), eq("default"));
        try (var factory = mockStatic(MetadataAccessFactory.class);
             var caches = mockStatic(CacheManager.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            var executor = new SqlExecutor();
            assertSame(failure, assertThrows(IllegalArgumentException.class,
                    () -> executor.executeSqlAsync("CREATE TABLE t (id INT)")));
            verify(metadata).executeMetadataDdl(any(SqlNode.class), eq("default"));
            caches.verify(CacheManager::invalidateAll);
        }
    }

    @Test
    void uncertainWriteOutcomeInvalidatesWithoutRetryingTheWrite() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var cause = new TTransportException("response lost");
        var failure = new IllegalStateException("METADATA_WRITE_OUTCOME_UNKNOWN", cause);
        doThrow(failure).when(metadata).executeMetadataDdl(any(SqlNode.class), eq("default"));
        try (var factory = mockStatic(MetadataAccessFactory.class);
             var caches = mockStatic(CacheManager.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            var executor = new SqlExecutor();
            var actual = assertThrows(IllegalStateException.class, () -> executor.executeSqlAsync("DROP TABLE t"));
            assertSame(failure, actual);
            assertSame(cause, actual.getCause());
            verify(metadata).executeMetadataDdl(any(SqlNode.class), eq("default"));
            caches.verify(CacheManager::invalidateAll);
        }
    }

    @Test
    void metadataLookupFailurePreservesTheErrorWithoutInvalidatingCaches() {
        var failure = new IllegalStateException("metadata initialization failed");
        try (var factory = mockStatic(MetadataAccessFactory.class);
             var caches = mockStatic(CacheManager.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenThrow(failure);
            var executor = new SqlExecutor();
            assertSame(failure, assertThrows(IllegalStateException.class, () -> executor.executeSqlAsync("DROP TABLE t")));
            factory.verify(MetadataAccessFactory::getInstance);
            caches.verifyNoInteractions();
        }
    }

    @Test
    void routesPersistentDdlToMetadataWithTheSessionDatabase() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        try (var factory = mockStatic(MetadataAccessFactory.class);
             var caches = mockStatic(CacheManager.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            var executor = new SqlExecutor();
            executor.executeSqlAsync("USE review");
            String[] statements = {
                    "CREATE DATABASE IF NOT EXISTS review_db",
                    "ALTER DATABASE review_db SET ('comment'='review')",
                    "DROP DATABASE IF EXISTS review_db CASCADE",
                    "CREATE TABLE t (id BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ('connector'='redis')",
                    "CREATE TABLE copied LIKE t",
                    "ALTER TABLE t SET ('cache-ttl'='0')",
                    "DROP TABLE t",
                    "CREATE FUNCTION f AS 'example.ReviewUdf' LANGUAGE JAVA USING JAR 'file:///tmp/review.jar'",
                    "ALTER FUNCTION f AS 'example.ReviewUdfV2'",
                    "DROP FUNCTION f"
            };
            for (String sql : statements) {
                clearInvocations(metadata);
                assertNotNull(executor.executeSqlAsync(sql), sql);
                var node = ArgumentCaptor.forClass(SqlNode.class);
                verify(metadata).executeMetadataDdl(node.capture(), eq("review"));
                assertEquals(CompileManager.parseSql(sql).toString(), node.getValue().toString(), sql);
                verifyNoMoreInteractions(metadata);
            }
            caches.verify(CacheManager::invalidateAll, times(statements.length));
        }
    }

    @Test
    void leavesJobDdlViewsAndTemporaryFunctionsForTheRemoteRoute() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        try (var factory = mockStatic(MetadataAccessFactory.class);
             var caches = mockStatic(CacheManager.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            var executor = new SqlExecutor();
            for (String sql : new String[]{"CREATE TABLE t AS SELECT 1 AS id",
                    "CREATE OR REPLACE TABLE t AS SELECT 1 AS id",
                    "DROP TEMPORARY TABLE temp_t",
                    "CREATE TEMPORARY FUNCTION temp_f AS 'example.ReviewUdf'",
                    "ALTER TEMPORARY FUNCTION temp_f AS 'example.ReviewUdfV2'",
                    "DROP TEMPORARY FUNCTION temp_f",
                    "CREATE TEMPORARY SYSTEM FUNCTION system_f AS 'example.ReviewUdf'",
                    "CREATE VIEW v AS SELECT 1 AS id", "DROP VIEW v"}) {
                assertNull(executor.executeSqlAsync(sql), sql);
            }
            verifyNoInteractions(metadata);
            caches.verifyNoInteractions();
        }
    }

    @Test
    void functionCompilerRetainsOwnershipOfItsBody() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        try (var factory = mockStatic(MetadataAccessFactory.class);
             var caches = mockStatic(CacheManager.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            var executor = new SqlExecutor();
            assertNotNull(executor.executeSqlAsync("CREATE SQL FUNCTION review_function"));
            var error = assertThrows(Exception.class, () -> executor.executeSqlAsync("CREATE TABLE t (id INT)"));
            assertEquals("sql is not compilable", error.getMessage());
            verifyNoInteractions(metadata);
            caches.verifyNoInteractions();
        }
    }

    @Test
    void rejectsUnsupportedPersistentDdlLocallyWithoutGatewayFallback() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        try (var adapter = new FlinkHiveDdlAdapter();
             var factory = mockStatic(MetadataAccessFactory.class);
             var caches = mockStatic(CacheManager.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            doAnswer(call -> {
                adapter.executeDdl(call.getArgument(0, SqlNode.class), call.getArgument(1, String.class));
                return null;
            }).when(metadata).executeMetadataDdl(any(SqlNode.class), eq("default"));
            var executor = new SqlExecutor();
            String[] statements = {"CREATE TABLE t (id INT, extra AS id + 1)",
                    "ALTER TABLE t ADD extra AS id + 1", "ALTER TABLE t DROP WATERMARK",
                    "CREATE TABLE other.defaultdb.t (id INT)"};
            for (String sql : statements) {
                assertThrows(UnsupportedOperationException.class, () -> executor.executeSqlAsync(sql), sql);
            }
            verify(metadata, times(statements.length)).executeMetadataDdl(any(SqlNode.class), eq("default"));
            caches.verify(CacheManager::invalidateAll, times(statements.length));
        }
    }

    @Test
    void useDoesNotRequireDatabaseInSessionSchema() throws Exception {
        SqlExecutor executor = new SqlExecutor();
        assertNotNull(executor.executeSqlAsync("USE REVIEW"));
        assertEquals("REVIEW", executor.getDefaultSchema());
        assertNotNull(executor.executeSqlAsync("USE hive.review"));
        assertEquals("review", executor.getDefaultSchema());
        assertThrows(IllegalArgumentException.class, () -> executor.executeSqlAsync("USE other.review"));
        assertEquals("review", executor.getDefaultSchema());
    }

    @Test
    void rejectsCatalogSwitchingAndTemporaryTableCreation() {
        SqlExecutor executor = new SqlExecutor();
        for (String sql : new String[]{
                "USE CATALOG hive",
                "USE CATALOG other_catalog",
                "CREATE TEMPORARY TABLE tmp (id BIGINT) WITH ('connector' = 'datagen')",
                "CREATE TEMPORARY TABLE tmp AS SELECT 1 AS id",
                "CREATE TEMPORARY TABLE tmp LIKE source_table",
                "CREATE OR REPLACE TEMPORARY TABLE tmp AS SELECT 1 AS id"
        }) {
            assertThrows(UnsupportedOperationException.class, () -> executor.executeSqlAsync(sql), sql);
        }
        assertEquals("default", executor.getDefaultSchema());
    }

    @Test
    void setDoesNotSaveStatementTerminatorAsValue() throws Exception {
        SqlExecutor executor = new SqlExecutor();
        assertNotNull(executor.executeSqlAsync("SET parallelism.default=2;  "));
        assertEquals("2", executor.getSessionSettings().get("parallelism.default"));
        assertEquals("2", executor.getExecuteContext().getVariable("parallelism.default"));
        assertNotNull(executor.executeSqlAsync("SET 'custom.setting' = 'value;'"));
        assertEquals("value;", executor.getSessionSettings().get("custom.setting"));
    }

    @Test
    void setPreservesLiteralValuesAndResetOnlyRemovesSavedOverrides() throws Exception {
        SqlExecutor executor = new SqlExecutor();
        executor.setExecuteParams(Map.of("request.param", "retained"));
        assertNotNull(executor.executeSqlAsync("SET 'custom.setting' = 'it''s quoted'"));
        assertEquals("it's quoted", executor.getSessionSettings().get("custom.setting"));
        assertEquals("it's quoted", executor.getExecuteContext().getVariable("custom.setting"));
        assertNull(executor.executeSqlAsync("SET"));
        assertNull(executor.executeSqlAsync("RESET 'custom.setting'"));
        assertTrue(executor.getSessionSettings().containsKey("custom.setting"));
        executor.resetSessionSettings("custom.setting");
        assertNull(executor.getExecuteContext().getVariable("custom.setting"));
        executor.executeSqlAsync("SET 'another.setting' = 'value'");
        executor.resetSessionSettings(null);
        assertTrue(executor.getSessionSettings().isEmpty());
        assertNull(executor.getExecuteContext().getVariable("another.setting"));
        assertEquals("retained", executor.getExecuteContext().getVariable("request.param"));
    }

    @Test
    void rejectsSqlFunctionDdlInLocalMetadataMode(@TempDir Path sqlDir) {
        String previous = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        try {
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(sqlDir.toString());
            SqlExecutor executor = new SqlExecutor();

            assertThrows(UnsupportedOperationException.class,
                    () -> executor.executeSqlAsync("CREATE SQL FUNCTION test_function"));
            assertThrows(UnsupportedOperationException.class,
                    () -> executor.executeSqlAsync("CREATE OR REPLACE SQL FUNCTION test_function"));
        } finally {
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(previous);
        }
    }
}
