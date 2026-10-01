package com.sqlrec.executor;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SqlExecutorLocalMetadataTest {
    @Test
    void useDoesNotRequireDatabaseInSessionSchema() throws Exception {
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
        try {
            SqlExecutor executor = new SqlExecutor();
            assertNotNull(executor.executeSqlAsync("USE REVIEW"));
            assertEquals("REVIEW", executor.getDefaultSchema());
            assertNotNull(executor.executeSqlAsync("USE hive.review"));
            assertEquals("review", executor.getDefaultSchema());
            assertThrows(IllegalArgumentException.class, () -> executor.executeSqlAsync("USE other.review"));
            assertEquals("review", executor.getDefaultSchema());
        } finally {
            CalciteSchemaFactory.setGlobalSchema(null);
        }
    }

    @Test
    void rejectsCatalogSwitchingAndTemporaryTableCreation() {
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
        try {
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
        } finally {
            CalciteSchemaFactory.setGlobalSchema(null);
        }
    }

    @Test
    void setDoesNotSaveStatementTerminatorAsValue() throws Exception {
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
        try {
            SqlExecutor executor = new SqlExecutor();
            assertNotNull(executor.executeSqlAsync("SET parallelism.default=2;  "));
            assertEquals("2", executor.getSessionSettings().get("parallelism.default"));
            assertEquals("2", executor.getExecuteContext().getVariable("parallelism.default"));
            assertNotNull(executor.executeSqlAsync("SET 'custom.setting' = 'value;'"));
            assertEquals("value;", executor.getSessionSettings().get("custom.setting"));
        } finally {
            CalciteSchemaFactory.setGlobalSchema(null);
        }
    }

    @Test
    void setPreservesLiteralValuesAndResetOnlyRemovesSavedOverrides() throws Exception {
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
        try {
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
        } finally {
            CalciteSchemaFactory.setGlobalSchema(null);
        }
    }

    @Test
    void rejectsSqlFunctionDdlInLocalMetadataMode(@TempDir Path sqlDir) {
        String previous = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        try {
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(sqlDir.toString());
            CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
            SqlExecutor executor = new SqlExecutor();

            assertThrows(UnsupportedOperationException.class,
                    () -> executor.executeSqlAsync("CREATE SQL FUNCTION test_function"));
            assertThrows(UnsupportedOperationException.class,
                    () -> executor.executeSqlAsync("CREATE OR REPLACE SQL FUNCTION test_function"));
        } finally {
            CalciteSchemaFactory.setGlobalSchema(null);
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(previous);
        }
    }
}
