package com.sqlrec.db;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.schema.CacheManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MetadataAccessFactoryFileDdlTest {
    @Test
    void executorsCaptureDdlPolicyAndRestartRestoresOnlyFileDefinitions(@TempDir Path directory) throws Exception {
        String seed = "CREATE TABLE seeded (id INT) WITH ('connector'='not_installed');";
        Files.writeString(directory.resolve("schema.sql"), seed);
        String oldDirectory = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        boolean oldFlag = SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.getDefaultValue();
        Field instance = MetadataAccessFactory.class.getDeclaredField("instance");
        instance.setAccessible(true);
        Object previous = instance.get(null);
        try {
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(directory.toString());
            SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(true);
            instance.set(null, null);
            CalciteSchemaFactory.setGlobalSchema(null);
            CacheManager.invalidateAll();
            MetadataAccess metadata = MetadataAccessFactory.getInstance();
            SqlExecutor executor = new SqlExecutor();
            executor.executeSql("DROP TABLE seeded");
            executor.executeSql("CREATE SQL FUNCTION runtime_function");
            executor.executeSql("RETURN SELECT 9 AS id");
            executor.executeSql("CREATE API runtime_api WITH runtime_function");
            assertNotNull(metadata.getSqlFunction("runtime_function"));
            assertNotNull(metadata.getSqlApi("runtime_api"));
            SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(false);
            assertSame(metadata, MetadataAccessFactory.getInstance());
            var disabled = new SqlExecutor();
            assertThrows(UnsupportedOperationException.class, () -> disabled.executeSql("CREATE TABLE denied (id INT)"));
            executor.executeSql("CREATE TABLE runtime_only (id INT)");
            executor.executeSql("FLUSH");
            assertEquals(List.of("runtime_only"), metadata.getTables("default").stream().map(table -> table.getTableName()).toList());
            assertEquals(seed, Files.readString(directory.resolve("schema.sql")));

            instance.set(null, null);
            CalciteSchemaFactory.setGlobalSchema(null);
            CacheManager.invalidateAll();
            MetadataAccess restarted = MetadataAccessFactory.getInstance();
            assertEquals(List.of("seeded"), restarted.getTables("default").stream().map(table -> table.getTableName()).toList());
            assertTrue(restarted.getSqlFunctionList().isEmpty());
            assertTrue(restarted.getSqlApiList().isEmpty());
            SqlExecutor readOnly = new SqlExecutor();
            assertThrows(UnsupportedOperationException.class, () -> readOnly.executeSql("DROP TABLE seeded"));
            assertThrows(UnsupportedOperationException.class, () -> readOnly.executeSql("CREATE API a WITH f"));
            assertThrows(UnsupportedOperationException.class, () -> readOnly.executeSql("CREATE SQL FUNCTION f"));
        } finally {
            instance.set(null, previous);
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(oldDirectory);
            SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(oldFlag);
            CalciteSchemaFactory.setGlobalSchema(null);
            CacheManager.invalidateAll();
        }
    }
}
