package com.sqlrec.db;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.model.ServiceManager;
import com.sqlrec.schema.CacheManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockStatic;

class MetadataAccessFactoryFileDdlTest {
    private static final String MODEL = "CREATE MODEL model_review (x FLOAT) "
            + "WITH ('model'='external', 'MODEL_PATH'='/tmp/model_review');";
    private static final String SERVICE = "CREATE SERVICE svcreview ON MODEL model_review "
            + "WITH ('url'='http://example.invalid/predict');";

    @TempDir
    Path directory;

    private String previousDirectory;
    private boolean previousDdlFlag;
    private Field instance;
    private MetadataAccess previousInstance;

    @BeforeEach
    void useFileMetadata() throws Exception {
        previousDirectory = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        previousDdlFlag = SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.getDefaultValue();
        instance = MetadataAccessFactory.class.getDeclaredField("instance");
        instance.setAccessible(true);
        previousInstance = (MetadataAccess) instance.get(null);
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(directory.toString());
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(true);
        instance.set(null, null);
        CalciteSchemaFactory.setGlobalSchema(null);
        CacheManager.invalidateAll();
    }

    @AfterEach
    void restoreMetadata() throws Exception {
        instance.set(null, previousInstance);
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(previousDirectory);
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(previousDdlFlag);
        CalciteSchemaFactory.setGlobalSchema(null);
        CacheManager.invalidateAll();
    }

    @Test
    void executorsCaptureDdlPolicyAndRestartRestoresOnlyFileDefinitions() throws Exception {
        String seed = "CREATE TABLE seeded (id INT) WITH ('connector'='not_installed');";
        Files.writeString(directory.resolve("schema.sql"), seed);
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
    }

    @Test
    void failedServiceImportCanBeRetriedAfterCorrectingTheFile() throws Exception {
        Path sqlFile = directory.resolve("schema.sql");
        Files.writeString(sqlFile, SERVICE);

        for (int attempt = 0; attempt < 2; attempt++) {
            var failure = assertThrows(RuntimeException.class, MetadataAccessFactory::getInstance);
            assertEquals("model not exists: model_review", failure.getCause().getMessage());
        }

        Files.writeString(sqlFile, MODEL + "\n" + SERVICE);
        MetadataAccess metadata = MetadataAccessFactory.getInstance();
        assertNotNull(metadata.getService("svcreview"));
        assertSame(metadata, MetadataAccessFactory.getInstance());
    }

    @Test
    void fileImportCompletesWhileTheSameServiceIsLoadingInTheCache() throws Exception {
        Files.writeString(directory.resolve("schema.sql"), MODEL + "\n" + SERVICE);
        CountDownLatch cacheLoadStarted = new CountDownLatch(1);
        var lookupMetadata = new CompletableFuture<MetadataAccess>();

        try (var workers = Executors.newFixedThreadPool(2)) {
            var lookup = workers.submit(() -> {
                try (var factory = mockStatic(MetadataAccessFactory.class)) {
                    factory.when(MetadataAccessFactory::getInstance).thenAnswer(call -> {
                        cacheLoadStarted.countDown();
                        return lookupMetadata.get();
                    });
                    return ServiceManager.getServiceConfig("svcreview");
                }
            });
            try {
                assertTrue(cacheLoadStarted.await(10, TimeUnit.SECONDS));
                var startup = workers.submit(MetadataAccessFactory::getInstance);
                MetadataAccess metadata = startup.get(10, TimeUnit.SECONDS);
                assertNotNull(metadata.getService("svcreview"));
                lookupMetadata.complete(metadata);
                assertNotNull(lookup.get(10, TimeUnit.SECONDS));
            } finally {
                lookupMetadata.cancel(false);
            }
        }
    }
}
