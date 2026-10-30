package com.sqlrec.schema;

import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.udf.UdfManager;
import com.sqlrec.utils.TableFactoryUtils;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class MetadataSchemaTest {
    @Test
    void databaseSchemasAndEvictionIgnoreDatabaseNameCase() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var definition = table("items");
        when(metadata.getTables("db")).thenReturn(List.of(definition));
        Table original = mock(Table.class);
        Table afterTableEviction = mock(Table.class);
        Table afterDatabaseEviction = mock(Table.class);
        CalciteSchemaFactory.setGlobalSchema(null);
        try (var factories = mockStatic(TableFactoryUtils.class)) {
            factories.when(() -> TableFactoryUtils.getTableFromHmsTable(definition))
                    .thenReturn(original, afterTableEviction, afterDatabaseEviction);
            var first = ConcurrentCalciteSchema.createRootSchema();
            var second = ConcurrentCalciteSchema.createRootSchema();
            CalciteSchemaFactory.addDatabase(first, metadata, "db");
            CalciteSchemaFactory.addDatabase(second, metadata, "DB");
            var firstSchema = (HmsSchema) first.getSubSchema("db", false).schema;
            var secondSchema = (HmsSchema) second.getSubSchema("DB", false).schema;
            assertSame(firstSchema, secondSchema);
            assertSame(original, firstSchema.getTable("items"));
            assertSame(original, secondSchema.getTable("items"));

            CalciteSchemaFactory.evictTable("Db", "ITEMS");
            assertSame(afterTableEviction, secondSchema.getTable("items"));
            assertSame(afterTableEviction, firstSchema.getTable("items"));
            CalciteSchemaFactory.evictDatabase("dB");
            assertSame(afterDatabaseEviction, firstSchema.getTable("items"));
            assertSame(afterDatabaseEviction, secondSchema.getTable("items"));

            CalciteSchemaFactory.evictTable("missing", "items");
            CalciteSchemaFactory.evictDatabase("missing");
            assertSame(afterDatabaseEviction, firstSchema.getTable("items"));
        } finally {
            CalciteSchemaFactory.setGlobalSchema(null);
        }
    }

    @Test
    void metadataRefreshReusesOtherTablesAndDetectsChangesWithinTheSameSecond() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var definition = table("items");
        definition.putToParameters("transient_lastDdlTime", "1700000000");
        var unaffected = table("other");
        when(metadata.getTables("db")).thenReturn(List.of(definition, unaffected));
        Table original = mock(Table.class);
        Table altered = mock(Table.class);
        Table other = mock(Table.class);
        try (var factories = mockStatic(TableFactoryUtils.class)) {
            factories.when(() -> TableFactoryUtils.getTableFromHmsTable(definition)).thenReturn(original, altered);
            factories.when(() -> TableFactoryUtils.getTableFromHmsTable(unaffected)).thenReturn(other);
            HmsSchema schema = new HmsSchema("db", metadata);
            assertSame(original, schema.getTable("items"));
            assertSame(other, schema.getTable("other"));
            schema.invalidateMetadata();
            assertSame(original, schema.getTable("items"));
            definition.putToParameters("flink.path", "changed");
            schema.invalidateMetadata();
            assertSame(altered, schema.getTable("items"));
            assertSame(other, schema.getTable("other"));
            factories.verify(() -> TableFactoryUtils.getTableFromHmsTable(unaffected), times(1));
        }
    }

    @Test
    void tableEvictionRebuildsAnIdenticalDefinitionWithoutAnIntermediateRead() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var definition = table("items");
        when(metadata.getTables("db")).thenReturn(List.of(definition));
        Table original = mock(Table.class);
        Table recreated = mock(Table.class);
        try (var factories = mockStatic(TableFactoryUtils.class)) {
            factories.when(() -> TableFactoryUtils.getTableFromHmsTable(definition)).thenReturn(original, recreated);
            HmsSchema schema = new HmsSchema("db", metadata);
            assertSame(original, schema.getTable("items"));
            schema.evictTable("ITEMS");
            assertSame(recreated, schema.getTable("items"));
        }
    }

    @Test
    void evictionWaitsForAnInFlightRefreshAndDoesNotRestoreTheOldTable() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var definition = table("items");
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch evicting = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        when(metadata.getTables("db")).thenAnswer(call -> {
            if (loads.incrementAndGet() == 2) {
                loading.countDown();
                if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("refresh timed out");
            }
            return List.of(definition);
        });
        Table original = mock(Table.class);
        Table recreated = mock(Table.class);
        try (var factories = mockStatic(TableFactoryUtils.class);
             var workers = Executors.newFixedThreadPool(2)) {
            factories.when(() -> TableFactoryUtils.getTableFromHmsTable(definition)).thenReturn(original, recreated);
            HmsSchema schema = new HmsSchema("db", metadata);
            assertSame(original, schema.getTable("items"));
            schema.invalidateMetadata();
            try {
                var reader = workers.submit(() -> schema.getTable("items"));
                assertTrue(loading.await(10, TimeUnit.SECONDS));
                var eviction = workers.submit(() -> {
                    evicting.countDown();
                    schema.evictTable("items");
                });
                assertTrue(evicting.await(10, TimeUnit.SECONDS));
                release.countDown();
                assertSame(original, reader.get(10, TimeUnit.SECONDS));
                eviction.get(10, TimeUnit.SECONDS);
                assertSame(recreated, schema.getTable("items"));
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void invalidationReloadsTablesEvenWhenDdlTimestampsAreEqual() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var definition = table("items");
        definition.putToParameters("transient_lastDdlTime", "1700000000");
        when(metadata.getTables("db")).thenReturn(List.of(definition));
        Table original = mock(Table.class);
        Table altered = mock(Table.class);
        try (var factories = mockStatic(TableFactoryUtils.class)) {
            factories.when(() -> TableFactoryUtils.getTableFromHmsTable(definition))
                    .thenReturn(original, altered);
            HmsSchema schema = new HmsSchema("db", metadata);
            assertSame(original, schema.getTable("items"));
            schema.invalidateCache();
            assertSame(altered, schema.getTable("items"));
        }
    }

    @Test
    @SilenceLoggers(TableFactoryUtils.class)
    void unsupportedTableDoesNotPreventLoadingOtherTables() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var unsupported = table("unsupported");
        var supported = table("supported");
        when(metadata.getTables("db")).thenReturn(List.of(unsupported, supported));
        HmsTableFactory connector = mock(HmsTableFactory.class);
        Table loaded = mock(Table.class);
        when(connector.getTableFromHmsTable(unsupported)).thenThrow(new UnsupportedOperationException("unsupported table definition"));
        when(connector.getTableFromHmsTable(supported)).thenReturn(loaded);
        try (var factories = mockStatic(TableFactoryUtils.class)) {
            factories.when(() -> TableFactoryUtils.getTableFactory("review")).thenReturn(connector);
            factories.when(() -> TableFactoryUtils.getTableFromHmsTable(unsupported)).thenCallRealMethod();
            factories.when(() -> TableFactoryUtils.getTableFromHmsTable(supported)).thenCallRealMethod();
            HmsSchema schema = new HmsSchema("db", metadata);
            assertNull(schema.getTable("unsupported"));
            assertSame(loaded, schema.getTable("supported"));
        }
    }

    @Test
    @SilenceLoggers(HmsSchema.class)
    void missingUdfDependencyDoesNotPreventLoadingOtherFunctions() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var broken = function("broken", "example.Broken");
        var healthy = function("healthy", "example.Healthy");
        when(metadata.getFunctions("db")).thenReturn(List.of(broken, healthy));
        ScalarFunction loaded = mock(ScalarFunction.class);
        try (var udf = mockStatic(UdfManager.class)) {
            udf.when(() -> UdfManager.createScalarFunction(anyString())).thenReturn(loaded);
            udf.when(() -> UdfManager.createScalarFunction("example.Broken"))
                    .thenThrow(new NoClassDefFoundError("missing dependency"));
            HmsSchema schema = new HmsSchema("db", metadata);
            assertTrue(schema.getFunctions("broken").isEmpty());
            assertTrue(schema.getFunctions("healthy").contains(loaded));
        }
    }

    private static org.apache.hadoop.hive.metastore.api.Table table(String name) {
        var table = new org.apache.hadoop.hive.metastore.api.Table();
        table.setTableName(name);
        table.setDbName("db");
        table.putToParameters("flink.connector", "review");
        return table;
    }

    private static org.apache.hadoop.hive.metastore.api.Function function(String name, String className) {
        var function = new org.apache.hadoop.hive.metastore.api.Function();
        function.setFunctionName(name);
        function.setClassName(className);
        return function;
    }
}
