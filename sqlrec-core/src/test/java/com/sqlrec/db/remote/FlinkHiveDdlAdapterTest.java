package com.sqlrec.db.remote;

import com.sqlrec.common.config.Consts;
import com.sqlrec.compiler.CompileManager;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlAddPartitions;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class FlinkHiveDdlAdapterTest {
    @Test
    void resolvesOnlyUnqualifiedNamesAgainstTheSessionDatabase() {
        assertEquals(new ObjectPath("session_db", "t"),
                FlinkHiveDdlAdapter.objectPath(new String[]{"t"}, "session_db"));
        assertEquals(new ObjectPath("other", "t"),
                FlinkHiveDdlAdapter.objectPath(new String[]{"other", "t"}, "session_db"));
        assertEquals(new ObjectPath("other", "t"),
                FlinkHiveDdlAdapter.objectPath(new String[]{"hive", "other", "t"}, "session_db"));
    }

    @Test
    void rejectsUnsupportedStatementsAndForeignCatalogsBeforeOpeningHms() {
        try (var adapter = new FlinkHiveDdlAdapter(() -> {
            throw new AssertionError("Rejected statements must not open HMS");
        })) {
            for (String sql : new String[]{
                    "CREATE TEMPORARY TABLE t (id INT)", "CREATE TEMPORARY FUNCTION f AS 'example.Udf'",
                    "CREATE TABLE t AS SELECT 1", "SELECT 1", "CREATE VIEW v AS SELECT 1", "USE CATALOG hive",
                    "CREATE TABLE other.defaultdb.t (id INT)", "ALTER TABLE other.defaultdb.t SET ('path'='x')",
                    "CREATE FUNCTION other.defaultdb.f AS 'example.Udf'", "CREATE DATABASE other.db",
                    "CREATE TABLE hive.defaultdb.copy LIKE other.defaultdb.source",
                    "CREATE TABLE t (id INT, extra AS id + 1)",
                    "CREATE TABLE t (ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)",
                    "ALTER TABLE t ADD extra AS id + 1", "ALTER TABLE t ADD WATERMARK FOR ts AS ts",
                    "ALTER TABLE t DROP WATERMARK"}) {
                assertThrows(UnsupportedOperationException.class, () -> adapter.executeDdl(sql, "default"), sql);
            }
            assertThrows(UnsupportedOperationException.class,
                    () -> adapter.executeQuery("SHOW CREATE TABLE other.defaultdb.t", "default"));
        }
    }

    @Test
    void preparationFailureClosesTheConnectionWithoutWritingOrRetrying() throws Exception {
        HiveCatalog catalog = mock(HiveCatalog.class);
        CatalogManager manager = manager(catalog);
        var failure = new CatalogException("read failed", new TTransportException("connection lost"));
        when(catalog.getDatabase("review")).thenThrow(failure);
        Supplier<CatalogManager> factory = mock(Supplier.class);
        when(factory.get()).thenReturn(manager);
        try (var adapter = new FlinkHiveDdlAdapter(factory)) {
            assertSame(failure, assertThrows(CatalogException.class,
                    () -> adapter.executeDdl("ALTER DATABASE review SET ('review'='value')", "default")));
            verify(catalog).getDatabase("review");
            verify(catalog, never()).alterDatabase(anyString(), any(), anyBoolean());
            verify(factory).get();
        }
        verify(manager).close();
    }

    @Test
    void lostWriteResponseIsNotRetriedAndTheNextRequestUsesANewConnection() throws Exception {
        HiveCatalog broken = mock(HiveCatalog.class);
        HiveCatalog healthy = mock(HiveCatalog.class);
        CatalogManager first = manager(broken);
        CatalogManager second = manager(healthy);
        var failure = new CatalogException("response lost", new TTransportException("connection lost"));
        ObjectPath path = new ObjectPath("review", "items");
        doThrow(failure).when(broken).dropTable(path, false);
        Supplier<CatalogManager> factory = mock(Supplier.class);
        when(factory.get()).thenReturn(first, second);
        try (var adapter = new FlinkHiveDdlAdapter(factory)) {
            var error = assertThrows(IllegalStateException.class,
                    () -> adapter.executeDdl("DROP TABLE review.items", "default"));
            assertTrue(error.getMessage().startsWith("METADATA_WRITE_OUTCOME_UNKNOWN:"));
            assertSame(failure, error.getCause());
            verify(broken).dropTable(path, false);
            verify(first).close();
            verify(factory).get();
            adapter.executeDdl("DROP TABLE IF EXISTS review.items", "default");
            verify(healthy).dropTable(path, true);
            verify(factory, times(2)).get();
        }
        verify(first).close();
        verify(second).close();
    }

    @Test
    void queryTransportFailureDiscardsTheConnectionAndPreservesTheError() throws Exception {
        HiveCatalog catalog = mock(HiveCatalog.class);
        CatalogManager manager = manager(catalog);
        var failure = new CatalogException("read failed", new TTransportException("connection lost"));
        when(catalog.getTable(new ObjectPath("review", "items"))).thenThrow(failure);
        try (var adapter = new FlinkHiveDdlAdapter(() -> manager)) {
            assertSame(failure, assertThrows(CatalogException.class,
                    () -> adapter.executeQuery("SHOW CREATE TABLE review.items", "default")));
        }
        verify(manager).close();
    }

    @Test
    void validationFailurePreservesTheConnectionAndDoesNotWrite() throws Exception {
        HiveCatalog catalog = mock(HiveCatalog.class);
        CatalogManager manager = manager(catalog);
        when(catalog.databaseExists("missing")).thenReturn(false);
        Supplier<CatalogManager> factory = mock(Supplier.class);
        when(factory.get()).thenReturn(manager);
        try (var adapter = new FlinkHiveDdlAdapter(factory)) {
            assertThrows(IllegalArgumentException.class, () -> adapter.executeDdl("DROP TABLE items", "missing"));
            verify(catalog, never()).dropTable(any(), anyBoolean());
            verify(manager, never()).close();
            adapter.executeDdl("DROP TABLE review.items", "missing");
            verify(catalog).dropTable(new ObjectPath("review", "items"), false);
            verify(factory).get();
        }
    }

    @Test
    void missingAlterIfExistsSkipsReadingAndWritingTheDefinition() throws Exception {
        HiveCatalog catalog = mock(HiveCatalog.class);
        ObjectPath path = new ObjectPath("review", "missing");
        when(catalog.tableExists(path)).thenReturn(false);
        try (var adapter = new FlinkHiveDdlAdapter(() -> manager(catalog))) {
            adapter.executeDdl("ALTER TABLE IF EXISTS review.missing ADD note STRING", "default");
            adapter.executeDdl("ALTER TABLE IF EXISTS review.missing RENAME TO renamed", "default");
            verify(catalog, never()).getTable(any());
            verify(catalog, never()).alterTable(any(), any(), anyBoolean());
            verify(catalog, never()).renameTable(any(), anyString(), anyBoolean());
        }
    }

    @Test
    void partitionDefinitionsArePreparedBeforeAnyWrite() throws Exception {
        HiveCatalog catalog = mock(HiveCatalog.class);
        var sql = (SqlAddPartitions) CompileManager.parseSql(
                "ALTER TABLE review.items ADD PARTITION (category='a') PARTITION (category='b') WITH ('review'='value')");
        // Simulate a conversion error in the second definition after the first was prepared.
        sql.getPartProps().set(1, new SqlNodeList(List.of(new SqlIdentifier("invalid", SqlParserPos.ZERO)), SqlParserPos.ZERO));
        try (var adapter = new FlinkHiveDdlAdapter(() -> manager(catalog))) {
            assertThrows(ClassCastException.class, () -> adapter.executeDdl(sql, "default"));
            verify(catalog, never()).createPartition(any(), any(), any(), anyBoolean());
        }
    }

    @Test
    void partitionWithoutPropertiesUsesAnEmptyDefinition() throws Exception {
        HiveCatalog catalog = mock(HiveCatalog.class);
        try (var adapter = new FlinkHiveDdlAdapter(() -> manager(catalog))) {
            adapter.executeDdl("ALTER TABLE review.items ADD PARTITION (category='a')", "default");
            verify(catalog).createPartition(eq(new ObjectPath("review", "items")),
                    eq(new CatalogPartitionSpec(Map.of("category", "a"))),
                    argThat(partition -> partition.getProperties().isEmpty() && partition.getComment() == null), eq(false));
        }
    }

    @Test
    void initializationFailureCleansUpAndAllowsTheNextRequestToInitialize() throws Exception {
        HiveCatalog failed = mock(HiveCatalog.class);
        var failure = new CatalogException("open failed");
        doThrow(failure).when(failed).open();
        HiveCatalog healthy = mock(HiveCatalog.class);
        CatalogManager healthyManager = manager(healthy);
        Supplier<CatalogManager> factory = mock(Supplier.class);
        when(factory.get()).thenAnswer(call -> FlinkHiveDdlAdapter.openCatalog(failed)).thenReturn(healthyManager);
        try (var adapter = new FlinkHiveDdlAdapter(factory)) {
            assertSame(failure, assertThrows(CatalogException.class,
                    () -> adapter.executeDdl("DROP TABLE review.items", "default")));
            verify(failed).close();
            adapter.executeDdl("DROP TABLE review.items", "default");
            verify(healthy).dropTable(new ObjectPath("review", "items"), false);
            verify(factory, times(2)).get();
        }
    }

    @Test
    void managerInitializationFailureClosesTheAlreadyOpenedCatalog() {
        HiveCatalog catalog = mock(HiveCatalog.class);
        var failure = new CatalogException("initialization failed");
        when(catalog.getDefaultDatabase()).thenThrow(failure);
        assertSame(failure, assertThrows(CatalogException.class, () -> FlinkHiveDdlAdapter.openCatalog(catalog)));
        verify(catalog).open();
        verify(catalog).close();
    }

    @Test
    void writeRejectionPreservesTheErrorAndTheConnection() throws Exception {
        HiveCatalog catalog = mock(HiveCatalog.class);
        CatalogManager manager = manager(catalog);
        var failure = new CatalogException("write rejected");
        ObjectPath path = new ObjectPath("review", "items");
        doThrow(failure).when(catalog).dropTable(path, false);
        try (var adapter = new FlinkHiveDdlAdapter(() -> manager)) {
            assertSame(failure, assertThrows(CatalogException.class,
                    () -> adapter.executeDdl("DROP TABLE review.items", "default")));
            verify(catalog).dropTable(path, false);
            verify(manager, never()).close();
        }
        verify(manager).close();
    }

    @Test
    void cleanupFailureDoesNotReplaceTheOriginalError() throws Exception {
        HiveCatalog catalog = mock(HiveCatalog.class);
        CatalogManager manager = manager(catalog);
        var failure = new CatalogException("read failed", new TTransportException("connection lost"));
        var closeFailure = new CatalogException("close failed");
        when(catalog.getDatabase("review")).thenThrow(failure);
        doThrow(closeFailure).when(manager).close();
        try (var adapter = new FlinkHiveDdlAdapter(() -> manager)) {
            assertSame(failure, assertThrows(CatalogException.class,
                    () -> adapter.executeDdl("ALTER DATABASE review SET ('review'='value')", "default")));
            assertArrayEquals(new Throwable[]{closeFailure}, failure.getSuppressed());
        }
        verify(manager).close();
    }

    @Test
    void adaptersHaveIndependentConnectionsAndCloseIsIdempotent() throws Exception {
        HiveCatalog firstCatalog = mock(HiveCatalog.class);
        HiveCatalog secondCatalog = mock(HiveCatalog.class);
        CatalogManager firstManager = manager(firstCatalog);
        CatalogManager secondManager = manager(secondCatalog);
        Supplier<CatalogManager> firstFactory = mock(Supplier.class);
        when(firstFactory.get()).thenReturn(firstManager);
        try (var first = new FlinkHiveDdlAdapter(firstFactory);
             var second = new FlinkHiveDdlAdapter(() -> secondManager)) {
            first.close(); // Closing an unused adapter does not initialize it.
            verifyNoInteractions(firstFactory);
            first.executeDdl("DROP TABLE review.first_table", "default");
            second.executeDdl("DROP TABLE review.second_table", "default");
            first.close();
            first.close();
            verify(firstManager).close();
            verify(secondManager, never()).close();
            second.executeDdl("DROP TABLE review.second_again", "default");
            verify(secondCatalog).dropTable(new ObjectPath("review", "second_again"), false);
        }
        verify(firstManager).close();
        verify(secondManager).close();
    }

    private static CatalogManager manager(HiveCatalog catalog) {
        CatalogManager manager = mock(CatalogManager.class);
        when(manager.getCatalog(Consts.HIVE_CATALOG_NAME)).thenReturn(Optional.of(catalog));
        return manager;
    }
}
