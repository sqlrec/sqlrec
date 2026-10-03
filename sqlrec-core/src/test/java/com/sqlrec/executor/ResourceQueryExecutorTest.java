package com.sqlrec.executor;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.HdfsAccess;
import com.sqlrec.db.StoreAccess;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ResourceQueryExecutorTest {
    @Test
    void metadataQueriesUseCatalogExceptForUnqualifiedSessionTables() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var catalogResult = SqlProcessResult.msg("catalog result", "msg");
        when(metadata.executeMetadataQuery(any(), eq("review"))).thenReturn(catalogResult);
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        CacheTable cached = new CacheTable("items", null, DataTypeUtils.getStringTypeField("cached_column"));
        cached.setCreateSql("CACHE TABLE items AS SELECT 'cached' AS cached_column");
        root.add("items", cached);
        ResourceQueryExecutor executor = new ResourceQueryExecutor(metadata, root);
        try (var config = mockStatic(SqlRecConfigs.class)) {
            config.when(SqlRecConfigs::isFileSystemMetadata).thenReturn(false);
            assertEquals(List.of("cached_column"), names(executor.execute(
                    CompileManager.parseSql("DESCRIBE items"), "review")));
            assertEquals(List.of(cached.getCreateSql()), names(executor.execute(
                    CompileManager.parseSql("SHOW CREATE TABLE items"), "review")));
            verifyNoInteractions(metadata);
            for (String sql : List.of("DESCRIBE review.items", "SHOW CREATE TABLE hive.review.items",
                    "DESCRIBE durable", "SHOW CREATE TABLE durable", "SHOW FUNCTIONS")) {
                assertSame(catalogResult, executor.execute(CompileManager.parseSql(sql), "review"), sql);
            }
        }
    }

    @Test
    void fileMetadataQueriesUseDefinitionsAndQualifiedNamesBypassCacheTables() throws Exception {
        var files = new SqlFileSchemaAccess(List.of(CompileManager.parseSql(
                "CREATE TABLE hive.review.items (stored_column STRING) WITH ('connector'='not_installed')")), List.of());
        MetadataAccess metadata = new MetadataAccess(files, mock(StoreAccess.class), mock(HdfsAccess.class));
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        CacheTable cached = new CacheTable("items", null, DataTypeUtils.getStringTypeField("cached_column"));
        cached.setCreateSql("CACHE TABLE items AS SELECT 'cached' AS cached_column");
        root.add("items", cached);
        ResourceQueryExecutor executor = new ResourceQueryExecutor(metadata, root);
        try (var config = mockStatic(SqlRecConfigs.class)) {
            config.when(SqlRecConfigs::isFileSystemMetadata).thenReturn(true);
            assertEquals(List.of("cached_column"), names(executor.execute(
                    CompileManager.parseSql("DESCRIBE items"), "review")));
            assertEquals(List.of(cached.getCreateSql()), names(executor.execute(
                    CompileManager.parseSql("SHOW CREATE TABLE items"), "review")));
            for (String name : List.of("review.items", "hive.REVIEW.ITEMS")) {
                var described = executor.execute(CompileManager.parseSql("DESCRIBE " + name), "default");
                assertEquals(List.of("stored_column"), names(described));
                assertEquals(6, described.getFields().size());
                String ddl = (String) names(executor.execute(CompileManager.parseSql("SHOW CREATE TABLE " + name), "default")).get(0);
                assertTrue(ddl.contains("not_installed"));
            }
            assertFalse(names(executor.execute(CompileManager.parseSql("SHOW FUNCTIONS"), "review")).isEmpty());
            assertThrows(Exception.class, () -> executor.execute(CompileManager.parseSql("DESCRIBE missing"), "review"));
            assertThrows(UnsupportedOperationException.class, () -> executor.execute(
                    CompileManager.parseSql("DESCRIBE other.review.items"), "review"));
        }
    }

    @Test
    void showTablesFiltersLikeAndNotLikeUsingFlinkSemantics() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        when(metadata.getDatabases()).thenReturn(List.of("review"));
        when(metadata.getTables("review")).thenReturn(List.of(table("items_b"), table("other"), table("items_a")));
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        // Listing persistent tables must also work before the session knows this database.
        ResourceQueryExecutor executor = new ResourceQueryExecutor(metadata, root);
        assertEquals(List.of("items_a", "items_b"), names(executor.execute(
                CompileManager.parseSql("SHOW TABLES IN hive.REVIEW LIKE 'items_%'"), "default")));
        assertEquals(List.of("other"), names(executor.execute(
                CompileManager.parseSql("SHOW TABLES IN review NOT LIKE 'items_%'"), "default")));
        assertTrue(names(executor.execute(
                CompileManager.parseSql("SHOW TABLES IN review LIKE 'ITEMS_%'"), "default")).isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> executor.execute(
                CompileManager.parseSql("SHOW TABLES IN other.review"), "default"));
        assertThrows(RuntimeException.class, () -> executor.execute(
                CompileManager.parseSql("SHOW TABLES IN missing"), "default"));
    }

    private static List<Object> names(SqlProcessResult result) {
        return result.getEnumerable().select(row -> row[0]).toList();
    }

    private static Table table(String name) {
        Table table = new Table();
        table.setTableName(name);
        return table;
    }
}
