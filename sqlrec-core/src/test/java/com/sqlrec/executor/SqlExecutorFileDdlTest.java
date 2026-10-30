package com.sqlrec.executor;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.compiler.SqlApiCache;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.LocalHdfsAccess;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.db.remote.HmsClient;
import com.sqlrec.model.ModelManager;
import com.sqlrec.model.ServiceManager;
import com.sqlrec.schema.CacheManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.utils.TableFactoryUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SqlExecutorFileDdlTest {
    private String oldDirectory;
    private boolean oldDdlDefault;
    private MetadataAccess metadata;
    private SqlFileSchemaAccess files;
    private MockedStatic<MetadataAccessFactory> factory;

    @BeforeEach
    void setUp() throws Exception {
        oldDirectory = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        oldDdlDefault = SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.getDefaultValue();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue("/unused/sql-file-ddl-test");
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(true);
        files = new SqlFileSchemaAccess(List.of(), List.of());
        metadata = new MetadataAccess(files, new InMemoryStoreAccess(List.of(), List.of(), List.of()), new LocalHdfsAccess());
        factory = mockStatic(MetadataAccessFactory.class);
        factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
        CalciteSchemaFactory.setGlobalSchema(null);
        CacheManager.invalidateAll();
    }

    @AfterEach
    void tearDown() {
        CacheManager.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(null);
        factory.close();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(oldDirectory);
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(oldDdlDefault);
    }

    @Test
    void fileDatabaseDdlUpdatesOnlyTheExecutingSession() throws Exception {
        databaseDdlUpdatesOnlyTheExecutingSession();
    }

    @Test
    void remoteDatabaseDdlUpdatesOnlyTheExecutingSession() throws Exception {
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue("");
        databaseDdlUpdatesOnlyTheExecutingSession();
    }

    private void databaseDdlUpdatesOnlyTheExecutingSession() throws Exception {
        try (var tables = mockStatic(TableFactoryUtils.class);
             var hms = mockStatic(HmsClient.class);
             var clients = mockStatic(HiveMetastoreClientFactory.class)) {
            tables.when(() -> TableFactoryUtils.getTableFromHmsTable(any())).thenAnswer(invocation -> {
                var table = invocation.getArgument(0, org.apache.hadoop.hive.metastore.api.Table.class);
                List<String> columns = table.getSd().getCols().stream().map(column -> column.getName()).toList();
                Object[] values = columns.stream().map(ignored -> 7).toArray();
                return new CacheTable(table.getTableName(), Linq4j.singletonEnumerable(values),
                        java.util.stream.IntStream.range(0, columns.size())
                                .mapToObj(index -> DataTypeUtils.getRelDataTypeField(columns.get(index), index, "INT")).toList());
            });
            SqlExecutor first = new SqlExecutor();
            SqlExecutor second = new SqlExecutor();
            first.executeSql("CACHE TABLE saved AS SELECT 42 AS id");
            second.executeSql("CACHE TABLE saved AS SELECT 43 AS id");
            first.executeSql("CREATE DATABASE runtime_db");
            first.executeSql("USE runtime_db");
            first.executeSql("CREATE TABLE items (id INT) WITH ('connector'='test')");
            assertEquals(7, first.executeSql("SELECT id FROM items").scan(null).single()[0]);
            assertThrows(Exception.class, () -> second.executeSql("SELECT id FROM runtime_db.items"));
            second.executeSql("FLUSH");
            assertThrows(Exception.class, () -> second.executeSql("SELECT id FROM runtime_db.items"));
            assertThrows(org.apache.hadoop.hive.metastore.api.AlreadyExistsException.class,
                    () -> second.executeSql("CREATE DATABASE runtime_db"));
            assertThrows(Exception.class, () -> second.executeSql("SELECT id FROM runtime_db.items"));
            SqlExecutor createdAfterDdl = new SqlExecutor();
            assertEquals(7, createdAfterDdl.executeSql("SELECT id FROM runtime_db.items").scan(null).single()[0]);
            second.executeSql("CREATE DATABASE IF NOT EXISTS runtime_db");
            second.executeSql("USE runtime_db");
            assertEquals(7, second.executeSql("SELECT id FROM items").scan(null).single()[0]);
            first.executeSql("ALTER TABLE items ADD extra INT");
            assertEquals(2, second.executeSql("SELECT * FROM items").getDataFields().size());
            assertEquals(List.of("id", "extra"), names(second.executeSqlAsync("DESCRIBE items")));
            first.executeSql("FLUSH");
            assertEquals(2, second.executeSql("SELECT * FROM items").getDataFields().size());
            assertEquals(42, first.executeSql("SELECT id FROM saved").scan(null).single()[0]);
            assertEquals(43, second.executeSql("SELECT id FROM saved").scan(null).single()[0]);
            first.executeSql("USE DEFAULT");
            first.executeSql("DROP DATABASE runtime_db CASCADE");
            assertThrows(Exception.class, () -> second.executeSql("SELECT id FROM items"));
            assertEquals(43, second.executeSql("SELECT id FROM saved").scan(null).single()[0]);
            first.executeSql("CREATE DATABASE runtime_db");
            first.executeSql("CREATE TABLE runtime_db.items (replacement INT) WITH ('connector'='test')");
            assertEquals(7, first.executeSql("SELECT replacement FROM runtime_db.items").scan(null).single()[0]);
            assertEquals(7, second.executeSql("SELECT replacement FROM items").scan(null).single()[0]);
            assertThrows(Exception.class, () -> second.executeSql("SELECT id FROM items"));
            hms.verifyNoInteractions();
            clients.verifyNoInteractions();
        }
    }

    @Test
    void scalarUdfDdlRefreshesQueriesAndDependentSqlFunctionsAcrossSessions() throws Exception {
        SqlExecutor writer = new SqlExecutor();
        SqlExecutor reader = new SqlExecutor();
        String query = "SELECT ddl_scalar('value') AS result_value";
        assertThrows(Exception.class, () -> reader.executeSql(query));
        writer.executeSql("CREATE FUNCTION ddl_scalar AS '" + FirstScalarUdf.class.getName() + "'");
        assertEquals("value-first", reader.executeSql(query).scan(null).single()[0]);
        writer.executeSql("CREATE SQL FUNCTION ddl_scalar_wrapper");
        writer.executeSql("RETURN " + query);
        assertEquals("value-first", reader.executeSql("CALL ddl_scalar_wrapper()").scan(null).single()[0]);

        var snapshot = files.getFunction("default", "ddl_scalar");
        writer.executeSql("ALTER FUNCTION ddl_scalar AS '" + SecondScalarUdf.class.getName() + "'");
        assertEquals(FirstScalarUdf.class.getName(), snapshot.getClassName());
        assertEquals("value-second", reader.executeSql(query).scan(null).single()[0]);
        assertEquals("value-second", reader.executeSql("CALL ddl_scalar_wrapper()").scan(null).single()[0]);
        writer.executeSql("DROP FUNCTION ddl_scalar");
        assertThrows(Exception.class, () -> reader.executeSql(query));
        assertThrows(Exception.class, () -> reader.executeSql("CALL ddl_scalar_wrapper()"));
        writer.executeSql("CREATE FUNCTION ddl_scalar AS '" + FirstScalarUdf.class.getName() + "'");
        assertEquals("value-first", reader.executeSql("CALL ddl_scalar_wrapper()").scan(null).single()[0]);
    }

    @Test
    void tableUdfDdlRefreshesPositiveAndNegativeClassCachesAcrossSessions() throws Exception {
        SqlExecutor writer = new SqlExecutor();
        SqlExecutor reader = new SqlExecutor();
        String call = "CALL ddl_table_udf()";
        assertThrows(Exception.class, () -> reader.executeSql(call));
        writer.executeSql("CREATE FUNCTION ddl_table_udf AS '" + FirstTableUdf.class.getName() + "'");
        assertEquals(1, reader.executeSql(call).scan(null).single()[0]);
        writer.executeSql("ALTER FUNCTION ddl_table_udf AS '" + SecondTableUdf.class.getName() + "'");
        assertEquals(2, reader.executeSql(call).scan(null).single()[0]);
        writer.executeSql("DROP FUNCTION ddl_table_udf");
        assertThrows(Exception.class, () -> reader.executeSql(call));
        writer.executeSql("CREATE FUNCTION ddl_table_udf AS '" + FirstTableUdf.class.getName() + "'");
        assertEquals(1, reader.executeSql(call).scan(null).single()[0]);
    }

    public static class FirstScalarUdf {
        public String evaluate(String value) { return value + "-first"; }
    }

    public static class SecondScalarUdf {
        public String evaluate(String value) { return value + "-second"; }
    }

    public static class FirstTableUdf {
        public CacheTable evaluate() { return tableUdfResult(1); }
    }

    public static class SecondTableUdf {
        public CacheTable evaluate() { return tableUdfResult(2); }
    }

    private static CacheTable tableUdfResult(int value) {
        return new CacheTable("result", Linq4j.singletonEnumerable(new Object[]{value}),
                List.of(DataTypeUtils.getRelDataTypeField("id", 0, "INT")));
    }

    @Test
    void sqlFunctionsAndApisUseMemoryAndPreserveTheirDependencyChecks() throws Exception {
        SqlExecutor executor = new SqlExecutor();
        executor.executeSql("CREATE SQL FUNCTION runtime_function");
        executor.executeSql("RETURN SELECT 1 AS result_value");
        assertEquals(1, executor.executeSql("CALL runtime_function()").scan(null).single()[0]);
        executor.executeSql("CREATE API runtime_api WITH runtime_function");
        assertEquals("runtime_function", SqlApiCache.get("runtime_api").getFunctionName());
        assertThrows(Exception.class, () -> executor.executeSql("DROP SQL FUNCTION runtime_function"));
        executor.executeSql("CREATE OR REPLACE SQL FUNCTION runtime_function");
        executor.executeSql("RETURN SELECT 2 AS result_value");
        assertEquals(2, executor.executeSql("CALL runtime_function()").scan(null).single()[0]);
        executor.executeSql("CREATE SQL FUNCTION replacement_function");
        executor.executeSql("RETURN SELECT 3 AS result_value");
        executor.executeSql("CREATE OR REPLACE API runtime_api WITH replacement_function");
        assertEquals("replacement_function", SqlApiCache.get("runtime_api").getFunctionName());
        executor.executeSql("DROP SQL FUNCTION runtime_function");
        executor.executeSql("DROP API runtime_api");
        assertThrows(IllegalArgumentException.class, () -> SqlApiCache.get("runtime_api"));
        executor.executeSql("DROP SQL FUNCTION replacement_function");
        executor.executeSql("DROP SQL FUNCTION IF EXISTS replacement_function");
        executor.executeSql("DROP API IF EXISTS runtime_api");
        assertNull(metadata.getSqlFunction("replacement_function"));
        assertNull(metadata.getSqlApi("runtime_api"));
    }

    @Test
    void ddlPermissionIsCapturedAtInitializationAndUnsupportedOperationsStayBlocked() throws Exception {
        SqlExecutor executor = new SqlExecutor();
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(false);
        executor.executeSql("SET 'SQL_SCHEMA_DDL_ENABLED'='false'");
        executor.executeSql("CREATE TABLE items (id INT)");
        try (var models = mockStatic(ModelManager.class);
             var services = mockStatic(ServiceManager.class)) {
            for (String sql : List.of("CREATE MODEL m (id INT)", "DROP MODEL m", "DROP SERVICE s",
                    "CREATE TABLE t AS SELECT 1 AS id", "CREATE VIEW v AS SELECT 1 AS id",
                    "CREATE TEMPORARY FUNCTION f AS 'missing.Udf'")) {
                assertThrows(UnsupportedOperationException.class, () -> executor.executeSql(sql), sql);
            }
            models.verifyNoInteractions();
            services.verifyNoInteractions();
        }
        SqlExecutor locked = new SqlExecutor();
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(true);
        locked.executeSql("SET 'SQL_SCHEMA_DDL_ENABLED'='true'");
        for (String sql : List.of("CREATE SQL FUNCTION f", "CREATE API a WITH f", "DROP API IF EXISTS a",
                "DROP SQL FUNCTION IF EXISTS f", "CREATE TABLE t (id INT)")) {
            assertThrows(UnsupportedOperationException.class, () -> locked.executeSql(sql), sql);
        }
    }

    @Test
    void allDdlPathsRejectReadOnlyMetadataWithoutEvictingCaches() throws Exception {
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(false);
        var readOnly = new MetadataAccess(new SqlFileSchemaAccess(List.of(
                com.sqlrec.compiler.CompileManager.parseSql("CREATE TABLE items (id INT)")), List.of(
                com.sqlrec.compiler.CompileManager.parseSql("CREATE FUNCTION f AS 'missing.Udf'"))),
                new InMemoryStoreAccess(List.of(List.of(com.sqlrec.compiler.CompileManager.parseSql("CREATE SQL FUNCTION stored_function"))),
                        List.of(com.sqlrec.compiler.CompileManager.parseSql("CREATE API stored_api WITH stored_function")),
                        List.of()), new LocalHdfsAccess());
        factory.when(MetadataAccessFactory::getInstance).thenReturn(readOnly);
        SqlExecutor executor = new SqlExecutor();
        try (var caches = mockStatic(CacheManager.class)) {
            for (String sql : List.of("CREATE DATABASE review", "CREATE DATABASE IF NOT EXISTS `default`",
                    "ALTER DATABASE `default` SET ('owner'='test')", "DROP DATABASE IF EXISTS missing",
                    "CREATE TABLE extra (id INT)", "CREATE TABLE IF NOT EXISTS items (id INT)",
                    "CREATE TABLE copied LIKE items", "ALTER TABLE items ADD extra INT",
                    "ALTER TABLE IF EXISTS missing ADD extra INT", "DROP TABLE items", "DROP TABLE IF EXISTS missing",
                    "CREATE FUNCTION f2 AS 'missing.Udf'", "ALTER FUNCTION f AS 'missing.Updated'",
                    "DROP FUNCTION f", "DROP FUNCTION IF EXISTS missing", "CREATE SQL FUNCTION extra_function",
                    "CREATE OR REPLACE SQL FUNCTION stored_function", "DROP SQL FUNCTION stored_function",
                    "CREATE API extra_api WITH stored_function", "CREATE OR REPLACE API stored_api WITH stored_function",
                    "DROP API stored_api", "DROP API IF EXISTS missing")) {
                var error = assertThrows(UnsupportedOperationException.class, () -> executor.executeSql(sql), sql);
                assertTrue(error.getMessage().contains("SQL_SCHEMA_DDL_ENABLED=true"), sql);
            }
            caches.verifyNoInteractions();
        }
        assertEquals(List.of("default"), readOnly.getDatabases());
        assertEquals(List.of("items"), readOnly.getTables("default").stream().map(table -> table.getTableName()).toList());
        assertEquals("missing.Udf", readOnly.getFunction("default", "f").getClassName());
        assertNotNull(readOnly.getSqlFunction("stored_function"));
        assertNotNull(readOnly.getSqlApi("stored_api"));
    }

    @Test
    void readOnlySqlExecutorObservesInternalMetadataChanges() throws Exception {
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(false);
        SqlExecutor executor = new SqlExecutor();
        files.executeMetadataDdl(com.sqlrec.compiler.CompileManager.parseSql("CREATE DATABASE internal_db"), "default");
        files.executeMetadataDdl(com.sqlrec.compiler.CompileManager.parseSql(
                "CREATE TABLE internal_db.items (id INT)"), "default");
        executor.executeSql("USE internal_db");
        assertEquals(List.of("items"), names(executor.executeSqlAsync("SHOW TABLES")));
        assertEquals(List.of("id"), names(executor.executeSqlAsync("DESCRIBE items")));
        var api = new com.sqlrec.entity.SqlApi();
        api.setName("internal_api");
        api.setFunctionName("internal_function");
        metadata.insertSqlApi(api);
        assertEquals(List.of("internal_api"), names(executor.executeSqlAsync("SHOW APIS")));
        assertThrows(UnsupportedOperationException.class, () -> executor.executeSql("DROP TABLE items"));
        assertThrows(UnsupportedOperationException.class, () -> executor.executeSql("DROP API internal_api"));
        assertNotNull(metadata.getTable("internal_db", "items"));
        assertNotNull(metadata.getSqlApi("internal_api"));
    }

    @Test
    void failedFunctionDefinitionsAndDuplicateApisLeaveStoredResourcesIntact() throws Exception {
        SqlExecutor executor = new SqlExecutor();
        executor.executeSql("CREATE SQL FUNCTION existing_function");
        executor.executeSql("RETURN SELECT 1 AS id");
        executor.executeSql("CREATE API existing_api WITH existing_function");
        var originalFunction = metadata.getSqlFunction("existing_function").getSqlList();
        executor.executeSql("CREATE SQL FUNCTION existing_function");
        assertThrows(RuntimeException.class, () -> executor.executeSql("RETURN SELECT 99 AS id"));
        assertEquals(originalFunction, metadata.getSqlFunction("existing_function").getSqlList());
        assertEquals(1, executor.executeSql("CALL existing_function()").scan(null).single()[0]);
        assertThrows(RuntimeException.class, () -> executor.executeSql("CREATE API existing_api WITH missing_function"));
        assertEquals("existing_function", SqlApiCache.get("existing_api").getFunctionName());
        executor.executeSql("CREATE SQL FUNCTION broken_function");
        assertThrows(Exception.class, () -> executor.executeSql("CREATE TABLE not_a_function_body (id INT)"));
        assertNull(metadata.getSqlFunction("broken_function"));
        assertTrue(files.getTables("default").isEmpty());
        executor.executeSql("CREATE SQL FUNCTION recovered_function");
        executor.executeSql("RETURN SELECT 2 AS id");
        assertEquals(2, executor.executeSql("CALL recovered_function()").scan(null).single()[0]);
    }

    @Test
    void databaseDdlPreservesCustomSubSchemas() throws Exception {
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        root.add("custom", new AbstractSchema() {
            @Override
            protected java.util.Map<String, org.apache.calcite.schema.Table> getTableMap() {
                return java.util.Map.of("saved", new CacheTable("saved", Linq4j.singletonEnumerable(new Object[]{42}),
                        List.of(DataTypeUtils.getRelDataTypeField("id", 0, "INT"))));
            }
        });
        CalciteSchemaFactory.setGlobalSchema(root);
        SqlExecutor executor = new SqlExecutor();
        executor.executeSql("CREATE DATABASE review");
        executor.executeSql("DROP DATABASE REVIEW");
        executor.executeSql("DROP DATABASE IF EXISTS custom");
        executor.executeSql("CREATE DATABASE custom");
        executor.executeSql("DROP DATABASE custom");
        assertEquals(42, executor.executeSql("SELECT id FROM custom.saved").scan(null).single()[0]);
    }

    private static List<Object> names(SqlProcessResult result) {
        return result.getEnumerable().select(row -> row[0]).toList();
    }
}
