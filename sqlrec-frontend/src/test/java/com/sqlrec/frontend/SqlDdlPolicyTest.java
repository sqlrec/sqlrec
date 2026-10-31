package com.sqlrec.frontend;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.db.HdfsAccess;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.frontend.rest.RestSqlExecutor;
import com.sqlrec.frontend.rest.RestFunctionExecutor;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.schema.CacheManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlDdlPolicyTest {
    private String oldDirectory;
    private boolean oldDdlFlag;
    private MetadataAccess metadata;
    private MockedStatic<MetadataAccessFactory> factory;

    @BeforeEach
    void setUp() throws Exception {
        oldDirectory = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        oldDdlFlag = SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.getDefaultValue();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue("/unused/sql-ddl-policy-test");
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(false);
        metadata = new MetadataAccess(new SqlFileSchemaAccess(List.of(), List.of()),
                new InMemoryStoreAccess(List.of(), List.of(), List.of()), mock(HdfsAccess.class));
        factory = mockStatic(MetadataAccessFactory.class);
        factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
        CacheManager.invalidateAll();
    }

    @AfterEach
    void tearDown() {
        CacheManager.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(null);
        factory.close();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(oldDirectory);
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(oldDdlFlag);
    }

    @Test
    void restSqlUsesExecutorPolicyAndRequestParamsCannotChangeIt() throws Exception {
        var denied = RestSqlExecutor.execute("""
                {"sqls":["CREATE TABLE items (id INT)"],"params":{"SQL_SCHEMA_DDL_ENABLED":"true"}}
                """);
        assertTrue(denied.getData().get(0).getMsg().contains("SQL_SCHEMA_DDL_ENABLED=true"));
        assertTrue(metadata.getTables("default").isEmpty());
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(true);
        var allowed = RestSqlExecutor.execute("""
                {"sqls":["CREATE TABLE items (id INT)"],"params":{"SQL_SCHEMA_DDL_ENABLED":"false"}}
                """);
        assertNull(allowed.getData().get(0).getMsg());
        assertNotNull(metadata.getTable("default", "items"));
    }

    @Test
    void ddlCreatedApisExecuteAndRefreshAfterFunctionAndApiReplacement() throws Exception {
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(true);
        CalciteSchemaFactory.setGlobalSchema(null);
        SqlExecutor executor = new SqlExecutor();
        String tableDdl = "CREATE TABLE ddl_api_items (id BIGINT, label STRING, "
                + "PRIMARY KEY (id) NOT ENFORCED) WITH ('connector'='filesystem')";
        executor.executeSql(tableDdl);
        executor.executeSql("INSERT INTO ddl_api_items VALUES (7, 'created by DDL')");
        executor.executeSql("CREATE SQL FUNCTION ddl_api_function");
        executor.executeSql("RETURN SELECT CAST(id AS INT) AS id FROM ddl_api_items");
        executor.executeSql("CREATE API ddl_runtime_api WITH ddl_api_function");
        assertEquals(List.of(Map.of("id", 7)), RestFunctionExecutor.execute("ddl_runtime_api", "{}").getData());
        executor.executeSql("CREATE OR REPLACE SQL FUNCTION ddl_api_function");
        executor.executeSql("RETURN SELECT CAST(id AS INT) + 1 AS id FROM ddl_api_items");
        assertEquals(List.of(Map.of("id", 8)), RestFunctionExecutor.execute("ddl_runtime_api", "{}").getData());
        executor.executeSql("CREATE SQL FUNCTION ddl_api_replacement");
        executor.executeSql("RETURN SELECT CAST(id AS INT) + 2 AS id FROM ddl_api_items");
        executor.executeSql("CREATE OR REPLACE API ddl_runtime_api WITH ddl_api_replacement");
        assertEquals(List.of(Map.of("id", 9)), RestFunctionExecutor.execute("ddl_runtime_api", "{}").getData());
        assertThrows(Exception.class,
                () -> executor.executeSql("CREATE API ddl_runtime_api WITH missing_function"));
        assertEquals(List.of(Map.of("id", 9)), RestFunctionExecutor.execute("ddl_runtime_api", "{}").getData());
        executor.executeSql("DROP TABLE ddl_api_items");
        assertThrows(Exception.class, () -> RestFunctionExecutor.execute("ddl_runtime_api", "{}"));
        executor.executeSql(tableDdl);
        executor.executeSql("INSERT INTO ddl_api_items VALUES (10, 'recreated by DDL')");
        assertEquals(List.of(Map.of("id", 12)), RestFunctionExecutor.execute("ddl_runtime_api", "{}").getData());
        executor.executeSql("DROP API ddl_runtime_api");
        assertThrows(IllegalArgumentException.class, () -> RestFunctionExecutor.execute("ddl_runtime_api", "{}"));
        executor.executeSql("DROP SQL FUNCTION ddl_api_function");
        executor.executeSql("DROP SQL FUNCTION ddl_api_replacement");
        executor.executeSql("DROP TABLE ddl_api_items");
    }

    @Test
    void batchCliUsesTheSameExecutorDdlPolicy() throws Exception {
        PrintStream previous = System.out;
        PrintStream previousError = System.err;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        try (PrintStream captured = new PrintStream(output, true, StandardCharsets.UTF_8);
             PrintStream capturedError = new PrintStream(error, true, StandardCharsets.UTF_8)) {
            System.setOut(captured);
            System.setErr(capturedError);
            assertEquals(1, new CommandLine(new Cli()).execute("-e", "CREATE TABLE items (id INT)", "--outputformat", "json"));
            assertTrue(error.toString(StandardCharsets.UTF_8).contains("SQL_SCHEMA_DDL_ENABLED=true"));
            assertTrue(metadata.getTables("default").isEmpty());
            SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(true);
            output.reset();
            error.reset();
            assertEquals(0, new CommandLine(new Cli()).execute("-e", "CREATE TABLE items (id INT)", "--outputformat", "json"));
            assertEquals("", error.toString(StandardCharsets.UTF_8));
            assertNotNull(metadata.getTable("default", "items"));
        } finally {
            System.setOut(previous);
            System.setErr(previousError);
        }
    }
}
