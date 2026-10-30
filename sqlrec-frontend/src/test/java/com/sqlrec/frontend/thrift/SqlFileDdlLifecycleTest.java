package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.db.HdfsAccess;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.SqlFileParser;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.db.remote.HmsClient;
import com.sqlrec.schema.CacheManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.transport.TSocket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.sqlrec.frontend.thrift.LocalDdlLifecycleTest.execute;
import static com.sqlrec.frontend.thrift.LocalDdlLifecycleTest.fetch;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlFileDdlLifecycleTest {
    @Test
    @SilenceLoggers(SessionManager.class)
    void writableFileMetadataWorksAcrossThriftSessionsAndJdbcWithoutExternalServices(@TempDir Path directory) throws Exception {
        String initial = "CREATE TABLE seeded (id INT) WITH ('connector'='not_installed');";
        Path schemaFile = directory.resolve("schema.sql");
        Files.writeString(schemaFile, initial);
        SqlFileParser parser = new SqlFileParser(directory.toString());
        parser.load();
        var files = new SqlFileSchemaAccess(parser.getTableNodes(), parser.getUdfFunctionNodes());
        var metadata = new MetadataAccess(files,
                new InMemoryStoreAccess(List.of(), List.of(), List.of()), mock(HdfsAccess.class));
        String previousDirectory = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        String previousGateway = SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getDefaultValue();
        boolean previousDdlFlag = SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.getDefaultValue();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(directory.toString());
        SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue("");
        SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(true);
        CalciteSchemaFactory.setGlobalSchema(null);
        TCLIServiceImpl service = new TCLIServiceImpl();
        try (var factory = mockStatic(MetadataAccessFactory.class);
             var hms = mockStatic(HmsClient.class);
             var clients = mockStatic(HiveMetastoreClientFactory.class);
             var sockets = mockConstruction(TSocket.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            CacheManager.invalidateAll();
            var first = service.OpenSession(new TOpenSessionReq()).getSessionHandle();
            var second = service.OpenSession(new TOpenSessionReq()).getSessionHandle();
            try {
                execute(service, first, "ALTER TABLE seeded ADD label STRING", TOperationState.FINISHED_STATE);
                execute(service, first, "CREATE DATABASE runtime_db", TOperationState.FINISHED_STATE);
                execute(service, first, "USE runtime_db", TOperationState.FINISHED_STATE);
                execute(service, first, "CREATE TABLE items (id BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('connector'='not_installed')", TOperationState.FINISHED_STATE);
                execute(service, second, "USE runtime_db", TOperationState.FINISHED_STATE);
                assertEquals(List.of("items"), execute(service, second, "SHOW TABLES", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues());
                TGetTablesReq tables = new TGetTablesReq(second);
                tables.setSchemaName("runtime_db");
                assertEquals(List.of("items"), fetch(service, service.GetTables(tables).getOperationHandle())
                        .getColumns().get(2).getStringVal().getValues());
                execute(service, first, "ALTER TABLE items ADD amount DECIMAL(12, 3)", TOperationState.FINISHED_STATE);
                TGetColumnsReq columns = new TGetColumnsReq(second);
                columns.setSchemaName("runtime_db");
                columns.setTableName("items");
                var fields = fetch(service, service.GetColumns(columns).getOperationHandle());
                assertEquals(List.of("id", "amount"), fields.getColumns().get(3).getStringVal().getValues());
                assertEquals(List.of("BIGINT", "DECIMAL(12, 3)"), fields.getColumns().get(5).getStringVal().getValues());
                assertEquals(List.of(0, 1), fields.getColumns().get(10).getI32Val().getValues());
                TGetPrimaryKeysReq keys = new TGetPrimaryKeysReq(second);
                keys.setSchemaName("runtime_db");
                keys.setTableName("items");
                assertEquals(List.of("id"), fetch(service, service.GetPrimaryKeys(keys).getOperationHandle())
                        .getColumns().get(3).getStringVal().getValues());
                execute(service, first, "CREATE FUNCTION f AS 'missing.Udf' USING JAR 'file:///missing.jar'",
                        TOperationState.FINISHED_STATE);
                assertEquals(List.of("f"), execute(service, second, "SHOW USER FUNCTIONS", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues());
                execute(service, first, "CREATE SQL FUNCTION runtime_function", TOperationState.FINISHED_STATE);
                execute(service, first, "RETURN SELECT 7 AS id", TOperationState.FINISHED_STATE);
                execute(service, first, "CREATE API runtime_api WITH runtime_function", TOperationState.FINISHED_STATE);
                assertEquals(List.of("runtime_api"), execute(service, second, "SHOW APIS", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues());
                assertEquals(List.of(7), execute(service, second, "CALL runtime_function()", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getI32Val().getValues());
                execute(service, first, "FLUSH", TOperationState.FINISHED_STATE);
                assertEquals(List.of("id", "amount"), execute(service, second, "DESCRIBE items", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues());
                execute(service, first, "DROP DATABASE runtime_db CASCADE", TOperationState.ERROR_STATE);
                execute(service, first, "USE DEFAULT", TOperationState.FINISHED_STATE);
                execute(service, first, "DROP DATABASE runtime_db CASCADE", TOperationState.FINISHED_STATE);
                execute(service, second, "DESCRIBE items", TOperationState.ERROR_STATE);
                execute(service, first, "DROP API runtime_api", TOperationState.FINISHED_STATE);
                execute(service, first, "DROP SQL FUNCTION runtime_function", TOperationState.FINISHED_STATE);
                execute(service, first, "DROP TABLE seeded", TOperationState.FINISHED_STATE);
                assertEquals(initial, Files.readString(schemaFile));
                assertTrue(metadata.getTables("default").isEmpty());
                assertTrue(metadata.getSqlFunctionList().isEmpty());
                assertTrue(metadata.getSqlApiList().isEmpty());
            } finally {
                service.CloseSession(new TCloseSessionReq(first));
                service.CloseSession(new TCloseSessionReq(second));
            }
            hms.verifyNoInteractions();
            clients.verifyNoInteractions();
            assertTrue(sockets.constructed().isEmpty());
        } finally {
            service.stop();
            CacheManager.invalidateAll();
            CalciteSchemaFactory.setGlobalSchema(null);
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(previousDirectory);
            SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue(previousGateway);
            SqlRecConfigs.SQL_SCHEMA_DDL_ENABLED.setDefaultValue(previousDdlFlag);
        }
    }
}
