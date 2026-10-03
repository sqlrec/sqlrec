package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.db.HdfsAccess;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.StoreAccess;
import com.sqlrec.db.local.SqlFileParser;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.db.remote.HmsClient;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.transport.TSocket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.sqlrec.frontend.thrift.LocalDdlLifecycleTest.execute;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlFileMetadataLifecycleTest {
    @Test
    void fileMetadataQueriesWorkWithoutHmsGatewayOrInstalledConnectors(@TempDir Path directory) throws Exception {
        Files.writeString(directory.resolve("schema.sql"), """
                CREATE TABLE hive.review.items (id BIGINT, event_time TIMESTAMP(3) METADATA FROM 'timestamp',
                    PRIMARY KEY (id) NOT ENFORCED) WITH ('connector'='not_installed');
                CREATE FUNCTION hive.review.file_udf AS 'not.installed.Udf';
                """);
        SqlFileParser parser = new SqlFileParser(directory.toString());
        parser.load();
        var files = new SqlFileSchemaAccess(parser.getTableNodes(), parser.getUdfFunctionNodes());
        var metadata = new MetadataAccess(files, mock(StoreAccess.class), mock(HdfsAccess.class));
        String previousDirectory = SqlRecConfigs.SQL_SCHEMA_DIR.getDefaultValue();
        String previousGateway = SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getDefaultValue();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(directory.toString());
        SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue("");
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
        TCLIServiceImpl service = new TCLIServiceImpl();
        try (var factory = mockStatic(MetadataAccessFactory.class);
             var hms = mockStatic(HmsClient.class);
             var hiveClients = mockStatic(HiveMetastoreClientFactory.class);
             var sockets = mockConstruction(TSocket.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            var session = service.OpenSession(new TOpenSessionReq()).getSessionHandle();
            try {
                execute(service, session, "USE review", TOperationState.FINISHED_STATE);
                assertEquals(List.of("items"), execute(service, session, "SHOW TABLES", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues());
                var description = execute(service, session, "DESCRIBE hive.review.items", TOperationState.FINISHED_STATE);
                assertEquals(6, description.getColumnsSize());
                assertEquals(List.of("id", "event_time"), description.getColumns().get(0).getStringVal().getValues());
                assertTrue(execute(service, session, "SHOW CREATE TABLE items", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues().get(0).contains("not_installed"));
                assertEquals(List.of("file_udf"), execute(service, session,
                        "SHOW USER FUNCTIONS IN hive.review ILIKE 'FILE_%'", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues());
                assertTrue(execute(service, session, "SHOW FUNCTIONS", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues().size() > 1);

                execute(service, session, "CACHE TABLE items AS SELECT 1 AS cached_id", TOperationState.FINISHED_STATE);
                assertEquals(List.of("cached_id"), execute(service, session, "DESCRIBE items", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues());
                assertEquals(List.of("id", "event_time"), execute(service, session,
                        "DESCRIBE review.items", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues());
                execute(service, session, "DESCRIBE missing", TOperationState.ERROR_STATE);
                execute(service, session, "SHOW CREATE TABLE other.review.items", TOperationState.ERROR_STATE);
                execute(service, session, "DROP TABLE review.items", TOperationState.ERROR_STATE);
                // File edits and FLUSH do not reload the definitions or synchronize the session schema.
                Files.writeString(directory.resolve("schema.sql"), "CREATE TABLE review.changed (id INT);");
                execute(service, session, "FLUSH", TOperationState.FINISHED_STATE);
                assertEquals(List.of("id", "event_time"), execute(service, session,
                        "DESCRIBE review.items", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues());
            } finally {
                service.CloseSession(new TCloseSessionReq(session));
            }
            hms.verifyNoInteractions();
            hiveClients.verifyNoInteractions();
            assertTrue(sockets.constructed().isEmpty());
        } finally {
            service.stop();
            CalciteSchemaFactory.setGlobalSchema(null);
            SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(previousDirectory);
            SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue(previousGateway);
        }
    }
}
