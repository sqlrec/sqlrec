package com.sqlrec.frontend.thrift;

import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.HdfsAccess;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.StoreAccess;
import com.sqlrec.db.remote.HmsSchemaAccess;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.rpc.thrift.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static com.sqlrec.frontend.thrift.LocalDdlLifecycleTest.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Opt-in: actual HMS I/O through the Thrift handlers with Gateway forwarding disabled. */
@Tag("integration")
class GatewayIndependentDdlIntegrationTest {
    @Test
    @SilenceLoggers(SessionManager.class)
    void thriftTableLifecycleAndJdbcMetadataUseRealHmsWithoutGateway() throws Exception {
        String database = "sqlrec_thrift_" + UUID.randomUUID().toString().replace("-", "");
        String previousAddress = SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getDefaultValue();
        SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue("");
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
        HmsSchemaAccess schemaAccess = new HmsSchemaAccess();
        MetadataAccess metadata = new MetadataAccess(schemaAccess, mock(StoreAccess.class), mock(HdfsAccess.class));
        TCLIServiceImpl service = new TCLIServiceImpl();
        HiveConf conf = new HiveConf();
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, SqlRecConfigs.HIVE_METASTORE_URI.getValue());
        HiveCatalog reader = new HiveCatalog(Consts.HIVE_CATALOG_NAME, "default", conf, Consts.HIVE_CLIENT_VERSION);
        try (var factory = mockStatic(MetadataAccessFactory.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            reader.open();
            var opened = service.OpenSession(new TOpenSessionReq());
            assertEquals(TStatusCode.SUCCESS_STATUS, opened.getStatus().getStatusCode());
            TSessionHandle session = opened.getSessionHandle();
            try {
                execute(service, session, "CREATE DATABASE " + database, TOperationState.FINISHED_STATE);
                // The new DB was absent when the session's schema was created.
                execute(service, session, "USE " + database, TOperationState.FINISHED_STATE);
                execute(service, session, "CREATE TABLE items (id BIGINT, amount DECIMAL(10, 2))"
                        + " WITH ('connector'='filesystem', 'path'='file:///tmp/items', 'format'='json')",
                        TOperationState.FINISHED_STATE);
                ObjectPath original = new ObjectPath(database, "items");
                assertTrue(reader.tableExists(original));
                assertEquals("filesystem", reader.getTable(original).getOptions().get("connector"));
                assertEquals(List.of("id", "amount"), describe(service, session, "items"));
                execute(service, session, "ALTER TABLE items SET ('path'='file:///tmp/items-v2')", TOperationState.FINISHED_STATE);
                assertEquals("file:///tmp/items-v2", reader.getTable(original).getOptions().get("path"));
                assertTrue(execute(service, session, "SHOW CREATE TABLE items", TOperationState.FINISHED_STATE)
                        .getColumns().get(0).getStringVal().getValues().get(0).contains("items-v2"));
                execute(service, session, "ALTER TABLE items ADD note STRING", TOperationState.FINISHED_STATE);
                execute(service, session, "ALTER TABLE items MODIFY note VARCHAR(100)", TOperationState.FINISHED_STATE);
                TGetColumnsReq columns = new TGetColumnsReq(session);
                columns.setSchemaName(database);
                columns.setTableName("items");
                var fields = fetch(service, service.GetColumns(columns).getOperationHandle());
                assertEquals(List.of("id", "amount", "note"), fields.getColumns().get(3).getStringVal().getValues());
                assertEquals("VARCHAR(100)", fields.getColumns().get(5).getStringVal().getValues().get(2));
                execute(service, session, "ALTER TABLE items DROP note", TOperationState.FINISHED_STATE);
                assertEquals(List.of("id", "amount"), describe(service, session, "items"));
                execute(service, session, "ALTER TABLE items RENAME TO items_renamed", TOperationState.FINISHED_STATE);
                ObjectPath renamed = new ObjectPath(database, "items_renamed");
                assertFalse(reader.tableExists(original));
                assertTrue(reader.tableExists(renamed));
                assertEquals("file:///tmp/items-v2", reader.getTable(renamed).getOptions().get("path"));
                TGetTablesReq listing = new TGetTablesReq(session);
                listing.setSchemaName(database);
                assertEquals(List.of("items_renamed"), fetch(service, service.GetTables(listing).getOperationHandle())
                        .getColumns().get(2).getStringVal().getValues());
                execute(service, session, "DROP TABLE items_renamed", TOperationState.FINISHED_STATE);
                assertFalse(reader.tableExists(renamed));
                assertTrue(fetch(service, service.GetTables(listing).getOperationHandle())
                        .getColumns().get(2).getStringVal().getValues().isEmpty());
                execute(service, session, "DESCRIBE items_renamed", TOperationState.ERROR_STATE);
                execute(service, session, "USE `default`", TOperationState.FINISHED_STATE);
                execute(service, session, "DROP DATABASE " + database, TOperationState.FINISHED_STATE);
                assertFalse(reader.databaseExists(database));
            } finally {
                try {
                    schemaAccess.executeMetadataDdl(CompileManager.parseSql(
                            "DROP DATABASE IF EXISTS " + database + " CASCADE"), "default");
                } finally {
                    service.CloseSession(new TCloseSessionReq(session));
                }
            }
        } finally {
            service.stop();
            try {
                try {
                    reader.close();
                } finally {
                    schemaAccess.close();
                }
            } finally {
                CalciteSchemaFactory.setGlobalSchema(null);
                SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue(previousAddress);
            }
        }
    }
}
