package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.db.HdfsAccess;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.StoreAccess;
import com.sqlrec.db.remote.FlinkHiveDdlAdapter;
import com.sqlrec.db.remote.HmsClient;
import com.sqlrec.db.remote.HmsSchemaAccess;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.transport.TSocket;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Real Thrift handlers, SqlExecutor, metadata adapter and HiveCatalog; only HMS I/O is simulated. */
class LocalDdlLifecycleTest {
    @Test
    void tableDdlAndJdbcMetadataWorkWithGatewayDisabled() throws Exception {
        String previousAddress = SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getDefaultValue();
        SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue("");
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
        Map<String, Table> tables = new HashMap<>();
        Map<String, Database> databases = new HashMap<>();
        HiveMetastoreClientWrapper client = metastore(tables, databases);
        MetadataAccess metadata = new MetadataAccess(new HmsSchemaAccess(), mock(StoreAccess.class), mock(HdfsAccess.class));
        TCLIServiceImpl service = new TCLIServiceImpl();
        try (var clients = mockStatic(HiveMetastoreClientFactory.class);
             var factory = mockStatic(MetadataAccessFactory.class);
             var hms = mockStatic(HmsClient.class);
             var sockets = mockConstruction(TSocket.class)) {
            clients.when(() -> HiveMetastoreClientFactory.create(any(HiveConf.class), eq(Consts.HIVE_CLIENT_VERSION)))
                    .thenReturn(client);
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            hms.when(HmsClient::getAllDatabases).thenAnswer(call -> databases.keySet().stream().sorted().toList());
            hms.when(() -> HmsClient.getAllTables(anyString()))
                    .thenAnswer(call -> tables.values().stream().filter(table -> table.getDbName().equals(call.getArgument(0)))
                            .map(Table::getTableName).sorted().toList());
            hms.when(() -> HmsClient.getTableObj(anyString(), anyString()))
                    .thenAnswer(call -> client.getTable(call.getArgument(0), call.getArgument(1)));
            hms.when(() -> HmsClient.getPrimaryKeys(anyString(), anyString())).thenReturn(List.of());
            var opened = service.OpenSession(new TOpenSessionReq());
            assertEquals(TStatusCode.SUCCESS_STATUS, opened.getStatus().getStatusCode());
            TSessionHandle session = opened.getSessionHandle();
            try {
                execute(service, session, "CREATE DATABASE review", TOperationState.FINISHED_STATE);
                assertTrue(databases.containsKey("review"));
                // The session's schema was created before review existed.
                execute(service, session, "USE review", TOperationState.FINISHED_STATE);
                String create = "CREATE TABLE items (id BIGINT, amount DECIMAL(10, 2))"
                        + " WITH ('connector'='filesystem', 'path'='file:///tmp/items', 'format'='json')";
                execute(service, session, create, TOperationState.FINISHED_STATE);
                assertEquals("review", tables.get("items").getDbName());
                assertEquals(List.of("id", "amount"), describe(service, session, "items"));
                TGetTablesReq listing = new TGetTablesReq(session);
                listing.setSchemaName("review");
                assertEquals(List.of("items"), fetch(service, service.GetTables(listing).getOperationHandle())
                        .getColumns().get(2).getStringVal().getValues());
                execute(service, session, create, TOperationState.ERROR_STATE);
                execute(service, session, create.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
                        .replace("file:///tmp/items", "file:///tmp/ignored"), TOperationState.FINISHED_STATE);
                assertEquals("file:///tmp/items", HiveTableUtils.getFlinkTableOptions(tables.get("items")).get("path"));

                execute(service, session, "ALTER TABLE items SET ('path'='file:///tmp/items-v2')", TOperationState.FINISHED_STATE);
                var shown = execute(service, session, "SHOW CREATE TABLE items", TOperationState.FINISHED_STATE);
                assertTrue(shown.getColumns().get(0).getStringVal().getValues().get(0).contains("items-v2"));
                execute(service, session, "ALTER TABLE items ADD note STRING", TOperationState.FINISHED_STATE);
                assertEquals(List.of("id", "amount", "note"), describe(service, session, "items"));
                execute(service, session, "ALTER TABLE items MODIFY note VARCHAR(100)", TOperationState.FINISHED_STATE);
                TGetColumnsReq columns = new TGetColumnsReq(session);
                columns.setSchemaName("review");
                columns.setTableName("items");
                var fields = fetch(service, service.GetColumns(columns).getOperationHandle());
                assertEquals(List.of("id", "amount", "note"), fields.getColumns().get(3).getStringVal().getValues());
                assertEquals("VARCHAR(100)", fields.getColumns().get(5).getStringVal().getValues().get(2));
                execute(service, session, "ALTER TABLE items DROP note", TOperationState.FINISHED_STATE);
                assertEquals(List.of("id", "amount"), describe(service, session, "items"));
                execute(service, session, "ALTER TABLE items RENAME TO items_renamed", TOperationState.FINISHED_STATE);
                assertFalse(tables.containsKey("items"));
                assertEquals(List.of("id", "amount"), describe(service, session, "items_renamed"));
                execute(service, session, "DESCRIBE items", TOperationState.ERROR_STATE);
                execute(service, session, "DROP TABLE items_renamed", TOperationState.FINISHED_STATE);
                assertTrue(tables.isEmpty());
                assertTrue(fetch(service, service.GetTables(listing).getOperationHandle())
                        .getColumns().get(2).getStringVal().getValues().isEmpty());
                execute(service, session, "DESCRIBE items_renamed", TOperationState.ERROR_STATE);
                execute(service, session, "DROP TABLE items_renamed", TOperationState.ERROR_STATE);
                execute(service, session, "DROP TABLE IF EXISTS items_renamed", TOperationState.FINISHED_STATE);
                execute(service, session, "USE CATALOG hive", TOperationState.ERROR_STATE);
                execute(service, session, "CREATE TEMPORARY TABLE temp_items (id BIGINT)", TOperationState.ERROR_STATE);
                execute(service, session, "DROP DATABASE review", TOperationState.ERROR_STATE);
                assertTrue(databases.containsKey("review"));
                execute(service, session, "USE `default`", TOperationState.FINISHED_STATE);
                execute(service, session, "DROP DATABASE review", TOperationState.FINISHED_STATE);
                assertFalse(databases.containsKey("review"));
                assertTrue(sockets.constructed().isEmpty(), "Local DDL must not create a Gateway socket");
            } finally {
                service.CloseSession(new TCloseSessionReq(session));
            }
        } finally {
            service.stop();
            try {
                var close = FlinkHiveDdlAdapter.class.getDeclaredMethod("closeCatalog");
                close.setAccessible(true);
                close.invoke(null);
            } finally {
                CalciteSchemaFactory.setGlobalSchema(null);
                SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue(previousAddress);
            }
        }
    }

    static List<String> describe(TCLIServiceImpl service, TSessionHandle session, String table) throws Exception {
        return execute(service, session, "DESCRIBE " + table, TOperationState.FINISHED_STATE)
                .getColumns().get(0).getStringVal().getValues();
    }

    static TRowSet execute(TCLIServiceImpl service, TSessionHandle session, String sql,
                                   TOperationState expected) throws Exception {
        var request = new TExecuteStatementReq(session, sql);
        request.setRunAsync(true);
        var result = service.ExecuteStatement(request);
        assertEquals(TStatusCode.SUCCESS_STATUS, result.getStatus().getStatusCode(), sql);
        var handle = result.getOperationHandle();
        var status = service.GetOperationStatus(new TGetOperationStatusReq(handle));
        assertEquals(expected, status.getOperationState(), sql + ": " + status.getErrorMessage());
        if (expected == TOperationState.ERROR_STATE) {
            assertNotNull(status.getErrorMessage());
            assertEquals(TStatusCode.SUCCESS_STATUS, service.CloseOperation(new TCloseOperationReq(handle)).getStatus().getStatusCode());
            return null;
        }
        return fetch(service, handle);
    }

    static TRowSet fetch(TCLIServiceImpl service, TOperationHandle handle) throws Exception {
        try {
            assertNotNull(handle);
            assertEquals(TOperationState.FINISHED_STATE,
                    service.GetOperationStatus(new TGetOperationStatusReq(handle)).getOperationState());
            assertEquals(TStatusCode.SUCCESS_STATUS, service.GetResultSetMetadata(new TGetResultSetMetadataReq(handle))
                    .getStatus().getStatusCode());
            var result = service.FetchResults(new TFetchResultsReq(handle, TFetchOrientation.FETCH_NEXT, 100));
            assertEquals(TStatusCode.SUCCESS_STATUS, result.getStatus().getStatusCode());
            return result.getResults();
        } finally {
            assertEquals(TStatusCode.SUCCESS_STATUS, service.CloseOperation(new TCloseOperationReq(handle))
                    .getStatus().getStatusCode());
        }
    }

    private static HiveMetastoreClientWrapper metastore(Map<String, Table> tables, Map<String, Database> databases) throws Exception {
        HiveMetastoreClientWrapper client = mock(HiveMetastoreClientWrapper.class);
        databases.put("default", new Database("default", "", "file:///tmp", Map.of()));
        when(client.getDatabase(anyString())).thenAnswer(call -> {
            Database database = databases.get(call.<String>getArgument(0));
            if (database == null) throw new NoSuchObjectException();
            return database.deepCopy();
        });
        doAnswer(call -> {
            Database database = call.getArgument(0);
            if (databases.containsKey(database.getName())) throw new AlreadyExistsException();
            databases.put(database.getName(), database.deepCopy());
            return null;
        }).when(client).createDatabase(any(Database.class));
        doAnswer(call -> {
            if (databases.remove(call.<String>getArgument(0)) == null && !call.<Boolean>getArgument(2)) {
                throw new NoSuchObjectException();
            }
            return null;
        }).when(client).dropDatabase(anyString(), anyBoolean(), anyBoolean(), anyBoolean());
        when(client.getTable(anyString(), anyString())).thenAnswer(call -> {
            Table table = tables.get(call.<String>getArgument(1));
            if (table == null || !table.getDbName().equals(call.getArgument(0))) throw new NoSuchObjectException();
            return table.deepCopy();
        });
        when(client.tableExists(anyString(), anyString())).thenAnswer(call -> {
            Table table = tables.get(call.<String>getArgument(1));
            return table != null && table.getDbName().equals(call.getArgument(0));
        });
        doAnswer(call -> {
            Table table = call.getArgument(0);
            if (tables.containsKey(table.getTableName())) throw new AlreadyExistsException();
            tables.put(table.getTableName(), table.deepCopy());
            return null;
        }).when(client).createTable(any(Table.class));
        doAnswer(call -> {
            String name = call.getArgument(1);
            if (!tables.containsKey(name)) throw new NoSuchObjectException();
            Table table = call.getArgument(2);
            tables.remove(name);
            tables.put(table.getTableName(), table.deepCopy());
            return null;
        }).when(client).alter_table(anyString(), anyString(), any(Table.class));
        doAnswer(call -> {
            if (tables.remove(call.<String>getArgument(1)) == null && !call.<Boolean>getArgument(3)) {
                throw new NoSuchObjectException();
            }
            return null;
        }).when(client).dropTable(anyString(), anyString(), anyBoolean(), anyBoolean());
        return client;
    }
}
