package com.sqlrec.frontend.thrift;

import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.transport.TSocket;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LocalMetadataLifecycleTest {
    @Test
    void everySupportedJdbcMetadataRpcWorksWithGatewayDisabled() throws Exception {
        String oldAddress = SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getDefaultValue();
        SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue("");
        MetadataAccess metadata = mock(MetadataAccess.class);
        when(metadata.getDatabases()).thenReturn(List.of("default"));
        when(metadata.getTables("default")).thenReturn(List.of());
        when(metadata.getFunctions("default")).thenReturn(List.of());
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
        TCLIServiceImpl service = new TCLIServiceImpl();
        try (var factory = mockStatic(MetadataAccessFactory.class);
             var sockets = mockConstruction(TSocket.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            var session = service.OpenSession(new TOpenSessionReq()).getSessionHandle();
            TGetPrimaryKeysReq keys = new TGetPrimaryKeysReq(session);
            keys.setSchemaName("default");
            keys.setTableName("missing");
            List<TOperationHandle> handles = List.of(
                    service.GetTypeInfo(new TGetTypeInfoReq(session)).getOperationHandle(),
                    service.GetCatalogs(new TGetCatalogsReq(session)).getOperationHandle(),
                    service.GetSchemas(new TGetSchemasReq(session)).getOperationHandle(),
                    service.GetTables(new TGetTablesReq(session)).getOperationHandle(),
                    service.GetTableTypes(new TGetTableTypesReq(session)).getOperationHandle(),
                    service.GetColumns(new TGetColumnsReq(session)).getOperationHandle(),
                    service.GetFunctions(new TGetFunctionsReq(session, "%")).getOperationHandle(),
                    service.GetPrimaryKeys(keys).getOperationHandle());
            for (TOperationHandle handle : handles) {
                assertEquals(TOperationState.FINISHED_STATE,
                        service.GetOperationStatus(new TGetOperationStatusReq(handle)).getOperationState());
                assertEquals(TStatusCode.SUCCESS_STATUS, service.GetResultSetMetadata(
                        new TGetResultSetMetadataReq(handle)).getStatus().getStatusCode());
                assertEquals(TStatusCode.SUCCESS_STATUS, service.FetchResults(
                        new TFetchResultsReq(handle, TFetchOrientation.FETCH_NEXT, 100)).getStatus().getStatusCode());
                service.CloseOperation(new TCloseOperationReq(handle));
            }
            service.CloseSession(new TCloseSessionReq(session));
            assertTrue(sockets.constructed().isEmpty());
        } finally {
            service.stop();
            CalciteSchemaFactory.setGlobalSchema(null);
            SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue(oldAddress);
        }
    }

    @Test
    void ordinarySqlKeepsTheOriginalSingleFetchBehavior() throws Exception {
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        root.add("default", new AbstractSchema());
        CalciteSchemaFactory.setGlobalSchema(root);
        TCLIServiceImpl service = new TCLIServiceImpl();
        try {
            var session = service.OpenSession(new TOpenSessionReq()).getSessionHandle();
            var result = service.ExecuteStatement(new TExecuteStatementReq(session,
                    "SELECT 1 AS id UNION ALL SELECT 2 AS id"));
            assertEquals(TStatusCode.SUCCESS_STATUS, result.getStatus().getStatusCode());
            var handle = result.getOperationHandle();
            assertEquals(TOperationState.FINISHED_STATE,
                    service.GetOperationStatus(new TGetOperationStatusReq(handle)).getOperationState());
            // Ordinary SQL retains the old full-result fetch, even when maxRows is smaller.
            var first = service.FetchResults(new TFetchResultsReq(handle, TFetchOrientation.FETCH_NEXT, 1));
            assertEquals(List.of(1, 2), first.getResults().getColumns().get(0).getI32Val().getValues());
            assertFalse(first.isHasMoreRows());
            var second = service.FetchResults(new TFetchResultsReq(handle, TFetchOrientation.FETCH_FIRST, 1));
            assertTrue(second.getResults().getColumns().get(0).getI32Val().getValues().isEmpty());
            service.CloseOperation(new TCloseOperationReq(handle));
            service.CloseSession(new TCloseSessionReq(session));
        } finally {
            service.stop();
            CalciteSchemaFactory.setGlobalSchema(null);
        }
    }

    @Test
    void jdbcMetadataStatusFetchAndCloseStayOnTheLocalOperationPath() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        when(metadata.getDatabases()).thenReturn(List.of("default", "review"));
        CalciteSchemaFactory.setGlobalSchema(CalciteSchema.createRootSchema(false));
        TCLIServiceImpl service = new TCLIServiceImpl();
        try (var factory = mockStatic(MetadataAccessFactory.class)) {
            factory.when(MetadataAccessFactory::getInstance).thenReturn(metadata);
            var session = service.OpenSession(new TOpenSessionReq()).getSessionHandle();
            TGetSchemasReq request = new TGetSchemasReq();
            request.setSessionHandle(session);
            var result = service.GetSchemas(request);
            assertEquals(TStatusCode.SUCCESS_STATUS, result.getStatus().getStatusCode());
            var handle = result.getOperationHandle();
            assertEquals(TOperationState.FINISHED_STATE,
                    service.GetOperationStatus(new TGetOperationStatusReq(handle)).getOperationState());
            assertEquals(2, service.GetResultSetMetadata(new TGetResultSetMetadataReq(handle)).getSchema().getColumnsSize());
            var first = service.FetchResults(new TFetchResultsReq(handle, TFetchOrientation.FETCH_NEXT, 1));
            assertTrue(first.isHasMoreRows());
            assertEquals(0, first.getResults().getStartRowOffset());
            var second = service.FetchResults(new TFetchResultsReq(handle, TFetchOrientation.FETCH_NEXT, 1));
            assertFalse(second.isHasMoreRows());
            assertEquals(1, second.getResults().getStartRowOffset());
            assertEquals(TStatusCode.SUCCESS_STATUS,
                    service.CloseOperation(new TCloseOperationReq(handle)).getStatus().getStatusCode());
            assertEquals(TStatusCode.SUCCESS_STATUS,
                    service.CloseSession(new TCloseSessionReq(session)).getStatus().getStatusCode());
            verify(metadata, times(1)).getDatabases();
        } finally {
            service.stop();
            CalciteSchemaFactory.setGlobalSchema(null);
        }
    }
}
