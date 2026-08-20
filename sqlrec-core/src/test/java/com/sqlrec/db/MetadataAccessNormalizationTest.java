package com.sqlrec.db;

import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that MetadataAccess normalizes resource names to lower case on both
 * write (entity fields) and read/delete (query parameters), so StoreAccess
 * implementations can assume names are already normalized.
 */
class MetadataAccessNormalizationTest {

    @Test
    void insertSqlApiNormalizesNamesToLowerCase() {
        StoreAccess storeAccess = mock(StoreAccess.class);
        MetadataAccess db = new MetadataAccess(null, storeAccess, null);

        SqlApi sqlApi = new SqlApi();
        sqlApi.setName("MyApi");
        sqlApi.setFunctionName("MyFunc");
        db.insertSqlApi(sqlApi);

        ArgumentCaptor<SqlApi> captor = ArgumentCaptor.forClass(SqlApi.class);
        verify(storeAccess).insertSqlApi(captor.capture());
        assertEquals("myapi", captor.getValue().getName());
        assertEquals("myfunc", captor.getValue().getFunctionName());
    }

    @Test
    void upsertSqlApiNormalizesNamesToLowerCase() {
        StoreAccess storeAccess = mock(StoreAccess.class);
        MetadataAccess db = new MetadataAccess(null, storeAccess, null);

        SqlApi sqlApi = new SqlApi();
        sqlApi.setName("MyApi");
        sqlApi.setFunctionName("MyFunc");
        db.upsertSqlApi(sqlApi);

        ArgumentCaptor<SqlApi> captor = ArgumentCaptor.forClass(SqlApi.class);
        verify(storeAccess).upsertSqlApi(captor.capture());
        assertEquals("myapi", captor.getValue().getName());
        assertEquals("myfunc", captor.getValue().getFunctionName());
    }

    @Test
    void queryParametersAreNormalizedToLowerCase() {
        StoreAccess storeAccess = mock(StoreAccess.class);
        MetadataAccess db = new MetadataAccess(null, storeAccess, null);

        db.getSqlApi("MyApi");
        verify(storeAccess).getSqlApi("myapi");

        db.deleteSqlApi("MyApi");
        verify(storeAccess).deleteSqlApi("myapi");

        db.getSqlApiListByFunctionName("MyFunc");
        verify(storeAccess).getSqlApiListByFunctionName("myfunc");

        db.getSqlFunction("MyFunc");
        verify(storeAccess).getSqlFunction("myfunc");

        db.deleteSqlFunction("MyFunc");
        verify(storeAccess).deleteSqlFunction("myfunc");

        db.getCheckpoint("MyModel", "BestCkpt");
        verify(storeAccess).getCheckpoint("mymodel", "bestckpt");
    }

    @Test
    void insertSqlFunctionNormalizesNameToLowerCase() {
        StoreAccess storeAccess = mock(StoreAccess.class);
        MetadataAccess db = new MetadataAccess(null, storeAccess, null);
        when(storeAccess.getSqlFunction("myfunc")).thenReturn(null);

        SqlFunction sqlFunction = new SqlFunction();
        sqlFunction.setName("MyFunc");
        db.insertSqlFunction(sqlFunction);

        ArgumentCaptor<SqlFunction> captor = ArgumentCaptor.forClass(SqlFunction.class);
        verify(storeAccess).insertSqlFunction(captor.capture());
        assertEquals("myfunc", captor.getValue().getName());
    }

    @Test
    void insertModelNormalizesNameToLowerCase() {
        StoreAccess storeAccess = mock(StoreAccess.class);
        MetadataAccess db = new MetadataAccess(null, storeAccess, null);

        Model model = new Model();
        model.setName("MyModel");
        db.insertModel(model);

        ArgumentCaptor<Model> captor = ArgumentCaptor.forClass(Model.class);
        verify(storeAccess).insertModel(captor.capture());
        assertEquals("mymodel", captor.getValue().getName());

        db.getModel("MyModel");
        verify(storeAccess).getModel("mymodel");

        db.deleteModel("MyModel");
        verify(storeAccess).deleteModel("mymodel");
    }

    @Test
    void upsertCheckpointNormalizesNamesToLowerCase() {
        StoreAccess storeAccess = mock(StoreAccess.class);
        MetadataAccess db = new MetadataAccess(null, storeAccess, null);

        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setModelName("MyModel");
        checkpoint.setCheckpointName("BestCkpt");
        db.upsertCheckpoint(checkpoint);

        ArgumentCaptor<Checkpoint> captor = ArgumentCaptor.forClass(Checkpoint.class);
        verify(storeAccess).upsertCheckpoint(captor.capture());
        assertEquals("mymodel", captor.getValue().getModelName());
        assertEquals("bestckpt", captor.getValue().getCheckpointName());

        db.getCheckpointListByModelName("MyModel");
        verify(storeAccess).getCheckpointListByModelName("mymodel");

        db.deleteCheckpointByModelName("MyModel");
        verify(storeAccess).deleteCheckpointByModelName("mymodel");

        db.deleteCheckpoint("MyModel", "BestCkpt");
        verify(storeAccess).deleteCheckpoint("mymodel", "bestckpt");
    }

    @Test
    void upsertServiceNormalizesNamesToLowerCase() {
        StoreAccess storeAccess = mock(StoreAccess.class);
        MetadataAccess db = new MetadataAccess(null, storeAccess, null);

        Service service = new Service();
        service.setName("MyService");
        service.setModelName("MyModel");
        service.setCheckpointName("BestCkpt");
        db.upsertService(service);

        ArgumentCaptor<Service> captor = ArgumentCaptor.forClass(Service.class);
        verify(storeAccess).upsertService(captor.capture());
        assertEquals("myservice", captor.getValue().getName());
        assertEquals("mymodel", captor.getValue().getModelName());
        assertEquals("bestckpt", captor.getValue().getCheckpointName());

        db.getService("MyService");
        verify(storeAccess).getService("myservice");

        db.deleteService("MyService");
        verify(storeAccess).deleteService("myservice");

        db.getServiceListByModelName("MyModel");
        verify(storeAccess).getServiceListByModelName("mymodel");

        db.getServiceListByCheckpoint("MyModel", "BestCkpt");
        verify(storeAccess).getServiceListByCheckpoint("mymodel", "bestckpt");
    }
}
