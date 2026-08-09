package com.sqlrec.connectors.milvus.handler;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class MilvusHandlerUnitTest {

    @Mock
    private MilvusClientV2 mockClient;

    private MilvusHandler handler;
    private MilvusConfig config;

    @BeforeEach
    public void setUp() {
        config = new MilvusConfig();
        config.url = "http://localhost:19530";
        config.token = "root:Milvus";
        config.database = "test_db";
        config.collection = "test_collection";
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INT64"),
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("embedding", "FLOAT_VECTOR")
        );

        handler = new MilvusHandler(config);
        MilvusHandler.setClientForTest(mockClient);
    }

    @AfterEach
    public void tearDown() {
        MilvusHandler.clearTestClient();
    }

    @Test
    public void testScan() {
        QueryResp.QueryResult result = mock(QueryResp.QueryResult.class);
        Map<String, Object> entity = new HashMap<>();
        entity.put("id", 1L);
        entity.put("name", "alice");
        entity.put("embedding", Arrays.asList(0.1f, 0.2f));
        when(result.getEntity()).thenReturn(entity);

        QueryResp queryResp = mock(QueryResp.class);
        when(queryResp.getQueryResults()).thenReturn(Collections.singletonList(result));
        when(mockClient.query(any(QueryReq.class))).thenReturn(queryResp);

        List<Object[]> rows = handler.scan(Collections.emptyList());

        assertEquals(1, rows.size());
        assertEquals(1L, rows.get(0)[0]);
        assertEquals("alice", rows.get(0)[1]);
    }

    @Test
    public void testScanEmptyResult() {
        QueryResp queryResp = mock(QueryResp.class);
        when(queryResp.getQueryResults()).thenReturn(Collections.emptyList());
        when(mockClient.query(any(QueryReq.class))).thenReturn(queryResp);

        List<Object[]> rows = handler.scan(Collections.emptyList());

        assertTrue(rows.isEmpty());
    }

    @Test
    public void testScanNullResponse() {
        QueryResp queryResp = mock(QueryResp.class);
        when(queryResp.getQueryResults()).thenReturn(null);
        when(mockClient.query(any(QueryReq.class))).thenReturn(queryResp);

        List<Object[]> rows = handler.scan(Collections.emptyList());

        assertTrue(rows.isEmpty());
    }

    @Test
    public void testGetByPrimaryKey() {
        QueryResp.QueryResult result = mock(QueryResp.QueryResult.class);
        Map<String, Object> entity = new HashMap<>();
        entity.put("id", 42L);
        entity.put("name", "bob");
        when(result.getEntity()).thenReturn(entity);

        QueryResp queryResp = mock(QueryResp.class);
        when(queryResp.getQueryResults()).thenReturn(Collections.singletonList(result));
        when(mockClient.query(any(QueryReq.class))).thenReturn(queryResp);

        Map<Object, List<Object[]>> result2 = handler.getByPrimaryKey(new HashSet<>(Arrays.asList(42)));

        assertEquals(1, result2.size());
        assertEquals(1, result2.get(42L).size());
    }

    @Test
    public void testAdd() {
        when(mockClient.upsert(any(UpsertReq.class))).thenReturn(null);

        Object[] row = new Object[]{1L, "alice", Arrays.asList(0.1f, 0.2f)};
        boolean result = handler.add(row);

        assertTrue(result);
        verify(mockClient, times(1)).upsert(any(UpsertReq.class));
    }

    @Test
    public void testAddBatch() {
        when(mockClient.upsert(any(UpsertReq.class))).thenReturn(null);

        List<Object[]> records = Arrays.asList(
                new Object[]{1L, "alice", Arrays.asList(0.1f, 0.2f)},
                new Object[]{2L, "bob", Arrays.asList(0.3f, 0.4f)}
        );
        boolean result = handler.addBatch(records);

        assertTrue(result);
        verify(mockClient, times(1)).upsert(any(UpsertReq.class));
    }

    @Test
    public void testAddBatchEmpty() {
        boolean result = handler.addBatch(Collections.emptyList());

        assertTrue(result);
        verify(mockClient, never()).upsert(any(UpsertReq.class));
    }

    @Test
    public void testRemove() {
        when(mockClient.delete(any(DeleteReq.class))).thenReturn(null);

        Object[] row = new Object[]{1L, "alice", Arrays.asList(0.1f, 0.2f)};
        boolean result = handler.remove(row);

        assertTrue(result);
        verify(mockClient, times(1)).delete(any(DeleteReq.class));
    }

    @Test
    public void testRemoveBatch() {
        when(mockClient.delete(any(DeleteReq.class))).thenReturn(null);

        List<Object[]> records = Arrays.asList(
                new Object[]{1L, "alice", Arrays.asList(0.1f, 0.2f)},
                new Object[]{2L, "bob", Arrays.asList(0.3f, 0.4f)}
        );
        boolean result = handler.removeBatch(records);

        assertTrue(result);
        verify(mockClient, times(1)).delete(any(DeleteReq.class));
    }

    @Test
    public void testRemoveBatchEmpty() {
        boolean result = handler.removeBatch(Collections.emptyList());

        assertTrue(result);
        verify(mockClient, never()).delete(any(DeleteReq.class));
    }

    @Test
    public void testScanClientThrowsRuntimeException() {
        when(mockClient.query(any(QueryReq.class))).thenThrow(new RuntimeException("query failed"));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> handler.scan(Collections.emptyList()));
        assertTrue(ex.getMessage().contains("query failed"));
    }

    @Test
    public void testScanRetriesOnConnectionFailure() {
        QueryResp.QueryResult result = mock(QueryResp.QueryResult.class);
        Map<String, Object> entity = new HashMap<>();
        entity.put("id", 1L);
        entity.put("name", "alice");
        entity.put("embedding", Arrays.asList(0.1f, 0.2f));
        when(result.getEntity()).thenReturn(entity);

        QueryResp queryResp = mock(QueryResp.class);
        when(queryResp.getQueryResults()).thenReturn(Collections.singletonList(result));

        when(mockClient.query(any(QueryReq.class)))
                .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("connection refused")))
                .thenReturn(queryResp);

        List<Object[]> rows = handler.scan(Collections.emptyList());

        assertEquals(1, rows.size());
        assertEquals(1L, rows.get(0)[0]);
        verify(mockClient, times(2)).query(any(QueryReq.class));
    }

    @Test
    public void testScanDoesNotRetryOnNonConnectionFailure() {
        when(mockClient.query(any(QueryReq.class)))
                .thenThrow(new StatusRuntimeException(Status.INVALID_ARGUMENT));

        assertThrows(RuntimeException.class,
                () -> handler.scan(Collections.emptyList()));
        verify(mockClient, times(1)).query(any(QueryReq.class));
    }

    @Test
    public void testAddThrowsOnClientException() {
        when(mockClient.upsert(any(UpsertReq.class))).thenThrow(new RuntimeException("upsert failed"));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> handler.add(new Object[]{1L, "test", Arrays.asList(0.1f)}));
        assertTrue(ex.getMessage().contains("upsert failed"));
    }

    @Test
    public void testRemoveThrowsOnClientException() {
        when(mockClient.delete(any(DeleteReq.class))).thenThrow(new RuntimeException("delete failed"));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> handler.remove(new Object[]{1L, "test", Arrays.asList(0.1f)}));
        assertTrue(ex.getMessage().contains("delete failed"));
    }

    @Test
    public void testScanWithNullQueryResponse() {
        when(mockClient.query(any(QueryReq.class))).thenReturn(null);

        List<Object[]> rows = handler.scan(Collections.emptyList());

        assertTrue(rows.isEmpty());
    }

    @Test
    public void testGetByPrimaryKeyEmptyResult() {
        QueryResp queryResp = mock(QueryResp.class);
        when(queryResp.getQueryResults()).thenReturn(Collections.emptyList());
        when(mockClient.query(any(QueryReq.class))).thenReturn(queryResp);

        Map<Object, List<Object[]>> result = handler.getByPrimaryKey(new HashSet<>(Arrays.asList(1)));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testAddBatchWithNullRecords() {
        boolean result = handler.addBatch(null);

        assertTrue(result);
        verify(mockClient, never()).upsert(any(UpsertReq.class));
    }
}
