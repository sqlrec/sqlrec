package com.sqlrec.connectors.milvus.handler;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.VectorSearchRequest;
import com.sqlrec.common.schema.VectorSearchResult;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
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
    public void testVectorSearchPushesCorrelatedFilterBeforeTopK() {
        SearchResp searchResp = mock(SearchResp.class);
        when(searchResp.getSearchResults()).thenReturn(Collections.emptyList());
        when(mockClient.search(any(SearchReq.class))).thenReturn(searchResp);

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelDataType joinedType = typeFactory.builder()
                .add("preferred_category", SqlTypeName.VARCHAR)
                .add("id", SqlTypeName.BIGINT)
                .add("name", SqlTypeName.VARCHAR)
                .add("embedding", SqlTypeName.ANY)
                .build();
        RexNode leftCategory = rexBuilder.makeInputRef(
                joinedType.getFieldList().get(0).getType(), 0);
        RexNode rightName = rexBuilder.makeInputRef(
                joinedType.getFieldList().get(2).getType(), 2);
        RexNode filter = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, rightName, leftCategory);
        VectorSearchRequest request = new VectorSearchRequest(
                new Object[]{"book"},
                Arrays.asList(0.1f, 0.2f),
                "embedding",
                filter,
                5);

        List<VectorSearchResult> results = handler.searchByEmbedding(request);

        assertTrue(results.isEmpty());
        org.mockito.ArgumentCaptor<SearchReq> captor =
                org.mockito.ArgumentCaptor.forClass(SearchReq.class);
        verify(mockClient).search(captor.capture());
        SearchReq actual = captor.getValue();
        assertEquals("name == \"book\"", actual.getFilter());
        assertEquals(5, actual.getTopK());
        assertEquals(Arrays.asList("id", "name", "embedding"), actual.getOutputFields());
    }

    @Test
    public void testVectorSearchParsesFullRowsScoresAndMultipleResultGroups() {
        SearchResp.SearchResult first = mock(SearchResp.SearchResult.class);
        Map<String, Object> firstEntity = new HashMap<>();
        firstEntity.put("id", 11L);
        firstEntity.put("name", "book");
        firstEntity.put("embedding", Arrays.asList(0.1f, 0.2f));
        when(first.getEntity()).thenReturn(firstEntity);
        when(first.getScore()).thenReturn(0.91f);

        SearchResp.SearchResult second = mock(SearchResp.SearchResult.class);
        Map<String, Object> secondEntity = new HashMap<>();
        secondEntity.put("id", 12L);
        secondEntity.put("name", "movie");
        secondEntity.put("embedding", Arrays.asList(0.3f, 0.4f));
        when(second.getEntity()).thenReturn(secondEntity);
        when(second.getScore()).thenReturn(0.82f);

        SearchResp searchResp = mock(SearchResp.class);
        when(searchResp.getSearchResults()).thenReturn(Arrays.asList(
                Collections.singletonList(first),
                Collections.singletonList(second)));
        when(mockClient.search(any(SearchReq.class))).thenReturn(searchResp);

        List<VectorSearchResult> results = handler.searchByEmbedding(
                new VectorSearchRequest(
                        new Object[]{1L},
                        Arrays.asList(0.5f, 0.6f),
                        "embedding",
                        null,
                        2));

        assertEquals(2, results.size());
        assertArrayEquals(
                new Object[]{11L, "book", Arrays.asList(0.1f, 0.2f)},
                results.get(0).getRow());
        assertEquals(0.91d, results.get(0).getScore(), 0.0001d);
        assertArrayEquals(
                new Object[]{12L, "movie", Arrays.asList(0.3f, 0.4f)},
                results.get(1).getRow());

        org.mockito.ArgumentCaptor<SearchReq> captor =
                org.mockito.ArgumentCaptor.forClass(SearchReq.class);
        verify(mockClient).search(captor.capture());
        SearchReq actual = captor.getValue();
        assertEquals("embedding", actual.getAnnsField());
        assertEquals(2, actual.getTopK());
        assertTrue(actual.getFilter() == null || actual.getFilter().isEmpty());
    }

    @Test
    public void testVectorSearchHandlesNullResponseAndNullResults() {
        when(mockClient.search(any(SearchReq.class))).thenReturn(null);
        VectorSearchRequest request = new VectorSearchRequest(
                new Object[]{1L},
                Arrays.asList(0.1f, 0.2f),
                "embedding",
                null,
                1);

        assertTrue(handler.searchByEmbedding(request).isEmpty());

        SearchResp response = mock(SearchResp.class);
        when(response.getSearchResults()).thenReturn(null);
        when(mockClient.search(any(SearchReq.class))).thenReturn(response);
        assertTrue(handler.searchByEmbedding(request).isEmpty());
    }

    @Test
    public void testVectorSearchRetriesOnConnectionFailure() {
        SearchResp response = mock(SearchResp.class);
        when(response.getSearchResults()).thenReturn(Collections.emptyList());
        when(mockClient.search(any(SearchReq.class)))
                .thenThrow(new StatusRuntimeException(
                        Status.UNAVAILABLE.withDescription("connection refused")))
                .thenReturn(response);

        List<VectorSearchResult> results = handler.searchByEmbedding(
                new VectorSearchRequest(
                        new Object[]{1L},
                        Collections.singletonList(0.1f),
                        "embedding",
                        null,
                        1));

        assertTrue(results.isEmpty());
        verify(mockClient, times(2)).search(any(SearchReq.class));
    }

    @Test
    public void testAddBatchWithNullRecords() {
        boolean result = handler.addBatch(null);

        assertTrue(result);
        verify(mockClient, never()).upsert(any(UpsertReq.class));
    }
}
