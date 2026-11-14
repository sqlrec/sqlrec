package com.sqlrec.connectors.milvus.handler;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.VectorSearchRequest;
import com.sqlrec.common.schema.VectorSearchResult;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.filter.MilvusFilterBuilder;
import com.sqlrec.connectors.milvus.utils.MilvusClientPoolUtils;
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
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MilvusHandler {
    private static final Logger logger = LoggerFactory.getLogger(MilvusHandler.class);
    private static final Gson gson = JsonUtils.getGson();

    private final MilvusConfig milvusConfig;

    /** Test-only: when non-null, doWithClient uses this directly instead of the pool. */
    static volatile MilvusClientV2 testClient;

    public MilvusHandler(MilvusConfig milvusConfig) {
        this.milvusConfig = milvusConfig;
    }

    /** Test-only: inject a mock client. */
    static void setClientForTest(MilvusClientV2 mockClient) {
        testClient = mockClient;
    }

    /** Test-only: clear the injected mock client. */
    static void clearTestClient() {
        testClient = null;
    }

    private <T> T withClient(Function<MilvusClientV2, T> action) {
        String key = MilvusClientPoolUtils.poolKey(milvusConfig);
        try {
            return doWithClient(action);
        } catch (RuntimeException e) {
            if (isConnectionFailure(e)) {
                logger.warn("Milvus connection failure detected, invalidating pool and retrying once: {}", e.getMessage());
                invalidatePool(key);
                return doWithClient(action);
            }
            throw e;
        }
    }

    private <T> T doWithClient(Function<MilvusClientV2, T> action) {
        if (testClient != null) {
            return action.apply(testClient);
        }
        MilvusClientV2 client = getClient(milvusConfig);
        if (client == null) {
            throw new RuntimeException("Failed to get Milvus client from pool after timeout");
        }
        try {
            return action.apply(client);
        } finally {
            try {
                returnClient(client, milvusConfig);
            } catch (Exception e) {
                // Avoid masking the original failure from action.apply().
                logger.warn("Failed to return Milvus client to pool: {}", e.getMessage());
            }
        }
    }

    /**
     * Returns true if the throwable (or any cause in its chain) is a gRPC transport-level
     * failure (connection down, timeout, broken stream) rather than a semantic Milvus
     * error. Such failures mean the pool may hold broken clients and should be discarded.
     */
    private static boolean isConnectionFailure(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof StatusRuntimeException) {
                Status.Code code = ((StatusRuntimeException) cur).getStatus().getCode();
                return code == Status.Code.UNAVAILABLE
                        || code == Status.Code.DEADLINE_EXCEEDED
                        || code == Status.Code.UNKNOWN;
            }
            cur = cur.getCause();
        }
        return false;
    }

    public List<Object[]> scan(List<RexNode> filters) {
        String filterSql = MilvusFilterBuilder.buildScanFilter(filters, milvusConfig.fieldSchemas);
        // FilterableTable keeps predicates that are not removed from the supplied list,
        // so Calcite evaluates them after this scan. Milvus 2.6 requires a limit for an
        // empty expression; use the non-null primary key as an unlimited match-all filter
        // instead of imposing a fixed limit that could silently truncate a table scan.
        if (filterSql == null || filterSql.isEmpty()) {
            filterSql = MilvusFilterBuilder.buildMatchAllFilter(
                    milvusConfig.fieldSchemas, milvusConfig.primaryKeyIndex);
        }
        QueryReq queryReq = QueryReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .filter(filterSql)
                .build();

        QueryResp queryResp = withClient(client -> client.query(queryReq));
        return parseQueryResp(queryResp);
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        if (keySet == null || keySet.isEmpty()) {
            return Collections.emptyMap();
        }
        QueryReq queryReq = QueryReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .ids(new ArrayList<>(keySet))
                .build();

        QueryResp queryResp = withClient(client -> client.query(queryReq));
        List<Object[]> rows = parseQueryResp(queryResp);
        Map<Object, List<Object[]>> rowsMap = new HashMap<>();
        for (Object[] row : rows) {
            rowsMap.computeIfAbsent(row[milvusConfig.primaryKeyIndex], k -> new ArrayList<>()).add(row);
        }
        return rowsMap;
    }

    public List<VectorSearchResult> searchByEmbedding(VectorSearchRequest request) {
        List<String> fieldNames = milvusConfig.fieldSchemas.stream()
                .map(FieldSchema::getName)
                .collect(Collectors.toList());
        String filterExpression = MilvusFilterBuilder.buildJoinFilter(
                request.getFilterCondition(),
                request.getLeftRow(),
                fieldNames
        );

        SearchReq.SearchReqBuilder<?, ?> builder = SearchReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .annsField(request.getVectorField())
                .data(Collections.singletonList(new FloatVec(request.getEmbedding())))
                .outputFields(fieldNames)
                .topK(request.getTopK());
        if (filterExpression != null && !filterExpression.isEmpty()) {
            builder.filter(filterExpression);
        }

        SearchResp searchResp = withClient(client -> client.search(builder.build()));
        return parseSearchResults(searchResp);
    }

    private List<VectorSearchResult> parseSearchResults(SearchResp searchResp) {
        if (searchResp == null || searchResp.getSearchResults() == null) {
            return Collections.emptyList();
        }

        List<VectorSearchResult> rows = new ArrayList<>();
        for (List<SearchResp.SearchResult> results : searchResp.getSearchResults()) {
            for (SearchResp.SearchResult result : results) {
                Map<String, Object> entity = result.getEntity();
                rows.add(new VectorSearchResult(
                        toRow(entity, milvusConfig.fieldSchemas),
                        result.getScore()));
            }
        }
        return rows;
    }

    public boolean add(Object[] objects) {
        return addBatch(Collections.singletonList(objects));
    }

    public boolean addBatch(List<Object[]> records) {
        if (records == null || records.isEmpty()) {
            return true;
        }

        List<JsonObject> data = records.stream()
                .map(this::toJsonObject)
                .collect(Collectors.toList());

        UpsertReq upsertReq = UpsertReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .data(data)
                .build();

        withClient(client -> {
            client.upsert(upsertReq);
            return null;
        });
        return true;
    }

    public boolean remove(Object[] objects) {
        return removeBatch(Collections.singletonList(objects));
    }

    public boolean removeBatch(List<Object[]> records) {
        if (records == null || records.isEmpty()) {
            return true;
        }

        List<Object> ids = records.stream()
                .map(row -> row[milvusConfig.primaryKeyIndex])
                .collect(Collectors.toList());

        DeleteReq deleteReq = DeleteReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .ids(ids)
                .build();

        withClient(client -> {
            client.delete(deleteReq);
            return null;
        });
        return true;
    }

    private JsonObject toJsonObject(Object[] objects) {
        JsonObject jsonObject = new JsonObject();
        for (int i = 0; i < milvusConfig.fieldSchemas.size(); i++) {
            jsonObject.add(milvusConfig.fieldSchemas.get(i).getName(), gson.toJsonTree(objects[i]));
        }
        return jsonObject;
    }

    private List<Object[]> parseQueryResp(QueryResp queryResp) {
        if (queryResp == null || queryResp.getQueryResults() == null) {
            return Collections.emptyList();
        }

        List<QueryResp.QueryResult> results = queryResp.getQueryResults();
        List<Object[]> rows = new ArrayList<>(results.size());
        for (QueryResp.QueryResult result : results) {
            Map<String, Object> entity = result.getEntity();
            Object[] row = toRow(entity, milvusConfig.fieldSchemas);
            rows.add(row);
        }
        return rows;
    }

    private Object[] toRow(Map<String, Object> entity, List<FieldSchema> fieldSchemas) {
        return RowTransformUtils.projectRow(fieldSchemas, field -> entity.get(field.getName()));
    }

    public static void returnClient(MilvusClientV2 client, MilvusConfig milvusConfig) {
        MilvusClientPoolUtils.returnClient(client, milvusConfig);
    }

    public static MilvusClientV2 getClient(MilvusConfig milvusConfig) {
        return MilvusClientPoolUtils.getClient(milvusConfig);
    }

    public static synchronized void invalidatePool(String key) {
        MilvusClientPoolUtils.invalidatePool(key);
    }

    public static synchronized void closeAllClientPools() {
        MilvusClientPoolUtils.closeAll();
    }
}
