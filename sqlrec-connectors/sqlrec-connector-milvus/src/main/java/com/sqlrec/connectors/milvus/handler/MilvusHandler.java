package com.sqlrec.connectors.milvus.handler;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.FilterUtils;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.v2.client.ConnectConfig;
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

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MilvusHandler {
    private static final Logger logger = LoggerFactory.getLogger(MilvusHandler.class);
    private static Map<String, MilvusClientV2Pool> clientPools = new ConcurrentHashMap<>();
    private static Gson gson = JsonUtils.getGson();

    static {
        // Close every cached client pool on JVM exit so pooled MilvusClientV2 instances
        // (each holding a gRPC channel) are not leaked when the process terminates.
        Runtime.getRuntime().addShutdownHook(new Thread(MilvusHandler::closeAllClientPools, "MilvusHandler-shutdown"));
    }

    private MilvusConfig milvusConfig;

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
        String key = getClientPoolKey(milvusConfig);
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

    /**
     * Close and discard the cached client pool for {@code key} so the next call builds a
     * fresh pool. Called after a transport-level failure (Milvus restart, network blip)
     * so subsequent calls recover instead of reusing broken pooled clients.
     */
    public static synchronized void invalidatePool(String key) {
        MilvusClientV2Pool pool = clientPools.remove(key);
        if (pool != null) {
            try {
                pool.close();
            } catch (Exception e) {
                logger.warn("Failed to close Milvus client pool for {} during invalidation: {}", key, e.getMessage());
            }
        }
    }

    /**
     * Close every cached client pool and clear the cache. Registered as a JVM shutdown
     * hook so pooled gRPC channels are released on exit.
     */
    public static synchronized void closeAllClientPools() {
        for (Map.Entry<String, MilvusClientV2Pool> entry : new ArrayList<>(clientPools.entrySet())) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                logger.warn("Failed to close Milvus client pool for {} on shutdown: {}", entry.getKey(), e.getMessage());
            }
        }
        clientPools.clear();
        logger.info("Closed all Milvus client pools on shutdown");
    }

    public List<Object[]> scan(List<RexNode> filters) {
        String filterSql = FilterUtils.getMilvusFilterSqlString(filters, milvusConfig.fieldSchemas);
        QueryReq queryReq = QueryReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .filter(filterSql)
                .build();

        QueryResp queryResp = withClient(client -> client.query(queryReq));
        return parseQueryResp(queryResp);
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
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

    public List<Object[]> searchByEmbeddingWithScore(
            String fieldName,
            List<Float> embedding,
            RexNode filterCondition,
            Object[] leftValue,
            int limit,
            List<Integer> projectColumns) {
        int topK = limit == 0 ? 100 : limit;

        String filterExpression = FilterUtils.buildMilvusFilterExpression(
                filterCondition,
                leftValue,
                milvusConfig.fieldSchemas.stream().map(FieldSchema::getName).collect(Collectors.toList())
        );

        SearchReq.SearchReqBuilder<?, ?> builder = SearchReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .annsField(fieldName)
                .data(Collections.singletonList(new FloatVec(embedding)))
                .outputFields(getOutputFields(projectColumns))
                .topK(topK);
        if (filterExpression != null && !filterExpression.isEmpty()) {
            builder.filter(filterExpression);
        }

        SearchResp searchResp = withClient(client -> client.search(builder.build()));
        return parseSearchRespWithScore(searchResp);
    }

    private List<String> getOutputFields(List<Integer> projectColumns) {
        if (projectColumns == null || projectColumns.isEmpty()) {
            return milvusConfig.fieldSchemas.stream()
                    .map(FieldSchema::getName)
                    .collect(Collectors.toList());
        }
        return projectColumns.stream()
                .filter(i -> i >= 0 && i < milvusConfig.fieldSchemas.size())
                .map(i -> milvusConfig.fieldSchemas.get(i).getName())
                .collect(Collectors.toList());
    }

    private List<Object[]> parseSearchRespWithScore(SearchResp searchResp) {
        if (searchResp == null || searchResp.getSearchResults() == null) {
            return Collections.emptyList();
        }

        List<Object[]> rows = new ArrayList<>();
        for (List<SearchResp.SearchResult> results : searchResp.getSearchResults()) {
            for (SearchResp.SearchResult result : results) {
                Map<String, Object> entity = result.getEntity();
                float score = result.getScore();
                Object[] row = toRowWithScore(entity, milvusConfig.fieldSchemas, score);
                rows.add(row);
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
        Object[] row = new Object[fieldSchemas.size()];
        for (int i = 0; i < fieldSchemas.size(); i++) {
            row[i] = entity.get(fieldSchemas.get(i).getName());
        }
        return row;
    }

    private Object[] toRowWithScore(Map<String, Object> entity, List<FieldSchema> fieldSchemas, float score) {
        Object[] row = new Object[fieldSchemas.size() + 1];
        for (int i = 0; i < fieldSchemas.size(); i++) {
            row[i] = entity.get(fieldSchemas.get(i).getName());
        }
        row[fieldSchemas.size()] = score;
        return row;
    }

    public static void returnClient(MilvusClientV2 client, MilvusConfig milvusConfig) {
        String key = getClientPoolKey(milvusConfig);
        if (clientPools.containsKey(key)) {
            clientPools.get(key).returnClient(key, client);
        } else {
            logger.warn("client pool {} is not found", key);
            client.close();
        }
    }

    public static MilvusClientV2 getClient(MilvusConfig milvusConfig) {
        String key = getClientPoolKey(milvusConfig);
        if (!clientPools.containsKey(key)) {
            openClientPool(key, milvusConfig);
        }
        MilvusClientV2Pool pool = clientPools.get(key);
        if (pool == null) {
            // Pool was invalidated concurrently between containsKey and get (e.g. by a
            // transport-failure retry); reopen so this call does not NPE.
            openClientPool(key, milvusConfig);
            pool = clientPools.get(key);
        }
        return pool.getClient(key);
    }

    private static String getClientPoolKey(MilvusConfig milvusConfig) {
        return milvusConfig.url + "|" + milvusConfig.token;
    }

    private static synchronized void openClientPool(String key, MilvusConfig milvusConfig) {
        if (clientPools.containsKey(key)) {
            return;
        }

        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(milvusConfig.url)
                .token(milvusConfig.token)
                .build();
        PoolConfig poolConfig = PoolConfig.builder()
                .maxIdlePerKey(milvusConfig.poolMaxIdlePerKey)
                .maxTotalPerKey(milvusConfig.poolMaxTotalPerKey)
                .maxTotal(milvusConfig.poolMaxTotal)
                .maxBlockWaitDuration(Duration.ofSeconds(milvusConfig.poolMaxBlockWaitDuration))
                .minEvictableIdleDuration(Duration.ofSeconds(milvusConfig.poolMinEvictableIdleDuration))
                .build();

        try {
            MilvusClientV2Pool pool = new MilvusClientV2Pool(poolConfig, connectConfig);
            clientPools.put(key, pool);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
