package com.sqlrec.connectors.milvus.handler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.ToNumberPolicy;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.FilterUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
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
import org.apache.calcite.DataContext;
import org.apache.calcite.rex.RexNode;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MilvusHandler {
    private static Map<String, MilvusClientV2Pool> clientPools = new ConcurrentHashMap<>();
    private static Gson gson = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .create();

    private MilvusConfig milvusConfig;

    public MilvusHandler(MilvusConfig milvusConfig) {
        this.milvusConfig = milvusConfig;
    }

    public List<Object[]> scan(DataContext root, List<RexNode> filters) {
        String filterSql = FilterUtils.getMilvusFilterSqlString(filters, milvusConfig.fieldSchemas);
        QueryReq queryReq = QueryReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .filter(filterSql)
                .build();

        MilvusClientV2 client = getClient(milvusConfig);
        QueryResp queryResp = null;
        try {
            queryResp = client.query(queryReq);
        } finally {
            returnClient(client, milvusConfig);
        }

        List<Object[]> rows = parseQueryResp(queryResp);
        return rows;
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        QueryReq queryReq = QueryReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .ids(new ArrayList<>(keySet))
                .build();

        MilvusClientV2 client = getClient(milvusConfig);
        QueryResp queryResp = null;
        try {
            queryResp = client.query(queryReq);
        } finally {
            returnClient(client, milvusConfig);
        }

        List<Object[]> rows = parseQueryResp(queryResp);
        Map<Object, List<Object[]>> rowsMap = new HashMap<>();
        for (Object[] row : rows) {
            Object key = row[milvusConfig.primaryKeyIndex];
            rowsMap.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
        return rowsMap;
    }

    public List<Object[]> searchByEmbeddingWithScore(
            String fieldName,
            List<Float> embedding,
            String filterExpression,
            int limit,
            List<Integer> projectColumns) {
        if (limit == 0) {
            limit = 100;
        }

        List<String> outputFields = getOutputFields(projectColumns);

        FloatVec queryVector = new FloatVec(embedding);
        SearchReq.SearchReqBuilder<?, ?> builder = SearchReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .annsField(fieldName)
                .data(Collections.singletonList(queryVector))
                .outputFields(outputFields)
                .topK(limit);
        if (filterExpression != null && !filterExpression.isEmpty()) {
            builder.filter(filterExpression);
        }

        SearchReq searchReq = builder.build();
        MilvusClientV2 client = getClient(milvusConfig);
        SearchResp searchResp = null;
        try {
            searchResp = client.search(searchReq);
        } finally {
            returnClient(client, milvusConfig);
        }

        List<Object[]> rows = parseSearchRespWithScore(searchResp);
        return rows;
    }

    private List<String> getOutputFields(List<Integer> projectColumns) {
        if (projectColumns != null && !projectColumns.isEmpty()) {
            List<String> outputFields = new ArrayList<>();
            for (Integer projectColumn : projectColumns) {
                if (projectColumn >= 0 && projectColumn < milvusConfig.fieldSchemas.size()) {
                    outputFields.add(milvusConfig.fieldSchemas.get(projectColumn).name);
                }
            }
            return outputFields;
        } else {
            return milvusConfig.fieldSchemas.stream()
                    .map(f -> f.name)
                    .collect(Collectors.toList());
        }
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
        List<JsonObject> data = new ArrayList<>();
        JsonObject jsonObject = new JsonObject();
        for (int i = 0; i < milvusConfig.fieldSchemas.size(); i++) {
            FieldSchema fieldSchema = milvusConfig.fieldSchemas.get(i);
            jsonObject.add(fieldSchema.name, gson.toJsonTree(objects[i]));
        }
        data.add(jsonObject);

        UpsertReq upsertReq = UpsertReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .data(data)
                .build();

        MilvusClientV2 client = getClient(milvusConfig);
        try {
            client.upsert(upsertReq);
        } finally {
            returnClient(client, milvusConfig);
        }
        return true;
    }

    public boolean remove(Object[] objects) {
        DeleteReq deleteReq = DeleteReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .ids(Collections.singletonList(objects[milvusConfig.primaryKeyIndex]))
                .build();

        MilvusClientV2 client = getClient(milvusConfig);
        try {
            client.delete(deleteReq);
        } finally {
            returnClient(client, milvusConfig);
        }
        return true;
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
            row[i] = entity.get(fieldSchemas.get(i).name);
        }
        return row;
    }

    private Object[] toRowWithScore(Map<String, Object> entity, List<FieldSchema> fieldSchemas, float score) {
        Object[] row = new Object[fieldSchemas.size() + 1];
        for (int i = 0; i < fieldSchemas.size(); i++) {
            row[i] = entity.get(fieldSchemas.get(i).name);
        }
        row[fieldSchemas.size()] = score;
        return row;
    }

    public static void returnClient(MilvusClientV2 client, MilvusConfig milvusConfig) {
        String key = milvusConfig.url + milvusConfig.token;
        if (clientPools.containsKey(key)) {
            clientPools.get(key).returnClient(key, client);
        }
    }

    public static MilvusClientV2 getClient(MilvusConfig milvusConfig) {
        String key = milvusConfig.url + milvusConfig.token;
        if (!clientPools.containsKey(key)) {
            openClientPool(key, milvusConfig);
        }
        return clientPools.get(key).getClient(key);
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
                .maxIdlePerKey(10)
                .maxTotalPerKey(100)
                .maxTotal(100)
                .maxBlockWaitDuration(Duration.ofSeconds(5L))
                .minEvictableIdleDuration(Duration.ofSeconds(10L))
                .build();

        try {
            MilvusClientV2Pool pool = new MilvusClientV2Pool(poolConfig, connectConfig);
            clientPools.put(key, pool);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
