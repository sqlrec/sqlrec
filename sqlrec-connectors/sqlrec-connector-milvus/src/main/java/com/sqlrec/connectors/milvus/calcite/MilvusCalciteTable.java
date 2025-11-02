package com.sqlrec.connectors.milvus.calcite;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.ToNumberPolicy;
import com.sqlrec.common.schema.SqlRecVectorTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.FieldSchema;
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
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MilvusCalciteTable extends SqlRecVectorTable {
    private MilvusConfig milvusConfig;
    private static Map<String, MilvusClientV2Pool> clientPools = new ConcurrentHashMap<>();

    public MilvusCalciteTable(MilvusConfig milvusConfig) {
        this.milvusConfig = milvusConfig;
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root, List<RexNode> filters) {
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
        return Linq4j.asEnumerable(rows);
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

    @Override
    public List<Object[]> searchByEmbedding(String fieldName, List<Float> embedding, int limit, List<Integer> projectColumns) {
        if (limit == 0){
            limit = 100;
        }
        List<String> outputFields = null;
        if (projectColumns != null && !projectColumns.isEmpty()) {
            outputFields = projectColumns.stream()
                    .map(index -> milvusConfig.fieldSchemas.get(index).name)
                    .collect(Collectors.toList());
        } else {
            outputFields = milvusConfig.fieldSchemas.stream()
                    .map(f -> f.name)
                    .collect(Collectors.toList());
        }

        FloatVec queryVector = new FloatVec(embedding);
        SearchReq searchReq = SearchReq.builder()
                .collectionName(milvusConfig.collection)
                .databaseName(milvusConfig.database)
                .annsField(fieldName)
                .data(Collections.singletonList(queryVector))
                .outputFields(outputFields)
                .topK(limit)
                .build();
        MilvusClientV2 client = getClient(milvusConfig);
        SearchResp searchResp = null;
        try {
            searchResp = client.search(searchReq);
        } finally {
            returnClient(client, milvusConfig);
        }

        List<Object[]> rows = parseSearchResp(searchResp);
        return rows;
    }

    private List<Object[]> parseSearchResp(SearchResp searchResp) {
        if (searchResp == null || searchResp.getSearchResults() == null) {
            return Collections.emptyList();
        }

        List<Object[]> rows = new ArrayList<>();
        for (List<SearchResp.SearchResult> results : searchResp.getSearchResults()) {
            for (SearchResp.SearchResult result : results) {
                Map<String, Object> entity = result.getEntity();
                Object[] row = toRow(entity, milvusConfig.fieldSchemas);
                rows.add(row);
            }
        }
        return rows;
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
            FieldSchema fieldSchema = fieldSchemas.get(i);
            row[i] = entity.get(fieldSchema.name);
        }
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
                .maxIdlePerKey(10) // max idle clients per key
                .maxTotalPerKey(100) // max total(idle + active) clients per key
                .maxTotal(100) // max total clients for all keys
                .maxBlockWaitDuration(Duration.ofSeconds(5L)) // getClient() will wait 5 seconds if no idle client available
                .minEvictableIdleDuration(Duration.ofSeconds(10L)) // if number of idle clients is larger than maxIdlePerKey, redundant idle clients will be evicted after 10 seconds
                .build();

        try {
            MilvusClientV2Pool pool = new MilvusClientV2Pool(poolConfig, connectConfig);
            clientPools.put(key, pool);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public @Nullable Collection getModifiableCollection() {
        return new MilvusCollection(milvusConfig);
    }

    @Override
    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, @Nullable List<String> updateColumnList, @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
        return LogicalTableModify.create(table, catalogReader, child, operation,
                updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, getElementType(),
                tableName, clazz);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, milvusConfig.fieldSchemas);
    }

    @Override
    public int getPrimaryKeyIndex() {
        return milvusConfig.primaryKeyIndex;
    }

    @Override
    public boolean onlyFilterByPrimaryKey() {
        return false;
    }

    public static class MilvusCollection implements Collection<Object[]> {
        private int size = 0;
        private MilvusConfig milvusConfig;
        private Gson gson;

        public MilvusCollection(MilvusConfig milvusConfig) {
            this.milvusConfig = milvusConfig;
            gson = new GsonBuilder()
                    .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
                    .create();
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Object[]> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean add(Object[] objects) {
            size += 1;
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

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Object[])) {
                throw new RuntimeException("Milvus Collection only support Object[]");
            }
            size += 1;

            Object[] objects = (Object[]) o;
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

        @Override
        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends Object[]> c) {
            for (Object[] objects : c) {
                add(objects);
            }
            return true;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            for (Object o : c) {
                remove(o);
            }
            return true;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }
    }
}
