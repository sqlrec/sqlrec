package com.sqlrec.flink;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.handler.MilvusHandler;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Integration test for the Milvus Flink connector.
 *
 * Tests SQL-based INSERT (sink) operations against a live Milvus instance.
 * Uses MiniCluster for real Flink job execution.
 *
 * Milvus connector is sink-only in Flink (no DynamicTableSource), so tests
 * focus on INSERT operations. Every test class run creates an isolated collection
 * so test rows can never pollute benchmark data.
 *
 * Requires a running Milvus at {@code http://<DEFAULT_TEST_IP>:30022}
 * Tagged "integration" to skip in regular builds.
 */
@Tag("integration")
class MilvusConnectorTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    private static final String MILVUS_URL =
            "http://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30022";
    private static final String MILVUS_TOKEN = "root:Milvus";
    private static final String MILVUS_DB = "default";
    private static final int EMBEDDING_DIM = 64;
    private static final String TEST_COLLECTION = "sqlrec_milvus_connector_test_"
            + UUID.randomUUID().toString().replace("-", "");

    private static MilvusClientV2 collectionClient;

    @BeforeAll
    static void createTestCollection() {
        collectionClient = new MilvusClientV2(ConnectConfig.builder()
                .uri(MILVUS_URL)
                .token(MILVUS_TOKEN)
                .dbName(MILVUS_DB)
                .build());

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(false)
                .fieldSchemaList(Arrays.asList(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .autoID(false)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("title")
                                .dataType(DataType.VarChar)
                                .maxLength(512)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("genres")
                                .dataType(DataType.Array)
                                .elementType(DataType.VarChar)
                                .maxCapacity(64)
                                .maxLength(256)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("embedding")
                                .dataType(DataType.FloatVector)
                                .dimension(EMBEDDING_DIM)
                                .build()))
                .build();
        IndexParam embeddingIndex = IndexParam.builder()
                .fieldName("embedding")
                .indexName("embedding")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .build();

        collectionClient.createCollection(CreateCollectionReq.builder()
                .databaseName(MILVUS_DB)
                .collectionName(TEST_COLLECTION)
                .collectionSchema(schema)
                .indexParams(Collections.singletonList(embeddingIndex))
                .build());
    }

    @AfterAll
    static void dropTestCollection() {
        if (collectionClient == null) {
            return;
        }
        try {
            collectionClient.dropCollection(DropCollectionReq.builder()
                    .databaseName(MILVUS_DB)
                    .collectionName(TEST_COLLECTION)
                    .build());
        } finally {
            collectionClient.close();
        }
    }

    private StreamTableEnvironment createTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(env);
    }

    private void createMilvusTable(StreamTableEnvironment tEnv, String tableName) {
        tEnv.executeSql(
                "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT,\n" +
                "  title STRING,\n" +
                "  genres ARRAY<STRING>,\n" +
                "  embedding ARRAY<DOUBLE>,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'milvus',\n" +
                "  'url' = '" + MILVUS_URL + "',\n" +
                "  'token' = '" + MILVUS_TOKEN + "',\n" +
                "  'database' = '" + MILVUS_DB + "',\n" +
                "  'collection' = '" + TEST_COLLECTION + "',\n" +
                "  'batch-size' = '100',\n" +
                "  'flush-interval' = '5'\n" +
                ")"
        );
    }

    /**
     * Generates a 64-dimensional embedding as a Flink SQL ARRAY literal.
     * Values are deterministic based on seed to produce valid embeddings.
     */
    private String generateEmbeddingLiteral(long seed) {
        StringBuilder sb = new StringBuilder("ARRAY[");
        for (int i = 0; i < EMBEDDING_DIM; i++) {
            if (i > 0) sb.append(", ");
            // Simple deterministic value in [0, 1)
            sb.append(String.format("%.4f", ((seed + i * 7) % 100) / 100.0));
        }
        sb.append("]");
        return sb.toString();
    }

    // ---- Sink: INSERT into Milvus ----

    @Test
    void testInsertIntoMilvus() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();
        createMilvusTable(tEnv, "milvus_items");

        long baseId = System.currentTimeMillis();

        TableResult insertResult = tEnv.executeSql(
                "INSERT INTO milvus_items VALUES\n" +
                "  (" + baseId + ", 'Movie A', ARRAY['action', 'comedy'], " + generateEmbeddingLiteral(baseId) + "),\n" +
                "  (" + (baseId + 1) + ", 'Movie B', ARRAY['drama'], " + generateEmbeddingLiteral(baseId + 1) + "),\n" +
                "  (" + (baseId + 2) + ", 'Movie C', ARRAY['action', 'horror'], " + generateEmbeddingLiteral(baseId + 2) + ")"
        );
        insertResult.await();
    }

    // ---- Sink: Batch INSERT ----

    @Test
    void testBatchInsert() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();
        createMilvusTable(tEnv, "milvus_batch");

        long baseId = System.currentTimeMillis();

        StringBuilder sb = new StringBuilder("INSERT INTO milvus_batch VALUES\n");
        for (int i = 0; i < 5; i++) {
            if (i > 0) sb.append(",\n");
            sb.append("  (").append(baseId + i)
              .append(", 'Movie ").append(i).append("', ARRAY['action'], ")
              .append(generateEmbeddingLiteral(baseId + i)).append(")");
        }
        TableResult insertResult = tEnv.executeSql(sb.toString());
        insertResult.await();
    }

    // ---- Sink: INSERT with single row ----

    @Test
    void testSingleInsert() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();
        createMilvusTable(tEnv, "milvus_single");

        long id = System.currentTimeMillis();

        TableResult insertResult = tEnv.executeSql(
            "INSERT INTO milvus_single VALUES\n" +
            "  (" + id + ", 'Solo Movie', ARRAY['sci-fi'], " + generateEmbeddingLiteral(id) + ")"
        );
        insertResult.await();
    }

    // ---- Helpers for read-back verification ----

    /**
     * Builds a MilvusHandler with the same config derivation as
     * MilvusDynamicTableFactory, to read back rows by primary key.
     */
    private MilvusHandler createReadBackHandler() {
        ResolvedSchema schema = new ResolvedSchema(
                Arrays.asList(
                        Column.physical("id", DataTypes.BIGINT()),
                        Column.physical("title", DataTypes.STRING()),
                        Column.physical("genres", DataTypes.ARRAY(DataTypes.STRING())),
                        Column.physical("embedding", DataTypes.ARRAY(DataTypes.DOUBLE()))),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

        MilvusConfig config = new MilvusConfig();
        config.url = MILVUS_URL;
        config.token = MILVUS_TOKEN;
        config.database = MILVUS_DB;
        config.collection = TEST_COLLECTION;
        config.fieldSchemas = FlinkSchemaUtils.getFieldSchemas(schema);
        config.primaryKey = FlinkSchemaUtils.getPrimaryKey(schema);
        config.primaryKeyIndex = HiveTableUtils.getTablePrimaryKeyIndex(
                config.fieldSchemas, config.primaryKey);
        return new MilvusHandler(config);
    }

    /**
     * Generates a 64-dimensional embedding as Double[], using the same formula as
     * {@link #generateEmbeddingLiteral(long)}.
     */
    private Double[] generateEmbeddingArray(long seed) {
        Double[] values = new Double[EMBEDDING_DIM];
        for (int i = 0; i < EMBEDDING_DIM; i++) {
            values[i] = ((seed + i * 7) % 100) / 100.0;
        }
        return values;
    }

    /**
     * Reads rows back by primary key, polling until the expected number of rows
     * is visible. Milvus's default (bounded) consistency makes freshly written
     * or deleted rows visible with a short delay.
     */
    private Map<Object, List<Object[]>> awaitRows(
            MilvusHandler handler, Set<Object> ids, int expectedSize) throws Exception {
        Map<Object, List<Object[]>> rows = handler.getByPrimaryKey(ids);
        for (int i = 0; i < 20 && rows.size() != expectedSize; i++) {
            Thread.sleep(500);
            rows = handler.getByPrimaryKey(ids);
        }
        return rows;
    }

    // ---- Sink: INSERT verified by reading rows back from Milvus ----

    @Test
    void testInsertAndVerifyReadBack() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();
        createMilvusTable(tEnv, "milvus_verify");

        long baseId = System.currentTimeMillis();

        tEnv.executeSql(
            "INSERT INTO milvus_verify VALUES\n" +
            "  (" + baseId + ", 'Verify A', ARRAY['action'], " + generateEmbeddingLiteral(baseId) + "),\n" +
            "  (" + (baseId + 1) + ", 'Verify B', ARRAY['drama'], " + generateEmbeddingLiteral(baseId + 1) + ")"
        ).await();

        MilvusHandler readHandler = createReadBackHandler();
        Map<Object, List<Object[]>> rows = awaitRows(
                readHandler, Set.of(baseId, baseId + 1), 2);

        assertEquals(2, rows.size(), "Both inserted rows must be found in Milvus");
        assertEquals("Verify A", String.valueOf(rows.get(baseId).get(0)[1]));
        assertEquals("Verify B", String.valueOf(rows.get(baseId + 1).get(0)[1]));
    }

    // ---- Sink: retract stream (DELETE RowKind) drives the delete buffer ----

    @Test
    void testDeleteViaRetractStream() throws Exception {
        // one shared env: fromChangelogStream requires the stream to come from
        // the same StreamExecutionEnvironment as the table environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        createMilvusTable(tEnv, "milvus_retract");

        long baseId = System.currentTimeMillis();

        // Changelog stream: 3 inserts followed by a retract delete of baseId+1.
        // This drives the sink's delete path (removeBatch -> delete) end to end.
        DataStream<Row> changelog = env.fromElements(
            Row.ofKind(RowKind.INSERT, baseId, "Del A", new String[]{"action"}, generateEmbeddingArray(baseId)),
            Row.ofKind(RowKind.INSERT, baseId + 1, "Del B", new String[]{"drama"}, generateEmbeddingArray(baseId + 1)),
            Row.ofKind(RowKind.INSERT, baseId + 2, "Del C", new String[]{"sci-fi"}, generateEmbeddingArray(baseId + 2)),
            Row.ofKind(RowKind.DELETE, baseId + 1, "Del B", new String[]{"drama"}, generateEmbeddingArray(baseId + 1))
        );
        tEnv.fromChangelogStream(changelog).executeInsert("milvus_retract").await();

        MilvusHandler readHandler = createReadBackHandler();
        Map<Object, List<Object[]>> rows = awaitRows(
                readHandler, Set.of(baseId, baseId + 1, baseId + 2), 2);

        assertEquals(2, rows.size(), "2 of 3 ids should remain after the retract delete");
        assertFalse(rows.containsKey(baseId + 1),
                "deleted id must no longer be in Milvus");
    }
}
