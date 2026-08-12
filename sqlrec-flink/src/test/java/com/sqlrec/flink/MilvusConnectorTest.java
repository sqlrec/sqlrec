package com.sqlrec.flink;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Integration test for the Milvus Flink connector.
 *
 * Tests SQL-based INSERT (sink) operations against a live Milvus instance.
 * Uses MiniCluster for real Flink job execution.
 *
 * Milvus connector is sink-only in Flink (no DynamicTableSource), so tests
 * focus on INSERT operations. The test uses unique IDs per run to avoid
 * data conflicts.
 *
 * Requires a running Milvus at {@code http://<DEFAULT_TEST_IP>:31530}
 * with a collection named {@code item_embedding} containing fields:
 * id (BIGINT), title (VARCHAR), genres (ARRAY<STRING>), embedding (ARRAY<DOUBLE>)
 * with embedding dimension = 64.
 * Tagged "integration" to skip in regular builds.
 */
@Tag("integration")
class MilvusConnectorTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    private static final String MILVUS_URL =
            "http://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":31530";
    private static final String MILVUS_TOKEN = "root:Milvus";
    private static final String MILVUS_DB = "default";
    private static final int EMBEDDING_DIM = 64;

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
                "  'collection' = 'item_embedding',\n" +
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
}
