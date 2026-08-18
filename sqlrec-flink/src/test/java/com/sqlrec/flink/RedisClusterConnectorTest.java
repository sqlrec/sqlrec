package com.sqlrec.flink;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the Redis Flink connector in cluster mode.
 *
 * Tests SQL-based INSERT (sink) and lookup join SELECT (source) operations
 * against a live Redis cluster. Uses MiniCluster for real Flink job execution.
 *
 * Requires a running Redis cluster at {@code redis://<DEFAULT_TEST_IP>:30040/0}.
 * Deploy with: deploy/redis/deploy-cluster.sh
 *
 * Tagged "integration" to skip in regular builds.
 *
 * Note: DELETE FROM is not supported in Flink streaming mode, so tests use
 * unique IDs per run to avoid data conflicts and skip cleanup.
 */
@Tag("integration")
class RedisClusterConnectorTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    private static final String REDIS_CLUSTER_URL =
            "redis://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30040/0";

    private StreamTableEnvironment createTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(env);
    }

    private void createProbeView(StreamTableEnvironment tEnv, String viewName, String valuesSql) {
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW " + viewName + " AS\n" +
                "SELECT id, PROCTIME() AS proctime FROM (\n" +
                valuesSql + "\n" +
                ") AS t (id)"
        );
    }

    // ---- Sink: INSERT into Redis cluster (JSON mode) + Source: lookup join ----

    @Test
    void testInsertAndLookupJsonMode() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        tEnv.executeSql(
                "CREATE TABLE redis_cluster_users (\n" +
                "  user_id BIGINT,\n" +
                "  gender STRING,\n" +
                "  age BIGINT,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'url' = '" + REDIS_CLUSTER_URL + "',\n" +
                "  'redis-mode' = 'cluster',\n" +
                "  'data-structure' = 'json'\n" +
                ")"
        );

        long baseId = System.currentTimeMillis();

        TableResult insertResult = tEnv.executeSql(
                "INSERT INTO redis_cluster_users VALUES\n" +
                "  (" + baseId + ", 'F', 24),\n" +
                "  (" + (baseId + 1) + ", 'M', 32),\n" +
                "  (" + (baseId + 2) + ", 'F', 18)"
        );
        insertResult.await();

        createProbeView(tEnv, "source_cluster_users",
                "  VALUES (" + baseId + "), (" + (baseId + 1) + "), (" + (baseId + 2) + ")");

        TableResult result = tEnv.executeSql(
                "SELECT u.id, r.gender, r.age\n" +
                "FROM source_cluster_users AS u\n" +
                "JOIN redis_cluster_users FOR SYSTEM_TIME AS OF u.proctime AS r\n" +
                "ON u.id = r.user_id"
        );

        List<Row> rows = collectRows(result);
        assertEquals(3, rows.size(), "Should retrieve 3 users from Redis cluster");
    }

    // ---- Sink: INSERT into Redis cluster (list mode) + Source: lookup join ----

    @Test
    void testInsertAndLookupListMode() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        tEnv.executeSql(
                "CREATE TABLE redis_cluster_clicks (\n" +
                "  user_id BIGINT,\n" +
                "  movie_id BIGINT,\n" +
                "  bhv_time BIGINT,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'url' = '" + REDIS_CLUSTER_URL + "',\n" +
                "  'redis-mode' = 'cluster',\n" +
                "  'data-structure' = 'list',\n" +
                "  'ttl' = '3600'\n" +
                ")"
        );

        long userId = System.currentTimeMillis();

        TableResult insertResult = tEnv.executeSql(
                "INSERT INTO redis_cluster_clicks VALUES\n" +
                "  (" + userId + ", 2001, 1700000000),\n" +
                "  (" + userId + ", 2002, 1700000001),\n" +
                "  (" + userId + ", 2003, 1700000002)"
        );
        insertResult.await();

        createProbeView(tEnv, "query_cluster_src",
                "  VALUES (" + userId + ")");

        TableResult result = tEnv.executeSql(
                "SELECT r.user_id, r.movie_id\n" +
                "FROM query_cluster_src AS q\n" +
                "JOIN redis_cluster_clicks FOR SYSTEM_TIME AS OF q.proctime AS r\n" +
                "ON q.id = r.user_id"
        );

        List<Row> rows = collectRows(result);
        assertTrue(rows.size() >= 1, "Should retrieve at least one click from Redis cluster list");
    }

    // ---- Source: Lookup join with multiple fields ----

    @Test
    void testLookupJoinMultipleFields() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        tEnv.executeSql(
                "CREATE TABLE redis_cluster_profile (\n" +
                "  uid BIGINT,\n" +
                "  name STRING,\n" +
                "  score DOUBLE,\n" +
                "  PRIMARY KEY (uid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'url' = '" + REDIS_CLUSTER_URL + "',\n" +
                "  'redis-mode' = 'cluster',\n" +
                "  'data-structure' = 'json'\n" +
                ")"
        );

        long baseId = System.currentTimeMillis();

        tEnv.executeSql(
                "INSERT INTO redis_cluster_profile VALUES\n" +
                "  (" + baseId + ", 'Alice', 95.5),\n" +
                "  (" + (baseId + 1) + ", 'Bob', 87.0)"
        ).await();

        createProbeView(tEnv, "probe_cluster",
                "  VALUES (" + baseId + "), (" + (baseId + 1) + "), (" + (baseId + 999) + ")");

        TableResult result = tEnv.executeSql(
                "SELECT p.id, r.name, r.score\n" +
                "FROM probe_cluster AS p\n" +
                "JOIN redis_cluster_profile FOR SYSTEM_TIME AS OF p.proctime AS r\n" +
                "ON p.id = r.uid"
        );

        List<Row> rows = collectRows(result);
        assertEquals(2, rows.size(), "Should find 2 matching rows (excluding non-existent)");
    }

    // ---- Sink: retract stream (DELETE RowKind) drives the delete buffer ----

    @Test
    void testDeleteViaRetractStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
            "CREATE TABLE redis_cluster_retract (\n" +
            "  user_id BIGINT,\n" +
            "  gender STRING,\n" +
            "  age BIGINT,\n" +
            "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'redis',\n" +
            "  'url' = '" + REDIS_CLUSTER_URL + "',\n" +
            "  'redis-mode' = 'cluster',\n" +
            "  'data-structure' = 'json',\n" +
            "  'batch-size' = '10',\n" +
            "  'flush-interval' = '1'\n" +
            ")"
        );

        long baseId = System.currentTimeMillis();

        DataStream<Row> changelog = env.fromElements(
            Row.ofKind(RowKind.INSERT, baseId, "F", 24L),
            Row.ofKind(RowKind.INSERT, baseId + 1, "M", 32L),
            Row.ofKind(RowKind.INSERT, baseId + 2, "F", 18L),
            Row.ofKind(RowKind.DELETE, baseId + 1, "M", 32L)
        );
        tEnv.fromChangelogStream(changelog).executeInsert("redis_cluster_retract").await();

        createProbeView(tEnv, "probe_cluster_retract",
            "  VALUES (" + baseId + "), (" + (baseId + 1) + "), (" + (baseId + 2) + ")");
        TableResult result = tEnv.executeSql(
            "SELECT u.id\n" +
            "FROM probe_cluster_retract AS u\n" +
            "JOIN redis_cluster_retract FOR SYSTEM_TIME AS OF u.proctime AS r\n" +
            "ON u.id = r.user_id"
        );

        List<Long> foundIds = collectRows(result).stream()
            .map(r -> (Long) r.getField(0))
            .collect(Collectors.toList());
        assertEquals(2, foundIds.size(), "2 of 3 ids should remain after the retract delete");
        assertFalse(foundIds.contains(baseId + 1), "deleted id must no longer be in Redis cluster");
    }

    // ---- Sink + Source: string codec mode ----

    @Test
    void testInsertAndLookupStringMode() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        tEnv.executeSql(
            "CREATE TABLE redis_cluster_str (\n" +
            "  user_id BIGINT,\n" +
            "  name STRING,\n" +
            "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'redis',\n" +
            "  'url' = '" + REDIS_CLUSTER_URL + "',\n" +
            "  'redis-mode' = 'cluster',\n" +
            "  'data-structure' = 'string',\n" +
            "  'batch-size' = '2',\n" +
            "  'flush-interval' = '1'\n" +
            ")"
        );

        long baseId = System.currentTimeMillis();

        tEnv.executeSql(
            "INSERT INTO redis_cluster_str VALUES\n" +
            "  (" + baseId + ", 'Alice'),\n" +
            "  (" + (baseId + 1) + ", 'Bob')"
        ).await();

        createProbeView(tEnv, "probe_cluster_str",
            "  VALUES (" + baseId + "), (" + (baseId + 1) + ")");
        TableResult result = tEnv.executeSql(
            "SELECT u.id, r.name\n" +
            "FROM probe_cluster_str AS u\n" +
            "JOIN redis_cluster_str FOR SYSTEM_TIME AS OF u.proctime AS r\n" +
            "ON u.id = r.user_id"
        );

        List<Row> rows = collectRows(result);
        assertEquals(2, rows.size(), "StringCodec round trip should return both rows from Redis cluster");
    }

    // ---- Helpers ----

    private static List<Row> collectRows(TableResult result) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }
}
