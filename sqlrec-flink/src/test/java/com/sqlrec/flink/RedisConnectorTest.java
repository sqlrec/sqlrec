package com.sqlrec.flink;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the Redis Flink connector.
 *
 * Tests SQL-based INSERT (sink) and lookup join SELECT (source) operations
 * against a live Redis instance. Uses MiniCluster for real Flink job execution.
 *
 * Requires a running Redis at {@code redis://<DEFAULT_TEST_IP>:32379/0}.
 * Tagged "integration" to skip in regular builds.
 *
 * Note: DELETE FROM is not supported in Flink streaming mode, so tests use
 * unique IDs per run to avoid data conflicts and skip cleanup.
 */
@Tag("integration")
class RedisConnectorTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    private static final String REDIS_URL =
            "redis://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":32379/0";

    private StreamTableEnvironment createTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(env);
    }

    /**
     * Creates a temporary view with a proctime column for lookup join.
     * Lookup joins require a time attribute on the left (driving) side.
     */
    private void createProbeView(StreamTableEnvironment tEnv, String viewName, String valuesSql) {
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW " + viewName + " AS\n" +
                "SELECT id, PROCTIME() AS proctime FROM (\n" +
                valuesSql + "\n" +
                ") AS t (id)"
        );
    }

    // ---- Sink: INSERT into Redis (JSON mode) + Source: lookup join ----

    @Test
    void testInsertAndLookupJsonMode() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        tEnv.executeSql(
                "CREATE TABLE redis_users (\n" +
                "  user_id BIGINT,\n" +
                "  gender STRING,\n" +
                "  age BIGINT,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'url' = '" + REDIS_URL + "',\n" +
                "  'data-structure' = 'json'\n" +
                ")"
        );

        // Use unique IDs to avoid conflicts
        long baseId = System.currentTimeMillis();

        // Insert rows
        TableResult insertResult = tEnv.executeSql(
                "INSERT INTO redis_users VALUES\n" +
                "  (" + baseId + ", 'F', 24),\n" +
                "  (" + (baseId + 1) + ", 'M', 32),\n" +
                "  (" + (baseId + 2) + ", 'F', 18)"
        );
        insertResult.await();

        // Verify by reading back via lookup join
        createProbeView(tEnv, "source_users",
                "  VALUES (" + baseId + "), (" + (baseId + 1) + "), (" + (baseId + 2) + ")");

        TableResult result = tEnv.executeSql(
                "SELECT u.id, r.gender, r.age\n" +
                "FROM source_users AS u\n" +
                "JOIN redis_users FOR SYSTEM_TIME AS OF u.proctime AS r\n" +
                "ON u.id = r.user_id"
        );

        List<Row> rows = collectRows(result);
        assertEquals(3, rows.size(), "Should retrieve 3 users from Redis");
    }

    // ---- Sink: INSERT into Redis (list mode) + Source: lookup join ----

    @Test
    void testInsertAndLookupListMode() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        tEnv.executeSql(
                "CREATE TABLE redis_clicks (\n" +
                "  user_id BIGINT,\n" +
                "  movie_id BIGINT,\n" +
                "  bhv_time BIGINT,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'url' = '" + REDIS_URL + "',\n" +
                "  'data-structure' = 'list',\n" +
                "  'ttl' = '3600'\n" +
                ")"
        );

        long userId = System.currentTimeMillis();

        // Insert multiple items for the same user (list appends)
        TableResult insertResult = tEnv.executeSql(
                "INSERT INTO redis_clicks VALUES\n" +
                "  (" + userId + ", 2001, 1700000000),\n" +
                "  (" + userId + ", 2002, 1700000001),\n" +
                "  (" + userId + ", 2003, 1700000002)"
        );
        insertResult.await();

        // Verify via lookup join - should get entries from the list
        createProbeView(tEnv, "query_src",
                "  VALUES (" + userId + ")");

        TableResult result = tEnv.executeSql(
                "SELECT r.user_id, r.movie_id\n" +
                "FROM query_src AS q\n" +
                "JOIN redis_clicks FOR SYSTEM_TIME AS OF q.proctime AS r\n" +
                "ON q.id = r.user_id"
        );

        List<Row> rows = collectRows(result);
        assertTrue(rows.size() >= 1, "Should retrieve at least one click from Redis list");
    }

    // ---- Source: Lookup join with multiple fields ----

    @Test
    void testLookupJoinMultipleFields() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        tEnv.executeSql(
                "CREATE TABLE redis_profile (\n" +
                "  uid BIGINT,\n" +
                "  name STRING,\n" +
                "  score DOUBLE,\n" +
                "  PRIMARY KEY (uid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'url' = '" + REDIS_URL + "',\n" +
                "  'data-structure' = 'json'\n" +
                ")"
        );

        long baseId = System.currentTimeMillis();

        // Insert test data
        tEnv.executeSql(
                "INSERT INTO redis_profile VALUES\n" +
                "  (" + baseId + ", 'Alice', 95.5),\n" +
                "  (" + (baseId + 1) + ", 'Bob', 87.0)"
        ).await();

        // Lookup join: query existing and non-existing keys
        createProbeView(tEnv, "probe",
                "  VALUES (" + baseId + "), (" + (baseId + 1) + "), (" + (baseId + 999) + ")");

        TableResult result = tEnv.executeSql(
                "SELECT p.id, r.name, r.score\n" +
                "FROM probe AS p\n" +
                "JOIN redis_profile FOR SYSTEM_TIME AS OF p.proctime AS r\n" +
                "ON p.id = r.uid"
        );

        List<Row> rows = collectRows(result);
        assertEquals(2, rows.size(), "Should find 2 matching rows (excluding non-existent)");
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
