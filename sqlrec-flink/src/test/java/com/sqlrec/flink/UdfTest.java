package com.sqlrec.flink;

import com.sqlrec.udf.config.FunctionConfigs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that sqlrec Hive UDFs (IpFunction, L2NormFunction, RandomVecFunction,
 * UuidFunction) work correctly in a Flink SQL environment.
 *
 * Uses HiveCatalog + HiveModule to register Hive UDFs by class name via
 * CREATE FUNCTION. Flink's Hive integration handles type conversion via
 * Hive ObjectInspector.
 *
 * IpFunction and L2NormFunction use PrimitiveObjectInspector.getPrimitiveJavaObject()
 * to convert Writable elements (e.g. DoubleWritable) to Java types (Double)
 * before passing to DataTransformUtils.
 *
 * Uses Flink MiniCluster via {@link MiniClusterExtension} to run real Flink
 * jobs with SQL queries that exercise the UDFs.
 */
@Tag("integration")
class UdfTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    private static final String HIVE_VERSION = "3.1.3";

    // ---- Environment setup ----

    /**
     * Creates a StreamTableEnvironment with a HiveCatalog backed by an
     * embedded Derby metastore, and loads the HiveModule so that
     * Hive UDFs (GenericUDF/UDF) can be registered by class name.
     */
    private StreamTableEnvironment createTableEnvWithHive() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Programmatic HiveConf with embedded Derby metastore
        Path warehouseDir = Files.createTempDirectory("hive-warehouse");
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "");
        hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
                "jdbc:derby:memory:metastore_db;create=true");
        hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE,
                warehouseDir.toAbsolutePath().toString());
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, false);
        // Disable connection pooling for embedded Derby (avoids HikariCP/DBCP dependency)
        hiveConf.set("datanucleus.connectionPoolingType", "None");
        // Auto-create metastore schema (avoids running schematool)
        hiveConf.setBoolean("hive.metastore.schema.verification", false);
        hiveConf.setBoolean("datanucleus.schema.autoCreateAll", true);
        hiveConf.setBoolean("hive.metastore.try.direct.sql", true);

        // Register HiveCatalog (allowEmbedded=true for in-memory Derby metastore)
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", hiveConf, HIVE_VERSION, true);
        tEnv.registerCatalog("hive", hiveCatalog);
        tEnv.useCatalog("hive");

        // Load HiveModule so Flink can recognize Hive UDFs
        tEnv.loadModule("hive", new HiveModule(HIVE_VERSION));

        return tEnv;
    }

    /**
     * Registers a Hive UDF by class name via CREATE FUNCTION IF NOT EXISTS.
     */
    private void registerFunction(StreamTableEnvironment tEnv, String funcName, String className) {
        tEnv.executeSql(
                String.format("CREATE FUNCTION IF NOT EXISTS %s AS '%s'", funcName, className));
    }

    // ---- IpFunction tests ----

    @Test
    void testIpBasicInnerProduct() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "ip", "com.sqlrec.udf.scalar.IpFunction");

        // 1*4 + 2*5 + 3*6 = 32.0
        TableResult result = tEnv.executeSql(
                "SELECT ip(ARRAY[1.0, 2.0, 3.0], ARRAY[4.0, 5.0, 6.0])"
        );
        List<Double> values = collectDoubleColumn(result);
        assertEquals(1, values.size());
        assertEquals(32.0, values.get(0), 0.0001);
    }

    @Test
    void testIpOrthogonalVectors() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "ip", "com.sqlrec.udf.scalar.IpFunction");

        TableResult result = tEnv.executeSql(
                "SELECT ip(ARRAY[1.0, 0.0], ARRAY[0.0, 1.0])"
        );
        List<Double> values = collectDoubleColumn(result);
        assertEquals(1, values.size());
        assertEquals(0.0, values.get(0), 0.0001);
    }

    @Test
    void testIpWithTableSource() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "ip", "com.sqlrec.udf.scalar.IpFunction");

        TableResult result = tEnv.executeSql(
                "SELECT ip(emb1, emb2) AS score\n" +
                "FROM (VALUES \n" +
                "  (ARRAY[1.0, 2.0], ARRAY[3.0, 4.0]),\n" +
                "  (ARRAY[1.0, 0.0], ARRAY[0.0, 1.0])\n" +
                ") AS t (emb1, emb2)"
        );
        List<Double> values = collectDoubleColumn(result);
        assertEquals(2, values.size());
        // 1*3 + 2*4 = 11.0
        assertEquals(11.0, values.get(0), 0.0001);
        // 1*0 + 0*1 = 0.0
        assertEquals(0.0, values.get(1), 0.0001);
    }

    // ---- L2NormFunction tests ----

    @Test
    void testL2NormBasic() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "l2_norm", "com.sqlrec.udf.scalar.L2NormFunction");

        // [3, 4] -> L2 norm = 5 -> [0.6, 0.8]
        TableResult result = tEnv.executeSql(
                "SELECT l2_norm(ARRAY[3.0, 4.0])"
        );
        List<Double[]> values = collectDoubleArrayColumn(result);
        assertEquals(1, values.size());
        assertEquals(0.6, values.get(0)[0], 0.0001);
        assertEquals(0.8, values.get(0)[1], 0.0001);
    }

    @Test
    void testL2NormUnitVector() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "l2_norm", "com.sqlrec.udf.scalar.L2NormFunction");

        // Already normalized vector should remain the same
        TableResult result = tEnv.executeSql(
                "SELECT l2_norm(ARRAY[0.6, 0.8])"
        );
        List<Double[]> values = collectDoubleArrayColumn(result);
        assertEquals(1, values.size());
        assertEquals(0.6, values.get(0)[0], 0.0001);
        assertEquals(0.8, values.get(0)[1], 0.0001);
    }

    @Test
    void testL2NormZeroVector() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "l2_norm", "com.sqlrec.udf.scalar.L2NormFunction");

        // Zero vector: L2 norm = 0, should return [0.0, 0.0]
        TableResult result = tEnv.executeSql(
                "SELECT l2_norm(ARRAY[0.0, 0.0])"
        );
        List<Double[]> values = collectDoubleArrayColumn(result);
        assertEquals(1, values.size());
        assertEquals(0.0, values.get(0)[0], 0.0001);
        assertEquals(0.0, values.get(0)[1], 0.0001);
    }

    @Test
    void testRandomVecDimension() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "random_vec", "com.sqlrec.udf.scalar.RandomVecFunction");

        TableResult result = tEnv.executeSql(
                "SELECT random_vec('5')"
        );
        List<Double[]> values = collectDoubleArrayColumn(result);
        assertEquals(1, values.size());
        assertEquals(5, values.get(0).length, "Vector should have 5 dimensions");
    }

    @Test
    void testRandomVecIsNormalized() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "random_vec", "com.sqlrec.udf.scalar.RandomVecFunction");

        TableResult result = tEnv.executeSql(
                "SELECT random_vec('100')"
        );
        List<Double[]> values = collectDoubleArrayColumn(result);
        assertEquals(1, values.size());

        // Verify L2 norm is 1 (the vector should be normalized)
        double sumOfSquares = 0.0;
        for (double v : values.get(0)) {
            sumOfSquares += v * v;
        }
        assertEquals(1.0, sumOfSquares, 0.001,
                "Random vector should be L2-normalized (sum of squares = 1)");
    }

    @Test
    void testRandomVecDifferentDimensions() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "random_vec", "com.sqlrec.udf.scalar.RandomVecFunction");

        // Use different dimension args to get different results
        TableResult result = tEnv.executeSql(
                "SELECT random_vec('3') AS v1, random_vec('4') AS v2\n" +
                "FROM (VALUES (1)) AS t (x)"
        );
        List<Row> rows = collectRows(result);
        assertEquals(1, rows.size());
        Double[] v1 = (Double[]) rows.get(0).getField(0);
        Double[] v2 = (Double[]) rows.get(0).getField(1);
        assertEquals(3, v1.length, "First vector should have 3 dimensions");
        assertEquals(4, v2.length, "Second vector should have 4 dimensions");
    }

    @Test
    void testRandomVecNullInput() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "random_vec", "com.sqlrec.udf.scalar.RandomVecFunction");

        TableResult result = tEnv.executeSql(
                "SELECT random_vec(CAST(NULL AS STRING))"
        );
        List<Double[]> values = collectDoubleArrayColumn(result);
        assertEquals(1, values.size());
        assertNull(values.get(0));
    }

    // ---- UuidFunction tests ----

    @Test
    void testUuidFormat() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "uuid", "com.sqlrec.udf.scalar.UuidFunction");

        TableResult result = tEnv.executeSql("SELECT uuid()");
        List<String> values = collectStringColumn(result);
        assertEquals(1, values.size());
        // Verify it's a valid UUID (36 chars with dashes)
        String uuid = values.get(0);
        assertEquals(36, uuid.length(), "UUID string should be 36 characters");
        assertDoesNotThrow(() -> UUID.fromString(uuid), "Should be a valid UUID");
    }

    @Test
    void testUuidUnique() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "uuid", "com.sqlrec.udf.scalar.UuidFunction");

        // Generate multiple UUIDs and verify uniqueness
        TableResult result = tEnv.executeSql(
                "SELECT uuid() AS id FROM (VALUES (1), (2), (3), (4), (5)) AS t (x)"
        );
        List<String> values = collectStringColumn(result);
        assertEquals(5, values.size());
        assertEquals(5, values.stream().distinct().count(), "All UUIDs should be unique");
    }

    @Test
    void testUuidInComplexQuery() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "uuid", "com.sqlrec.udf.scalar.UuidFunction");

        TableResult result = tEnv.executeSql(
                "SELECT uuid() AS id, name\n" +
                "FROM (VALUES ('Alice'), ('Bob')) AS t (name)"
        );
        List<Row> rows = collectRows(result);
        assertEquals(2, rows.size());
        assertNotNull(rows.get(0).getField(0));
        assertNotNull(rows.get(1).getField(0));
        assertNotEquals(rows.get(0).getField(0), rows.get(1).getField(0));
    }

    // ---- Combined UDF test ----

    @Test
    void testMultipleUdfsInSingleQuery() throws Exception {
        StreamTableEnvironment tEnv = createTableEnvWithHive();
        registerFunction(tEnv, "ip", "com.sqlrec.udf.scalar.IpFunction");
        registerFunction(tEnv, "l2_norm", "com.sqlrec.udf.scalar.L2NormFunction");
        registerFunction(tEnv, "uuid", "com.sqlrec.udf.scalar.UuidFunction");

        // Use multiple UDFs in one query:
        // 1. Normalize a vector with l2_norm
        // 2. Compute inner product of two normalized vectors
        // 3. Generate a UUID for each row
        TableResult result = tEnv.executeSql(
                "SELECT uuid() AS id,\n" +
                "       ip(l2_norm(emb1), l2_norm(emb2)) AS similarity\n" +
                "FROM (VALUES \n" +
                "  (ARRAY[3.0, 4.0], ARRAY[3.0, 4.0]),\n" +
                "  (ARRAY[1.0, 0.0], ARRAY[0.0, 1.0])\n" +
                ") AS t (emb1, emb2)"
        );
        List<Row> rows = collectRows(result);
        assertEquals(2, rows.size());

        // Row 1: same direction -> similarity = 1.0
        Double sim1 = (Double) rows.get(0).getField(1);
        assertEquals(1.0, sim1, 0.001, "Same-direction vectors should have similarity 1.0");

        // Row 2: orthogonal -> similarity = 0.0
        Double sim2 = (Double) rows.get(1).getField(1);
        assertEquals(0.0, sim2, 0.001, "Orthogonal vectors should have similarity 0.0");
    }

    // ---- Helpers ----

    private static List<Double> collectDoubleColumn(TableResult result) throws Exception {
        List<Double> values = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                values.add((Double) row.getField(0));
            }
        }
        return values;
    }

    private static List<String> collectStringColumn(TableResult result) throws Exception {
        List<String> values = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                values.add((String) row.getField(0));
            }
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private static List<Double[]> collectDoubleArrayColumn(TableResult result) throws Exception {
        List<Double[]> values = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                values.add((Double[]) row.getField(0));
            }
        }
        return values;
    }

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
