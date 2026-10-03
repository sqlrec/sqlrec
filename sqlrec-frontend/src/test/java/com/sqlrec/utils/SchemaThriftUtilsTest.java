package com.sqlrec.utils;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaThriftUtilsTest {

    @Test
    public void testThriftValuesPreserveNullBitmapAcrossBytes() {
        List<Object[]> rows = List.of(
                new Object[]{null}, new Object[]{1L}, new Object[]{2}, new Object[]{3},
                new Object[]{4}, new Object[]{5}, new Object[]{6}, new Object[]{null},
                new Object[]{null}, new Object[]{9});

        var result = ThriftUtils.getValueList(Linq4j.asEnumerable(rows), 0, Integer.class);

        assertArrayEquals(new byte[]{(byte) 0x81, 0x01}, result.getKey());
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 0, 0, 9), result.getValue());
        assertNull(rows.get(0)[0]);
        assertEquals(1L, rows.get(1)[0]);
    }

    @Test
    public void testThriftValuesForMissingInputAndFailedCast() {
        var empty = ThriftUtils.getValueList(null, 0, String.class);
        assertArrayEquals(new byte[0], empty.getKey());
        assertTrue(empty.getValue().isEmpty());

        var result = ThriftUtils.getValueList(
                Linq4j.asEnumerable(List.of(new Object[]{"value"}, new Object[]{42})),
                0, String.class);
        assertArrayEquals(new byte[]{2}, result.getKey());
        assertEquals(List.of("value", ""), result.getValue());
    }

    @Test
    public void testParseCreateTableToHmsTable() throws Exception {
        String sql = "CREATE TABLE mytable (id INT, name VARCHAR) WITH ('connector' = 'redis', 'redis.uri' = 'localhost:6379')";
        SqlNode sqlNode = CompileManager.parseSql(sql);
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlCreateTable);

        Table hmsTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) sqlNode);
        assertNotNull(hmsTable);
        assertEquals("default", hmsTable.getDbName());
        assertEquals("mytable", hmsTable.getTableName());
        assertEquals(2, hmsTable.getSd().getCols().size());
        assertEquals("id", hmsTable.getSd().getCols().get(0).getName());
        assertEquals("INTEGER", hmsTable.getSd().getCols().get(0).getType());
        assertEquals("name", hmsTable.getSd().getCols().get(1).getName());
        assertEquals("VARCHAR", hmsTable.getSd().getCols().get(1).getType());
        assertEquals("redis", hmsTable.getParameters().get("flink.connector"));
        assertEquals("localhost:6379", hmsTable.getParameters().get("flink.redis.uri"));
    }

    @Test
    public void testParseCreateTableWithDatabase() throws Exception {
        String sql = "CREATE TABLE mydb.mytable (id BIGINT, score DOUBLE) WITH ('connector' = 'milvus')";
        SqlNode sqlNode = CompileManager.parseSql(sql);
        assertNotNull(sqlNode);

        Table hmsTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) sqlNode);
        assertNotNull(hmsTable);
        assertEquals("mydb", hmsTable.getDbName());
        assertEquals("mytable", hmsTable.getTableName());
        assertEquals(2, hmsTable.getSd().getCols().size());
    }

    @Test
    public void testParseCreateTableWithPrimaryKey() throws Exception {
        String sql = "CREATE TABLE mytable (id INT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'redis')";
        SqlNode sqlNode = CompileManager.parseSql(sql);
        assertNotNull(sqlNode);

        Table hmsTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) sqlNode);
        assertNotNull(hmsTable);
        assertEquals("id", hmsTable.getParameters().get("flink.schema.primary-key.columns"));
    }

    @Test
    public void testGenerateCreateSqlFromHmsTable() throws Exception {
        String sql = "CREATE TABLE mytable (id INT, name VARCHAR) WITH ('connector' = 'redis', 'redis.uri' = 'localhost:6379')";
        SqlNode sqlNode = CompileManager.parseSql(sql);
        Table hmsTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) sqlNode);

        String generatedDdl = SchemaUtils.generateCreateSqlFromHmsTable(hmsTable);
        assertNotNull(generatedDdl);
        assertTrue(generatedDdl.startsWith("CREATE TABLE mytable ("));
        assertTrue(generatedDdl.contains("id INTEGER"));
        assertTrue(generatedDdl.contains("name VARCHAR"));
        assertTrue(generatedDdl.contains("'connector' = 'redis'"));
        assertTrue(generatedDdl.contains("'redis.uri' = 'localhost:6379'"));
    }

    @Test
    public void testGenerateCreateSqlFromHmsTableWithPrimaryKey() throws Exception {
        String sql = "CREATE TABLE mytable (id INT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'redis')";
        SqlNode sqlNode = CompileManager.parseSql(sql);
        Table hmsTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) sqlNode);

        String generatedDdl = SchemaUtils.generateCreateSqlFromHmsTable(hmsTable);
        assertTrue(generatedDdl.contains("PRIMARY KEY (id)"));
        assertTrue(generatedDdl.contains("'connector' = 'redis'"));
    }

    @Test
    public void testFullRoundTrip() throws Exception {
        String sql = "CREATE TABLE mytable (id INT, name VARCHAR) WITH ('connector' = 'redis', 'redis.uri' = 'localhost:6379')";
        SqlNode sqlNode = CompileManager.parseSql(sql);
        Table hmsTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) sqlNode);
        String generatedDdl = SchemaUtils.generateCreateSqlFromHmsTable(hmsTable);

        // Parse the generated DDL again and verify it produces the same structure
        SqlNode roundTripNode = CompileManager.parseSql(generatedDdl);
        Table roundTripTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) roundTripNode);

        assertEquals(hmsTable.getDbName(), roundTripTable.getDbName());
        assertEquals(hmsTable.getTableName(), roundTripTable.getTableName());
        assertEquals(hmsTable.getSd().getCols().size(), roundTripTable.getSd().getCols().size());
        for (int i = 0; i < hmsTable.getSd().getCols().size(); i++) {
            assertEquals(hmsTable.getSd().getCols().get(i).getName(), roundTripTable.getSd().getCols().get(i).getName());
            assertEquals(hmsTable.getSd().getCols().get(i).getType(), roundTripTable.getSd().getCols().get(i).getType());
        }
        assertEquals(hmsTable.getParameters(), roundTripTable.getParameters());
    }

    @Test
    public void testFullRoundTripWithPrimaryKey() throws Exception {
        String sql = "CREATE TABLE mytable (id INT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'redis', 'redis.uri' = 'localhost:6379')";
        SqlNode sqlNode = CompileManager.parseSql(sql);
        Table hmsTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) sqlNode);
        String generatedDdl = SchemaUtils.generateCreateSqlFromHmsTable(hmsTable);

        // Parse the generated DDL again and verify round-trip
        SqlNode roundTripNode = CompileManager.parseSql(generatedDdl);
        Table roundTripTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) roundTripNode);

        assertEquals(hmsTable.getDbName(), roundTripTable.getDbName());
        assertEquals(hmsTable.getTableName(), roundTripTable.getTableName());
        assertEquals(hmsTable.getSd().getCols(), roundTripTable.getSd().getCols());
        assertEquals(hmsTable.getParameters(), roundTripTable.getParameters());
    }

    @Test
    public void testRoundTripDdlStringEquality() throws Exception {
        String sql = "CREATE TABLE mytable (id INT, name VARCHAR) WITH ('connector' = 'redis', 'redis.uri' = 'localhost:6379')";
        SqlNode sqlNode = CompileManager.parseSql(sql);
        Table hmsTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) sqlNode);
        String generatedDdl = SchemaUtils.generateCreateSqlFromHmsTable(hmsTable);

        // Generate DDL from the round-tripped HMS table should produce the same DDL
        SqlNode roundTripNode = CompileManager.parseSql(generatedDdl);
        Table roundTripTable = SchemaUtils.parseCreateTableToHmsTable((SqlCreateTable) roundTripNode);
        String roundTripDdl = SchemaUtils.generateCreateSqlFromHmsTable(roundTripTable);

        assertEquals(generatedDdl, roundTripDdl);
    }
}
