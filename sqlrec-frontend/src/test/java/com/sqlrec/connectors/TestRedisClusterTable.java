package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.connectors.redis.calcite.RedisCalciteTable;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;

/**
 * Integration test for the Redis Calcite connector in cluster mode.
 *
 * Requires a running Redis cluster accessible at
 * {@code redis://<DEFAULT_TEST_IP>:30018/0} (seed node).
 * Deploy with: deploy/redis/deploy-cluster.sh
 *
 * Tagged "integration" to skip in regular builds.
 */
@Tag("integration")
public class TestRedisClusterTable {

    private static final String REDIS_CLUSTER_URL =
            "redis://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30040/0";

    @Test
    public void testRedisClusterTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("ct1", getClusterRedisTable());
        tableMap.put("ct2", getClusterListRedisTable());
        tableMap.put("ct3", new MyTable());
        tableMap.put("ct4", new MyTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        new SqlTestCase("select * from ct1", null, new UnsupportedOperationException()).test(schema);

        new SqlTestCase("delete from ct1 where id = 1", null).test(schema);
        new SqlTestCase("insert into ct1 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("select * from ct1 where id = 1",
                Collections.singletonList(new Object[]{1, "Alice1", 1})).test(schema);
        new SqlTestCase("update ct1 set name = 'a' where id = 1", null).test(schema);
        new SqlTestCase("select * from ct1 where id = 1 and name = 'a'",
                Collections.singletonList(new Object[]{1, "a", 1})).test(schema);
        new SqlTestCase("update ct1 set name = 'Alice1' where id = 1", null).test(schema);
        new SqlTestCase("select * from ct1 where id = 1 and name = 'Alice1'",
                Collections.singletonList(new Object[]{1, "Alice1", 1})).test(schema);
        new SqlTestCase("delete from ct1 where id = 1", null).test(schema);
        new SqlTestCase("select * from ct1 where id = 1",
                Collections.emptyList()).test(schema);

        // List mode tests
        new SqlTestCase("delete from ct2 where id = 1", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice2', 2)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice3', 3)", null).test(schema);
        new SqlTestCase("select * from ct2 where id = 1",
                Arrays.asList(
                        new Object[]{1, "Alice3", 3},
                        new Object[]{1, "Alice2", 2},
                        new Object[]{1, "Alice1", 1})).test(schema);
        new SqlTestCase("select * from ct2 where id = 1 and name = 'Alice1'",
                Collections.singletonList(new Object[]{1, "Alice1", 1})).test(schema);
        new SqlTestCase("delete from ct2 where id = 1 and name = 'Alice1'", null).test(schema);
        new SqlTestCase("select * from ct2 where id = 1",
                Arrays.asList(
                        new Object[]{1, "Alice3", 3},
                        new Object[]{1, "Alice2", 2})).test(schema);

        // Join tests: cluster Redis table joined with in-memory table
        new SqlTestCase("select * from ct3 left join ct1 on ct3.id = ct1.id",
                Arrays.asList(
                        new Object[]{1, "Alice", null, null, null},
                        new Object[]{2, "Bob", null, null, null},
                        new Object[]{3, "Charlie", null, null, null})).test(schema);
        new SqlTestCase("select * from ct3 left join ct1 on ct3.id = ct1.id where ct3.name = 'Alice'",
                Collections.singletonList(new Object[]{1, "Alice", null, null, null})).test(schema);
        new SqlTestCase("select * from ct3 join ct1 on ct3.id = ct1.id",
                Collections.emptyList()).test(schema);
        new SqlTestCase("select * from ct3 join ct2 on ct3.id = ct2.id",
                Arrays.asList(
                        new Object[]{1, "Alice", 1, "Alice3", 3},
                        new Object[]{1, "Alice", 1, "Alice2", 2})).test(schema);
        new SqlTestCase("select * from ct3 join ct2 on ct3.id = ct2.id where ct2.name = 'Alice2'",
                Collections.singletonList(new Object[]{1, "Alice", 1, "Alice2", 2})).test(schema);
        new SqlTestCase("select ct3.id id1, ct2.id id2 from ct3 join ct2 on ct3.id = ct2.id where ct2.name = 'Alice2'",
                Collections.singletonList(new Object[]{1, 1})).test(schema);
    }

    @Test
    public void testRedisClusterListTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("ct2", getClusterListRedisTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        new SqlTestCase("delete from ct2 where id = 1", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice2', 2)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice3', 3)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice3', 4)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice3', 5)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice3', 6)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice3', 7)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice3', 8)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice3', 9)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice3', 10)", null).test(schema);
        new SqlTestCase("insert into ct2 (ID, NAME, CNT) values (1, 'Alice3', 11)", null).test(schema);
        new SqlTestCase("select * from ct2 where id = 1",
                Arrays.asList(
                        new Object[]{1, "Alice3", 11},
                        new Object[]{1, "Alice3", 10},
                        new Object[]{1, "Alice3", 9},
                        new Object[]{1, "Alice3", 8},
                        new Object[]{1, "Alice3", 7},
                        new Object[]{1, "Alice3", 6},
                        new Object[]{1, "Alice3", 5},
                        new Object[]{1, "Alice3", 4},
                        new Object[]{1, "Alice3", 3},
                        new Object[]{1, "Alice2", 2})).test(schema);
    }

    @Test
    public void testRedisClusterStringTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("ct5", getClusterStringRedisTable("ct5", "ID", "INTEGER", "SCORE", "INTEGER", 0));
        tableMap.put("ct6", getClusterStringRedisTable("ct6", "ID", "INTEGER", "SCORE", "DOUBLE", 0));
        tableMap.put("ct7", getClusterStringRedisTable("ct7", "ID", "INTEGER", "ACTIVE", "BOOLEAN", 0));
        tableMap.put("ct8", getClusterStringRedisTable("ct8", "ID", "INTEGER", "NAME", "VARCHAR", 0));

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        // integer value
        new SqlTestCase("delete from ct5 where id = 1", null).test(schema);
        new SqlTestCase("insert into ct5 (ID, SCORE) values (1, 100)", null).test(schema);
        new SqlTestCase("select * from ct5 where id = 1",
                Collections.singletonList(new Object[]{1, 100})).test(schema);
        new SqlTestCase("delete from ct5 where id = 1", null).test(schema);
        new SqlTestCase("select * from ct5 where id = 1",
                Collections.emptyList()).test(schema);

        // double value
        new SqlTestCase("delete from ct6 where id = 1", null).test(schema);
        new SqlTestCase("insert into ct6 (ID, SCORE) values (1, 3.14)", null).test(schema);
        new SqlTestCase("select * from ct6 where id = 1",
                Collections.singletonList(new Object[]{1, 3.14})).test(schema);
        new SqlTestCase("delete from ct6 where id = 1", null).test(schema);

        // boolean value
        new SqlTestCase("delete from ct7 where id = 1", null).test(schema);
        new SqlTestCase("insert into ct7 (ID, ACTIVE) values (1, true)", null).test(schema);
        new SqlTestCase("select * from ct7 where id = 1",
                Collections.singletonList(new Object[]{1, true})).test(schema);
        new SqlTestCase("delete from ct7 where id = 1", null).test(schema);

        // varchar value
        new SqlTestCase("delete from ct8 where id = 1", null).test(schema);
        new SqlTestCase("insert into ct8 (ID, NAME) values (1, 'hello')", null).test(schema);
        new SqlTestCase("select * from ct8 where id = 1",
                Collections.singletonList(new Object[]{1, "hello"})).test(schema);
        new SqlTestCase("delete from ct8 where id = 1", null).test(schema);

        // field count > 2 should throw exception
        boolean exceptionThrown = false;
        try {
            getClusterStringRedisTable("ct9", "ID", "INTEGER", "SCORE", "INTEGER", 0,
                    new FieldSchema("EXTRA", "INTEGER"));
        } catch (IllegalArgumentException e) {
            exceptionThrown = true;
            assert e.getMessage().contains("exactly 2 fields");
        }
        assert exceptionThrown : "Expected IllegalArgumentException for field count > 2";
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {
        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, "Alice"},
                    {2, "Bob"},
                    {3, "Charlie"}
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("ID", SqlTypeName.INTEGER)
                    .add("NAME", SqlTypeName.VARCHAR, 20)
                    .build();
        }
    }

    public static Table getClusterRedisTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("ID", "INTEGER"));
        fieldSchemas.add(new FieldSchema("NAME", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("CNT", "INTEGER"));

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.url = REDIS_CLUSTER_URL;
        redisConfig.redisMode = RedisOptions.CLUSTER_MODE;
        redisConfig.dataStructure = "json";
        redisConfig.ttl = 10000;
        redisConfig.database = "default";
        redisConfig.tableName = "ct1";
        redisConfig.fieldSchemas = fieldSchemas;
        redisConfig.primaryKey = "ID";
        redisConfig.primaryKeyIndex = 0;
        redisConfig.cacheTtl = 30;
        redisConfig.maxCacheSize = 100000;

        return new RedisCalciteTable(redisConfig);
    }

    public static Table getClusterListRedisTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("ID", "INTEGER"));
        fieldSchemas.add(new FieldSchema("NAME", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("CNT", "INTEGER"));

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.url = REDIS_CLUSTER_URL;
        redisConfig.redisMode = RedisOptions.CLUSTER_MODE;
        redisConfig.dataStructure = "list";
        redisConfig.ttl = 10000;
        redisConfig.database = "default";
        redisConfig.tableName = "ct2";
        redisConfig.fieldSchemas = fieldSchemas;
        redisConfig.primaryKey = "ID";
        redisConfig.primaryKeyIndex = 0;
        redisConfig.cacheTtl = 30;
        redisConfig.maxCacheSize = 100000;
        redisConfig.maxListSize = 10;

        return new RedisCalciteTable(redisConfig);
    }

    public static Table getClusterStringRedisTable(String tableName, String pkName, String pkType,
                                                    String valueName, String valueType, int primaryKeyIndex,
                                                    FieldSchema... extraFields) {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema(pkName, pkType));
        fieldSchemas.add(new FieldSchema(valueName, valueType));
        for (FieldSchema extra : extraFields) {
            fieldSchemas.add(extra);
        }

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.url = REDIS_CLUSTER_URL;
        redisConfig.redisMode = RedisOptions.CLUSTER_MODE;
        redisConfig.dataStructure = "string";
        redisConfig.ttl = 10000;
        redisConfig.database = "default";
        redisConfig.tableName = tableName;
        redisConfig.fieldSchemas = fieldSchemas;
        redisConfig.primaryKey = pkName;
        redisConfig.primaryKeyIndex = primaryKeyIndex;
        redisConfig.cacheTtl = 30;
        redisConfig.maxCacheSize = 100000;

        return new RedisCalciteTable(redisConfig);
    }
}
