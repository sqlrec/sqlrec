package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.connectors.redis.calcite.RedisCalciteTable;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.udf.UdfManager;
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

@Tag("integration")
public class TestRedisTable {
    @Test
    public void testRedisTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getRedisTable());
        tableMap.put("t2", getListRedisTable());
        tableMap.put("t3", new MyTable());
        tableMap.put("t4", new MyTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        new SqlTestCase("select * from t1", null, new RuntimeException()).test(schema);

        new SqlTestCase("delete from t1 where id = 1", null).test(schema);
        new SqlTestCase("insert into t1 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1",
                Collections.singletonList(new Object[]{1, "Alice1", 1})).test(schema);
        new SqlTestCase("update t1 set name = 'a' where id = 1", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1 and name = 'a'",
                Collections.singletonList(new Object[]{1, "a", 1})).test(schema);
        new SqlTestCase("update t1 set name = 'Alice1' where id = 1", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1 and name = 'Alice1'",
                Collections.singletonList(new Object[]{1, "Alice1", 1})).test(schema);
        new SqlTestCase("delete from t1 where id = 1", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1",
                Collections.emptyList()).test(schema);
        new SqlTestCase("delete from t2 where id = 1", null).test(schema);
        new SqlTestCase("delete from t2 where id = 2", null).test(schema);
        new SqlTestCase("delete from t2 where id = 3", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice2', 2)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)", null).test(schema);
        new SqlTestCase("select * from t2 where id = 1",
                Arrays.asList(
                        new Object[]{1, "Alice3", 3},
                        new Object[]{1, "Alice2", 2},
                        new Object[]{1, "Alice1", 1})).test(schema);
        new SqlTestCase("select * from t2 where id = 1 and name = 'Alice1'",
                Collections.singletonList(new Object[]{1, "Alice1", 1})).test(schema);
        new SqlTestCase("delete from t2 where id = 1 and name = 'Alice1'", null).test(schema);
        new SqlTestCase("select * from t2 where id = 1",
                Arrays.asList(
                        new Object[]{1, "Alice3", 3},
                        new Object[]{1, "Alice2", 2})).test(schema);

        new SqlTestCase("select * from t3 join t3 as t on t3.id = t.id", null, """
                LogicalProject(ID=[$0], NAME=[$1], ID0=[$2], NAME0=[$3])
                  LogicalJoin(condition=[=($0, $2)], joinType=[inner])
                    LogicalTableScan(table=[[default, t3]])
                    LogicalTableScan(table=[[default, t3]])""", """
                EnumerableMergeJoin(condition=[=($0, $2)], joinType=[inner])
                  EnumerableSort(sort0=[$0], dir0=[ASC])
                    EnumerableTableScan(table=[[default, t3]])
                  EnumerableSort(sort0=[$0], dir0=[ASC])
                    EnumerableTableScan(table=[[default, t3]])""", null).test(schema);

        new SqlTestCase("select * from t3 join t4 on t3.id = t4.id", null, """
                LogicalProject(ID=[$0], NAME=[$1], ID0=[$2], NAME0=[$3])
                  LogicalJoin(condition=[=($0, $2)], joinType=[inner])
                    LogicalTableScan(table=[[default, t3]])
                    LogicalTableScan(table=[[default, t4]])""", """
                EnumerableMergeJoin(condition=[=($0, $2)], joinType=[inner])
                  EnumerableSort(sort0=[$0], dir0=[ASC])
                    EnumerableTableScan(table=[[default, t3]])
                  EnumerableSort(sort0=[$0], dir0=[ASC])
                    EnumerableTableScan(table=[[default, t4]])""", null).test(schema);

        new SqlTestCase("select * from t3 left join t2 on t3.id = t2.id order by t3.id, t2.name",
                Arrays.asList(
                        new Object[]{1, "Alice", 1, "Alice2", 2},
                        new Object[]{1, "Alice", 1, "Alice3", 3},
                        new Object[]{2, "Bob", null, null, null},
                        new Object[]{3, "Charlie", null, null, null})).test(schema);
        new SqlTestCase("select * from t3 left join t2 on t3.id = t2.id where t3.name = 'Alice'",
                Arrays.asList(
                        new Object[]{1, "Alice", 1, "Alice3", 3},
                        new Object[]{1, "Alice", 1, "Alice2", 2})).test(schema);
        new SqlTestCase("select * from t3 join t2 on t3.id = t2.id",
                Arrays.asList(
                        new Object[]{1, "Alice", 1, "Alice3", 3},
                        new Object[]{1, "Alice", 1, "Alice2", 2})).test(schema);
        new SqlTestCase("select * from t3 join t2 on t3.id = t2.id where t3.name = 'Alice'",
                Arrays.asList(
                        new Object[]{1, "Alice", 1, "Alice3", 3},
                        new Object[]{1, "Alice", 1, "Alice2", 2})).test(schema);
        new SqlTestCase("select * from t3 join t2 on t3.id = t2.id where t2.name = 'Alice1'",
                Collections.emptyList()).test(schema);
        new SqlTestCase("select * from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'",
                Collections.singletonList(new Object[]{1, "Alice", 1, "Alice2", 2})).test(schema);
        new SqlTestCase("select t3.* from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'",
                Collections.singletonList(new Object[]{1, "Alice"})).test(schema);
        new SqlTestCase("select t3.*, t2.* from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'",
                Collections.singletonList(new Object[]{1, "Alice", 1, "Alice2", 2})).test(schema);
        new SqlTestCase("select t3.id id1, t2.id id2 from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'",
                Collections.singletonList(new Object[]{1, 1})).test(schema);
        new SqlTestCase("select id1, count(id2) from ( " +
                "select t3.id id1, t2.id id2 from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'" +
                ") group by id1 order by id1 limit 10",
                Collections.singletonList(new Object[]{1, 1L})).test(schema);
        new SqlTestCase("select * from ( " +
                "select t3.id id1, t2.id id2 from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'" +
                ") t " +
                "union all " +
                "select * from ( " +
                "select t3.id id1, t2.id id2 from t3 join t2 on t3.id = t2.id where t2.name = 'Alice3'" +
                ") t2 ",
                Arrays.asList(new Object[]{1, 1}, new Object[]{1, 1})).test(schema);

        // UPDATE replaces only the matching list entry under the shared index key.
        new SqlTestCase("update t2 set name = 'Alice3' where id = 1 and name = 'Alice2'",
                Collections.singletonList(new Object[]{1L})).test(schema);
        new SqlTestCase("select * from t2 where id = 1 order by cnt",
                Arrays.asList(new Object[]{1, "Alice3", 2}, new Object[]{1, "Alice3", 3})).test(schema);

        // One new row equals another selected old row; both must still be updated.
        new SqlTestCase("update t2 set cnt = cnt + 1 where id = 1",
                Collections.singletonList(new Object[]{2L})).test(schema);
        new SqlTestCase("select * from t2 where id = 1 order by cnt",
                Arrays.asList(new Object[]{1, "Alice3", 3}, new Object[]{1, "Alice3", 4})).test(schema);

        new SqlTestCase("update t2 set id = 2 where id = 1 and cnt = 4",
                Collections.singletonList(new Object[]{1L})).test(schema);
        new SqlTestCase("select * from t2 where id = 1",
                Collections.singletonList(new Object[]{1, "Alice3", 3})).test(schema);
        new SqlTestCase("select * from t2 where id = 2",
                Collections.singletonList(new Object[]{2, "Alice3", 4})).test(schema);

        // Identical list values are separate rows, so UPDATE must replace both.
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)", null).test(schema);
        new SqlTestCase("update t2 set name = 'Updated' where id = 1",
                Collections.singletonList(new Object[]{2L})).test(schema);
        new SqlTestCase("select * from t2 where id = 1",
                Arrays.asList(new Object[]{1, "Updated", 3}, new Object[]{1, "Updated", 3})).test(schema);
        new SqlTestCase("update t2 set name = 'Missing' where id = 99",
                Collections.singletonList(new Object[]{0L})).test(schema);

        new SqlTestCase("delete from t2 where id = 1", null).test(schema);
        new SqlTestCase("delete from t2 where id = 2", null).test(schema);
    }

    @Test
    public void testRedisListTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t2", getListRedisTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        new SqlTestCase("delete from t2 where id = 1", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice2', 2)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 4)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 5)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 6)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 7)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 8)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 9)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 10)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 11)", null).test(schema);
        new SqlTestCase("select * from t2 where id = 1",
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
    public void testRedisProtobufTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("redis_proto", getProtobufRedisTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);
        UdfManager.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "array_contains_all",
                "com.sqlrec.udf.scalar.ArrayContainsAllFunction"
        );

        long id = System.currentTimeMillis();
        String columns = "(int64_value, string_value, double_value, bool_value, status, "
                + "repeated_int32, repeated_string)";

        new SqlTestCase("delete from redis_proto where int64_value = " + id, null)
                .test(schema);
        new SqlTestCase(
                "insert into redis_proto " + columns + " values ("
                        + id + ", 'Alice', 95.5, true, 'ACTIVE', "
                        + "ARRAY[1, 2, 3], ARRAY['red', 'blue'])",
                null).test(schema);
        new SqlTestCase(
                "select * from redis_proto where int64_value = " + id,
                Collections.singletonList(new Object[]{
                        id, "Alice", 95.5D, true, "ACTIVE",
                        Arrays.asList(1, 2, 3), Arrays.asList("red", "blue")}))
                .test(schema);

        new SqlTestCase(
                "update redis_proto set string_value = 'Alice-updated', "
                        + "double_value = 99.25, bool_value = false, "
                        + "status = 'DISABLED', repeated_int32 = ARRAY[4, 5], "
                        + "repeated_string = ARRAY['green', 'yellow'] "
                        + "where int64_value = " + id,
                null).test(schema);
        new SqlTestCase(
                "select * from redis_proto where int64_value = " + id
                        + " and status = 'DISABLED' "
                        + "and array_contains_all(repeated_int32, ARRAY[4, 5])",
                Collections.singletonList(
                        new Object[]{
                                id, "Alice-updated", 99.25D, false, "DISABLED",
                                Arrays.asList(4, 5), Arrays.asList("green", "yellow")}))
                .test(schema);

        new SqlTestCase("delete from redis_proto where int64_value = " + id, null)
                .test(schema);
        new SqlTestCase(
                "select * from redis_proto where int64_value = " + id,
                Collections.emptyList()).test(schema);
    }

    @Test
    public void testRedisStringTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t5", getStringRedisTable("t5", "ID", "INTEGER", "SCORE", "INTEGER", 0));
        tableMap.put("t6", getStringRedisTable("t6", "ID", "INTEGER", "SCORE", "DOUBLE", 0));
        tableMap.put("t7", getStringRedisTable("t7", "ID", "INTEGER", "ACTIVE", "BOOLEAN", 0));
        tableMap.put("t8", getStringRedisTable("t8", "ID", "INTEGER", "NAME", "VARCHAR", 0));

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        // integer value
        new SqlTestCase("delete from t5 where id = 1", null).test(schema);
        new SqlTestCase("insert into t5 (ID, SCORE) values (1, 100)", null).test(schema);
        new SqlTestCase("select * from t5 where id = 1",
                Collections.singletonList(new Object[]{1, 100})).test(schema);
        new SqlTestCase("delete from t5 where id = 1", null).test(schema);
        new SqlTestCase("select * from t5 where id = 1",
                Collections.emptyList()).test(schema);

        // double value
        new SqlTestCase("delete from t6 where id = 1", null).test(schema);
        new SqlTestCase("insert into t6 (ID, SCORE) values (1, 3.14)", null).test(schema);
        new SqlTestCase("select * from t6 where id = 1",
                Collections.singletonList(new Object[]{1, 3.14})).test(schema);
        new SqlTestCase("delete from t6 where id = 1", null).test(schema);

        // boolean value
        new SqlTestCase("delete from t7 where id = 1", null).test(schema);
        new SqlTestCase("insert into t7 (ID, ACTIVE) values (1, true)", null).test(schema);
        new SqlTestCase("select * from t7 where id = 1",
                Collections.singletonList(new Object[]{1, true})).test(schema);
        new SqlTestCase("delete from t7 where id = 1", null).test(schema);

        // varchar value
        new SqlTestCase("delete from t8 where id = 1", null).test(schema);
        new SqlTestCase("insert into t8 (ID, NAME) values (1, 'hello')", null).test(schema);
        new SqlTestCase("select * from t8 where id = 1",
                Collections.singletonList(new Object[]{1, "hello"})).test(schema);
        new SqlTestCase("delete from t8 where id = 1", null).test(schema);

        // field count > 2 should throw exception
        boolean exceptionThrown = false;
        try {
            getStringRedisTable("t9", "ID", "INTEGER", "SCORE", "INTEGER", 0,
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

    public static Table getRedisTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("ID", "INTEGER"));
        fieldSchemas.add(new FieldSchema("NAME", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("CNT", "INTEGER"));

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.url = "redis://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30017/0";
        redisConfig.redisMode = "single";
        redisConfig.dataStructure = "json";
        redisConfig.ttl = 10000;
        redisConfig.database = "default";
        redisConfig.tableName = "t1";
        redisConfig.fieldSchemas = fieldSchemas;
        redisConfig.primaryKey = "ID";
        redisConfig.primaryKeyIndex = 0;
        redisConfig.cacheTtl = 30;
        redisConfig.maxCacheSize = 100000;

        return new RedisCalciteTable(redisConfig);
    }

    public static Table getListRedisTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("ID", "INTEGER"));
        fieldSchemas.add(new FieldSchema("NAME", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("CNT", "INTEGER"));

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.url = "redis://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30017/0";
        redisConfig.redisMode = "single";
        redisConfig.dataStructure = "list";
        redisConfig.ttl = 10000;
        redisConfig.database = "default";
        redisConfig.tableName = "t2";
        redisConfig.fieldSchemas = fieldSchemas;
        redisConfig.primaryKey = "ID";
        redisConfig.primaryKeyIndex = 0;
        redisConfig.cacheTtl = 30;
        redisConfig.maxCacheSize = 100000;
        redisConfig.maxListSize = 10;

        return new RedisCalciteTable(redisConfig);
    }

    public static Table getProtobufRedisTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("int64_value", "BIGINT"));
        fieldSchemas.add(new FieldSchema("string_value", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("double_value", "DOUBLE"));
        fieldSchemas.add(new FieldSchema("bool_value", "BOOLEAN"));
        fieldSchemas.add(new FieldSchema("status", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("repeated_int32", "ARRAY<INTEGER>"));
        fieldSchemas.add(new FieldSchema("repeated_string", "ARRAY<VARCHAR>"));

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.url = "redis://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30017/0";
        redisConfig.redisMode = "single";
        redisConfig.dataStructure = "json";
        redisConfig.format = "protobuf";
        redisConfig.protobufMessageClassName =
                "com.sqlrec.connectors.redis.proto.RedisAllTypes";
        redisConfig.ttl = 10000;
        redisConfig.database = "default";
        redisConfig.tableName = "redis_proto";
        redisConfig.fieldSchemas = fieldSchemas;
        redisConfig.primaryKey = "int64_value";
        redisConfig.primaryKeyIndex = 0;
        redisConfig.cacheTtl = 0;
        redisConfig.maxCacheSize = 100000;

        return new RedisCalciteTable(redisConfig);
    }

    public static Table getStringRedisTable(String tableName, String pkName, String pkType,
                                             String valueName, String valueType, int primaryKeyIndex,
                                             FieldSchema... extraFields) {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema(pkName, pkType));
        fieldSchemas.add(new FieldSchema(valueName, valueType));
        for (FieldSchema extra : extraFields) {
            fieldSchemas.add(extra);
        }

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.url = "redis://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30017/0";
        redisConfig.redisMode = "single";
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
