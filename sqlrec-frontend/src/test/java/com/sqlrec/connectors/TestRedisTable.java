package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.connectors.redis.calcite.RedisCalciteTable;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.schema.HmsSchema;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        HmsSchema.setGlobalSchema(schema);

        new SqlTestCase("delete from t1 where id = 1", null).test(schema);
        new SqlTestCase("insert into t1 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1", null).test(schema);
        new SqlTestCase("update t1 set name = 'a' where id = 1", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1 and name = 'a'", null).test(schema);
        new SqlTestCase("update t1 set name = 'Alice1' where id = 1", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1 and name = 'Alice1'", null).test(schema);
        new SqlTestCase("delete from t1 where id = 1", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1", null).test(schema);
        new SqlTestCase("delete from t2 where id = 1", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice2', 2)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)", null).test(schema);
        new SqlTestCase("select * from t2 where id = 1", null).test(schema);
        new SqlTestCase("select * from t2 where id = 1 and name = 'Alice1'", null).test(schema);
        new SqlTestCase("delete from t2 where id = 1 and name = 'Alice1'", null).test(schema);
        new SqlTestCase("select * from t2 where id = 1", null).test(schema);

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

        new SqlTestCase("select * from t3 left join t2 on t3.id = t2.id", null).test(schema);
        new SqlTestCase("select * from t3 left join t2 on t3.id = t2.id where t3.name = 'Alice'", null).test(schema);
        new SqlTestCase("select * from t3 join t2 on t3.id = t2.id", null).test(schema);
        new SqlTestCase("select * from t3 join t2 on t3.id = t2.id where t3.name = 'Alice'", null).test(schema);
        new SqlTestCase("select * from t3 join t2 on t3.id = t2.id where t2.name = 'Alice1'", null).test(schema);
        new SqlTestCase("select * from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'", null).test(schema);
        new SqlTestCase("select t3.* from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'", null).test(schema);
        new SqlTestCase("select t3.*, t2.* from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'", null).test(schema);
        new SqlTestCase("select t3.id id1, t2.id id2 from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'", null).test(schema);
        new SqlTestCase("select id1, count(id2) from ( " +
                "select t3.id id1, t2.id id2 from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'" +
                ") group by id1 order by id1 limit 10", null).test(schema);
        new SqlTestCase("select * from ( " +
                "select t3.id id1, t2.id id2 from t3 join t2 on t3.id = t2.id where t2.name = 'Alice2'" +
                ") t " +
                "union all " +
                "select * from ( " +
                "select t3.id id1, t2.id id2 from t3 join t2 on t3.id = t2.id where t2.name = 'Alice3'" +
                ") t2 ", null).test(schema);
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
        HmsSchema.setGlobalSchema(schema);

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
        new SqlTestCase("select * from t2 where id = 1", null).test(schema);
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
        redisConfig.url = "redis://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":32379/0";
        redisConfig.redisMode = "single";
        redisConfig.dataStructure = "string";
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
        redisConfig.url = "redis://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":32379/0";
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
}
