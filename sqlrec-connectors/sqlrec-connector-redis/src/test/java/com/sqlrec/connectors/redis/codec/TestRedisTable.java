package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.FieldSchema;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.connectors.redis.calcite.RedisCalciteTable;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

public class TestRedisTable {
    public static void main(String[] args) throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                Map<String, Table> tableMap = new HashMap<>();
                tableMap.put("t1", getRedisTable());
                tableMap.put("t2", getListRedisTable());
                tableMap.put("t3", new MyTable());
                return tableMap;
            }
        });
        HmsSchema.setGlobalSchema(schema);

        List<String> sqlList = Arrays.asList(
                "insert into t1 (ID, NAME, CNT) values (1, 'Alice1`', 1)",
                "select * from t1 where id = 1 and name = 'a'",
                "select * from t1 where id = 1 and name = 'Alice1'",
                "delete from t1 where id = 1",
                "select * from t1 where id = 1",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice1', 1)",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice2', 2)",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)",
                "select * from t2 where id = 1 and name = 'Alice1'",
                "delete from t2 where id = 1 and name = 'Alice1'",
                "select * from t2 where id = 1"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema);
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            } else {
                System.out.println("no result");
            }
        }
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

        @Override
        public SqlRecTableType getSqlRecTableType() {
            return SqlRecTableType.MEMORY;
        }
    }

    public static Table getRedisTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("ID", "INTEGER"));
        fieldSchemas.add(new FieldSchema("NAME", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("CNT", "INTEGER"));

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.url ="redis://127.0.0.1:32379/0";
        redisConfig.redisMode="single";
        redisConfig.dataStructure="string";
        redisConfig.ttl = 10000;
        redisConfig.tableName = "t1";
        redisConfig.fieldSchemas = fieldSchemas;
        redisConfig.primaryKey = "ID";
        redisConfig.primaryKeyIndex = 0;

        return new RedisCalciteTable(redisConfig);
    }

    public static Table getListRedisTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("ID", "INTEGER"));
        fieldSchemas.add(new FieldSchema("NAME", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("CNT", "INTEGER"));

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.url ="redis://127.0.0.1:32379/0";
        redisConfig.redisMode="single";
        redisConfig.dataStructure="list";
        redisConfig.ttl = 10000;
        redisConfig.tableName = "t2";
        redisConfig.fieldSchemas = fieldSchemas;
        redisConfig.primaryKey = "ID";
        redisConfig.primaryKeyIndex = 0;

        return new RedisCalciteTable(redisConfig);
    }
}
