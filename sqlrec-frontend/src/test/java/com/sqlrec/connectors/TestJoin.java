package com.sqlrec.connectors;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.node.SqlrecEnumerableJoin;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CalciteBindable;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.Const;
import com.sqlrec.utils.JavaFunctionUtils;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.sqlrec.connectors.TestRedisTable.getListRedisTable;
import static com.sqlrec.connectors.TestRedisTable.getRedisTable;

@Tag("integration")
public class TestJoin {
    @Test
    public void testJoin() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getRedisTable());
        tableMap.put("t2", getListRedisTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Const.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        HmsSchema.setGlobalSchema(schema);

        List<String> sqlList = Arrays.asList(
                "delete from t1 where id = 1",
                "insert into t1 (ID, NAME, CNT) values (1, 'Alice1', 1)",
                "delete from t2 where id = 1",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice1', 1)",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice2', 2)",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)",

                "cache table t3 as select id from t1 where id = 1",
                "select t3.id, t2.name from t3 join t2 on t3.id = t2.id limit 2"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = new CompileManager().compileSql(flinkSqlNode, schema, Const.DEFAULT_SCHEMA_NAME);

            if (sql.contains("join")) {
                assert bindable instanceof CalciteBindable;
                CalciteBindable calciteBindable = (CalciteBindable) bindable;
                RelNode bestExp = calciteBindable.getBestExp();
                assert bestExp instanceof EnumerableCalc;
                RelNode limit = ((EnumerableCalc) bestExp).getInput();
                assert limit instanceof EnumerableLimit;
                RelNode join = ((EnumerableLimit) limit).getInput();
                assert join instanceof SqlrecEnumerableJoin;
                SqlrecEnumerableJoin sqlRecJoin = (SqlrecEnumerableJoin) join;
                assert sqlRecJoin.getLimit() == 2;
                assert sqlRecJoin.getProjectList().equals(Arrays.asList(0, 2));
            }

            Enumerable enumerable = bindable.bind(schema, new ExecuteContextImpl());
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

    @Test
    public void testJoinInFunction() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getRedisTable());
        tableMap.put("t2", getListRedisTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Const.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        HmsSchema.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "test_join", Integer.TYPE);  // avoid find function in hms
        List<String> joinFuncSqlList = Arrays.asList(
                "create or replace sql function test_join",
                "define input table user_info(id integer)",
                "cache table t4 as select user_info.id, t2.name from user_info join t2 on user_info.id = t2.id",
                "return t4"
        );
        new CompileManager().compileSqlFunction("test_join", joinFuncSqlList);

        List<String> sqlList = Arrays.asList(
                "delete from t2 where id = 1",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice1', 1)",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice2', 2)",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)",

                "cache table t3 as select 1 as id",
                "call test_join(t3)"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = new CompileManager().compileSql(flinkSqlNode, schema, Const.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema, new ExecuteContextImpl());
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
                if (sql.contains("call test_join")) {
                    assert results.size() == 3;
                    assert results.get(0)[0].equals(1);
                    assert results.get(1)[0].equals(1);
                    assert results.get(2)[0].equals(1);
                }
            } else {
                System.out.println("no result");
            }
        }
    }
}
