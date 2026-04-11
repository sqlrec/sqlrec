package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.JavaFunctionUtils;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
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
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        HmsSchema.setGlobalSchema(schema);

        new SqlTestCase("delete from t1 where id = 1", null).test(schema);
        new SqlTestCase("insert into t1 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("delete from t2 where id = 1", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice2', 2)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)", null).test(schema);

        new SqlTestCase("cache table t3 as select id from t1 where id = 1", null).test(schema);

        new SqlTestCase("select * from t3 join t3 t on 1=1", Arrays.<Object[]>asList(new Object[]{1, 1}), """
                LogicalProject(id=[$0], id0=[$1])
                  LogicalJoin(condition=[true], joinType=[inner])
                    LogicalTableScan(table=[[t3]])
                    LogicalTableScan(table=[[t3]])""", """
                EnumerableNestedLoopJoin(condition=[true], joinType=[inner])
                  EnumerableTableScan(table=[[t3]])
                  EnumerableTableScan(table=[[t3]])""", null).test(schema);

        new SqlTestCase("select * from t3 join t3 t on t3.id = t.id", Arrays.<Object[]>asList(new Object[]{1, 1}), """
                LogicalProject(id=[$0], id0=[$1])
                  LogicalJoin(condition=[=($0, $1)], joinType=[inner])
                    LogicalTableScan(table=[[t3]])
                    LogicalTableScan(table=[[t3]])""", """
                EnumerableMergeJoin(condition=[=($0, $1)], joinType=[inner])
                  EnumerableSort(sort0=[$0], dir0=[ASC])
                    EnumerableTableScan(table=[[t3]])
                  EnumerableSort(sort0=[$0], dir0=[ASC])
                    EnumerableTableScan(table=[[t3]])""", null).test(schema);

        new SqlTestCase("select t3.id, t2.name from t3 join t2 on t3.id = t2.id limit 2", Arrays.<Object[]>asList(new Object[]{1, "Alice3"}, new Object[]{1, "Alice2"}), """
                LogicalSort(fetch=[2])
                  LogicalProject(id=[$0], name=[$2])
                    LogicalJoin(condition=[=($0, $1)], joinType=[inner])
                      LogicalTableScan(table=[[t3]])
                      LogicalTableScan(table=[[default, t2]])""", """
                EnumerableCalc(expr#0..3=[{inputs}], id=[$t0], name=[$t2])
                  EnumerableLimit(fetch=[2])
                    SqlrecEnumerableKvJoin(condition=[=($0, $1)], joinType=[inner])
                      EnumerableTableScan(table=[[t3]])
                      EnumerableTableScan(table=[[default, t2]])""", null).test(schema);
    }

    @Test
    public void testJoinInFunction() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getRedisTable());
        tableMap.put("t2", getListRedisTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        HmsSchema.setGlobalSchema(schema);

        JavaFunctionUtils.setSkipHmsQuery(true);
        List<String> joinFuncSqlList = Arrays.asList(
                "create or replace sql function test_join",
                "define input table user_info(id integer)",
                "cache table t4 as select user_info.id, t2.name from user_info join t2 on user_info.id = t2.id",
                "return t4"
        );
        new CompileManager().compileSqlFunction("test_join", joinFuncSqlList);

        new SqlTestCase("delete from t2 where id = 1", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice2', 2)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)", null).test(schema);

        new SqlTestCase("cache table t3 as select 1 as id", null).test(schema);
        new SqlTestCase("call test_join(t3)", Arrays.<Object[]>asList(new Object[]{1, "Alice3"}, new Object[]{1, "Alice2"}, new Object[]{1, "Alice1"})).test(schema);
    }
}
