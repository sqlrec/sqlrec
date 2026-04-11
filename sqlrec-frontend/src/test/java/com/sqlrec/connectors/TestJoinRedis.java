package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.sqlrec.connectors.TestRedisTable.getListRedisTable;
import static com.sqlrec.connectors.TestRedisTable.getRedisTable;

@Tag("integration")
public class TestJoinRedis {
    @Test
    public void testJoinRedis() throws Exception {
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

        new SqlTestCase("delete from t2 where id = 1", null).test(schema);
        new SqlTestCase("delete from t2 where id = 2", null).test(schema);
        new SqlTestCase("delete from t2 where id = 3", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice2', 2)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (2, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (2, 'Alice2', 2)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (2, 'Alice3', 3)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (3, 'Alice1', 1)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (3, 'Alice2', 2)", null).test(schema);
        new SqlTestCase("insert into t2 (ID, NAME, CNT) values (3, 'Alice3', 3)", null).test(schema);

        new SqlTestCase("cache table t3 as select cnt id from t2 where id = 1 order by id", null).test(schema);

        new SqlTestCase("select * from t3", Arrays.<Object[]>asList(new Object[]{1}, new Object[]{2}, new Object[]{3}), """
                LogicalProject(id=[$0])
                  LogicalTableScan(table=[[t3]])""", """
                EnumerableTableScan(table=[[t3]])""", null).test(schema);

        new SqlTestCase("select * from t3 join t2 on t3.id = t2.id", Arrays.<Object[]>asList(
                new Object[]{1, 1L, "Alice3", 3L},
                new Object[]{2, 2L, "Alice3", 3L},
                new Object[]{3, 3L, "Alice3", 3L},
                new Object[]{1, 1L, "Alice2", 2L},
                new Object[]{2, 2L, "Alice2", 2L},
                new Object[]{3, 3L, "Alice2", 2L},
                new Object[]{1, 1L, "Alice1", 1L},
                new Object[]{2, 2L, "Alice1", 1L},
                new Object[]{3, 3L, "Alice1", 1L}), """
                LogicalProject(id=[$0], ID0=[$1], NAME=[$2], CNT=[$3])
                  LogicalJoin(condition=[=($0, $1)], joinType=[inner])
                    LogicalTableScan(table=[[t3]])
                    LogicalTableScan(table=[[default, t2]])""", """
                SqlrecEnumerableKvJoin(condition=[=($0, $1)], joinType=[inner])
                  EnumerableTableScan(table=[[t3]])
                  EnumerableTableScan(table=[[default, t2]])""", null).test(schema);
    }
}
