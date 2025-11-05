package com.sqlrec.connectors;

import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
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
public class TestJoinRedis {
    @Test
    public void testJoinRedis() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getRedisTable());
        tableMap.put("t2", getListRedisTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        HmsSchema.setGlobalSchema(schema);

        List<String> sqlList = Arrays.asList(
                "delete from t2 where id = 1",
                "delete from t2 where id = 2",
                "delete from t2 where id = 3",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice1', 1)",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice2', 2)",
                "insert into t2 (ID, NAME, CNT) values (1, 'Alice3', 3)",
                "insert into t2 (ID, NAME, CNT) values (2, 'Alice1', 1)",
                "insert into t2 (ID, NAME, CNT) values (2, 'Alice2', 2)",
                "insert into t2 (ID, NAME, CNT) values (2, 'Alice3', 3)",
                "insert into t2 (ID, NAME, CNT) values (3, 'Alice1', 1)",
                "insert into t2 (ID, NAME, CNT) values (3, 'Alice2', 2)",
                "insert into t2 (ID, NAME, CNT) values (3, 'Alice3', 3)",
                "cache table t3 as select cnt id from t2 where id = 1 order by id",
                "select * from t3",
                "select * from t3 join t2 on t3.id = t2.id"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema, new ExecuteContextImpl());
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
                if (sql.contains("join")) {
                    assert results.size() == 9;
                    assert results.get(0)[0].equals(1);
                    assert results.get(1)[0].equals(2);
                    assert results.get(2)[0].equals(3);
                    assert results.get(0)[3].equals(3L);
                    assert results.get(1)[3].equals(3L);
                    assert results.get(2)[3].equals(3L);
                }
            } else {
                System.out.println("no result");
            }
        }
    }
}
