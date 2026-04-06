package com.sqlrec;

import com.sqlrec.common.config.Consts;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TestUnion {
    @Test
    public void testUnionSql() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        List<String> sqlList = Arrays.asList(
                "cache table t0 as select 1 as a union all select 2 as a union all select 3 as a",
                "cache table t1 as select 4 as a union all select 5 as a union all select 6 as a",
                "cache table t2 as select 7 as a union all select 8 as a union all select 9 as a",
                "select * from t0 union all select * from t1 union all select * from t2"
        );

        List<Object[]> results = null;
        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = new CompileManager().compileSql(flinkSqlNode, schema, Consts.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema, new ExecuteContextImpl());
            if (enumerable != null) {
                results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            } else {
                System.out.println("no result");
            }
        }

        assert results != null;
        assert results.size() == 9;
        assert results.get(0)[0].equals(1);
        assert results.get(1)[0].equals(4);
        assert results.get(2)[0].equals(7);
        assert results.get(3)[0].equals(2);
        assert results.get(4)[0].equals(5);
        assert results.get(5)[0].equals(8);
        assert results.get(6)[0].equals(3);
        assert results.get(7)[0].equals(6);
        assert results.get(8)[0].equals(9);
    }
}
