package com.sqlrec;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.Const;
import com.sqlrec.utils.JavaFunctionUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestSqlFunction {
    @Test
    public void testSqlCompile() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Const.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestNormalSql.MyTable());
            }
        });

        HmsSchema.setGlobalSchema(schema);
        JavaFunctionUtils.registerTableFunction("default", "fun1", Integer.TYPE);  // avoid find function in hms
        JavaFunctionUtils.registerTableFunction("default", "fun2", Integer.TYPE);  // avoid find function in hms
        JavaFunctionUtils.registerTableFunction("default", "fun3", Integer.TYPE);  // avoid find function in hms

        testSqlFunctionCompile(schema);

        List<String> sqlList = Arrays.asList(
                "cache table t1 as SELECT * FROM myTable",
                "call fun1(t1)",
                "call fun1(t1) async",
                "cache table t3 as call fun1(t1)",
                "select * from t3",
                "call fun2(t1)",
                "call fun2(t1) async",
                "cache table t4 as call fun3(t1)",
                "select * from t4"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, Const.DEFAULT_SCHEMA_NAME);

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

    public static void testSqlFunctionCompile(CalciteSchema schema) throws Exception {
        List<String> sqlList = Arrays.asList(
                "create sql function fun1",
                "define input table input1(id int, name string)",
                "cache table t2 as SELECT ID, NAME FROM input1 where ID > 0",
                "cache table t3 as SELECT ID, NAME FROM input1 where ID > 0",
                "cache table t4 as SELECT ID, NAME FROM input1 where ID > 0",
                "cache table t5 as SELECT ID, NAME FROM input1 where ID > 0",
                "cache table t6 as SELECT ID, NAME FROM input1 where ID > 0",
                "cache table t7 as SELECT * from t2 union all SELECT * from t3 union all SELECT * from t4 union all SELECT * from t5 union all SELECT * from t6",
                "cache table r as SELECT NAME, count(*) as cnt FROM t7 where ID > 0 group by NAME",
                "return r"
                );
         CompileManager.compileSqlFunction("fun1", sqlList);

        List<String> sqlList2 = Arrays.asList(
                "create sql function fun2",
                "define input table input1(id int, name string)",
                "cache table t1 as SELECT NAME, count(*) as cnt FROM input1 where ID > 1 group by NAME",
                "return"
        );
        CompileManager.compileSqlFunction("fun2", sqlList2);

        List<String> sqlList3 = Arrays.asList(
                "create sql function fun3",
                "define input table input1(id int, name string)",
                "cache table t2 as call fun1(input1)",
                "cache table t3 as call fun1(input1)",
                "cache table t4 as call fun1(input1)",
                "cache table t5 as call fun1(input1)",
                "cache table t6 as call fun1(input1)",
                "cache table r as SELECT * from t2 union all SELECT * from t3 union all SELECT * from t4 union all SELECT * from t5 union all SELECT * from t6",
                "return r"
        );
        CompileManager.compileSqlFunction("fun3", sqlList3);
    }
}
