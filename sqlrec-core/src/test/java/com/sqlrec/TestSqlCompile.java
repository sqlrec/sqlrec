package com.sqlrec;

import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.TableFunctionUtils;
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

public class TestSqlCompile {
    @Test
    public void testSqlCompile() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestNormalSqlCompiler.MyTable());
            }
        });

        HmsSchema.setGlobalSchema(schema);
        TableFunctionUtils.registerTableFunction("default", "fun1", Integer.TYPE);  // avoid find function in hms
        TableFunctionUtils.registerTableFunction("default", "fun2", Integer.TYPE);  // avoid find function in hms

        testSqlFunctionCompile(schema);

        List<String> sqlList = Arrays.asList(
                "select * from myTable",
                "cache table t0 as select 1 as a",
                "select * from t0",
                "cache table t1 as SELECT * FROM myTable",
                "select * from t1",
                "cache table t2 as SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME",
                "select * from t2",
                "call fun1(t1)",
                "cache table t3 as fun1(t1)",
                "select * from t3",
                "call fun2(t1)",
                "set param=test",
                "set 'param'='test'"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema, new ExecuteContext());
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
                "cache table t1 as SELECT NAME, count(*) as cnt FROM input1 where ID > 1 group by NAME",
                "return t1"
                );
         CompileManager.compileSqlFunction("fun1", sqlList);

        List<String> sqlList2 = Arrays.asList(
                "create sql function fun2",
                "define input table input1(id int, name string)",
                "cache table t1 as SELECT NAME, count(*) as cnt FROM input1 where ID > 1 group by NAME",
                "return"
        );
        CompileManager.compileSqlFunction("fun2", sqlList2);
    }
}
