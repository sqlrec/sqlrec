package com.sqlrec;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestSqlCompile {
    public static void main(String[] args) throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestCalciteSql.MyTable());
            }
        });

        HmsSchema.setGlobalSchema(schema);

        testSqlFunctionCompile(schema);

        List<String> sqlList = Arrays.asList(
                "select * from myTable",
                "cache table t0 as select 1 as a",
                "select * from t0",
                "cache table t1 as SELECT * FROM myTable",
                "select * from t1",
                "cache table t2 as SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME",
                "select * from t2",
                "call_sql_function fun1(t1)",
                "cache table t3 as fun1(t1)",
                "select * from t3"
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
    }
}
