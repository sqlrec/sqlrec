package com.sqlrec;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.Const;
import com.sqlrec.utils.JavaFunctionUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TestExceptionIgnore {
    @Test
    public void testExceptionIgnore() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        HmsSchema.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "test_fun", TestExceptionIgnore.TestFunction.class);

        List<String> sqlFun1 = Arrays.asList(
                "create sql function sql_fun1",
                "define input table input1(id int)",
                "cache table t2 as SELECT * FROM input1 where ID > 0",
                "cache table t3 as SELECT * FROM input1 where ID > 0",
                "cache table t4 as call test_fun(t3)",
                "cache table t5 as SELECT * from t2 union all SELECT * from t4",
                "return t5"
        );
        new CompileManager().compileSqlFunction("sql_fun1", sqlFun1);

        List<String> sqlFun2 = Arrays.asList(
                "create sql function sql_fun2",
                "define input table input1(id int)",
                "cache table t1 as call test_fun(input1)",
                "return t1"
        );
        new CompileManager().compileSqlFunction("sql_fun2", sqlFun2);

        List<String> sqlFun3 = Arrays.asList(
                "create sql function sql_fun3",
                "define input table input1(id int)",
                "cache table t1 as call test_fun(input1)",
                "return input1"
        );
        new CompileManager().compileSqlFunction("sql_fun3", sqlFun3);

        List<String> sqlList = Arrays.asList(
                "cache table t1 as select 1 as id",
                "cache table t3 as call sql_fun1(t1)",
                "cache table t4 as call sql_fun2(t1)",
                "cache table t5 as call sql_fun3(t1)"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = new CompileManager().compileSql(flinkSqlNode, schema, Const.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = null;

            try {
                enumerable = bindable.bind(schema, executeContext);
            } catch (Exception e) {
                if (sql.contains("call sql_fun2(t1)")) {
                    System.out.println("sql_fun2 throw exception success: " + e.getMessage());
                } else {
                    throw e;
                }
            }

            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            } else {
                System.out.println("sql return null");
            }
        }
    }

    public static class TestFunction {
        public CacheTable eval(CacheTable input) {
            if (input.scan(null).count() == 0) {
                return input;
            }
            throw new RuntimeException("mock exception");
        }
    }
}
