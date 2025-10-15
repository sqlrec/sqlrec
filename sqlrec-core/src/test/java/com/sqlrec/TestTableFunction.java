package com.sqlrec;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.common.udf.table.AddColFunction;
import com.sqlrec.common.udf.table.ShuffleFunction;
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

public class TestTableFunction {
    @Test
    public void testTableFunction() throws Exception {
        ExecuteContext executeContext = new ExecuteContext();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        HmsSchema.setGlobalSchema(schema);
        TableFunctionUtils.registerTableFunction("default", "shuffle", ShuffleFunction.class);
        TableFunctionUtils.registerTableFunction("default", "add_col", AddColFunction.class);
        TableFunctionUtils.registerTableFunction("default", "empty_fun", TestEmptyFun.class);
        TableFunctionUtils.registerTableFunction("default", "string_arg_fun", TestStringArgFun.class);
        TableFunctionUtils.registerTableFunction("default", "context_fun", TestContextFun.class);

        List<String> sqlList = Arrays.asList(
                "cache table t1 as select * from myTable",
                "call shuffle(t1)",
                "cache table t2 as shuffle(t1)",
                "select * from t2",
                "call add_col(t1, 'new_col', 'new_col_value')",
                "cache table t3 as add_col(t1, 'new_col', 'new_col_value')",
                "select * from t3",
                "set col_name=new_col_test",
                "set col_value=new_col_value_test",
                "set func_name=add_col",
                "call add_col(t1, 'new_col', get('col_value'))",
                "cache table t4 as add_col(t1, 'new_col', get('col_value'))",
                "select * from t4",
                "call get('func_name')(t1, 'new_col', get('col_value')) like t4",
                "cache table t5 as call get('func_name')(t1, 'new_col', get('col_value')) like t4",
                "call get('func_name')(t1, 'new_col', get('col_value'))",
                "call empty_fun()",
                "call string_arg_fun('test_arg')",
                "call string_arg_fun(get('col_name'))",
                "call context_fun('col_name')"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema, executeContext);
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            }
        }
    }

    public static class TestEmptyFun {
        public void eval() {

        }
    }

    public static class TestStringArgFun {
        public void eval(String arg) {
            System.out.println(arg);
        }
    }

    public static class TestContextFun {
        public CacheTable eval(ExecuteContext context, String argName) {
            String argValue = context.getVariable(argName);
            if (argValue != null) {
                System.out.println(argValue);
            } else {
                throw new RuntimeException("argName not found in context");
            }
            return null;
        }
    }
}
