package com.sqlrec;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.JavaFunctionUtils;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestJavaFunction {
    @Test
    public void testTableFunction() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        HmsSchema.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "empty_fun", TestEmptyFun.class);
        JavaFunctionUtils.registerTableFunction("default", "string_arg_fun", TestStringArgFun.class);
        JavaFunctionUtils.registerTableFunction("default", "context_fun", TestContextFun.class);

        List<String> sqlList2 = Arrays.asList(
                "create sql function test_add_col",
                "define input table input1(id int, name string, new_col string)",
                "return input1");
        new CompileManager().compileSqlFunction("test_add_col", sqlList2);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table t1 as select * from myTable",
                        Arrays.<Object[]>asList(
                                new Object[]{"t1", 3L}
                        )
                ),
                new SqlTestCase("call shuffle(t1)"),
                new SqlTestCase("call shuffle(t1) async"),
                new SqlTestCase(
                        "cache table t2 as call shuffle(t1)",
                        Arrays.<Object[]>asList(
                                new Object[]{"t2", 3L}
                        )
                ),
                new SqlTestCase("select * from t2"),
                new SqlTestCase("call add_col(t1, 'new_col', 'new_col_value')"),
                new SqlTestCase("call add_col(t1, 'new_col', 'new_col_value') async"),
                new SqlTestCase(
                        "cache table t3 as call add_col(t1, 'new_col', 'new_col_value')",
                        Arrays.<Object[]>asList(
                                new Object[]{"t3", 3L}
                        )
                ),
                new SqlTestCase(
                        "select * from t3",
                        Arrays.asList(
                                new Object[]{1, 1L, 1.0d, 1.0d, "1", true, "abc", Arrays.asList(1, 2, 3), Arrays.asList("a", "b", "c"), Arrays.asList(1.0d, 2.0d, 3.0d), Arrays.asList(1.0d, 2.0d, 3.0d), "new_col_value"},
                                new Object[]{2, 2L, 2.0d, 2.0d, "2", false, "bcd", Arrays.asList(4, 5, 6), Arrays.asList("d", "e", "f"), Arrays.asList(4.0d, 5.0d, 6.0d), Arrays.asList(4.0d, 5.0d, 6.0d), "new_col_value"},
                                new Object[]{3, 3L, 3.0d, 3.0d, "3", true, "cde", Arrays.asList(7, 8, 9), Arrays.asList("g", "h", "i"), Arrays.asList(7.0d, 8.0d, 9.0d), Arrays.asList(7.0d, 8.0d, 9.0d), "new_col_value"}
                        )
                ),
                new SqlTestCase("set col_name=new_col_test"),
                new SqlTestCase("set col_value=new_col_value_test"),
                new SqlTestCase("set func_name=add_col"),
                new SqlTestCase("call add_col(t1, 'new_col', get('col_value'))"),
                new SqlTestCase("call add_col(t1, 'new_col', get('col_value')) async"),
                new SqlTestCase(
                        "cache table t4 as call add_col(t1, 'new_col', get('col_value'))",
                        Arrays.<Object[]>asList(
                                new Object[]{"t4", 3L}
                        )
                ),
                new SqlTestCase(
                        "select * from t4",
                        Arrays.asList(
                                new Object[]{1, 1L, 1.0d, 1.0d, "1", true, "abc", Arrays.asList(1, 2, 3), Arrays.asList("a", "b", "c"), Arrays.asList(1.0d, 2.0d, 3.0d), Arrays.asList(1.0d, 2.0d, 3.0d), "new_col_value_test"},
                                new Object[]{2, 2L, 2.0d, 2.0d, "2", false, "bcd", Arrays.asList(4, 5, 6), Arrays.asList("d", "e", "f"), Arrays.asList(4.0d, 5.0d, 6.0d), Arrays.asList(4.0d, 5.0d, 6.0d), "new_col_value_test"},
                                new Object[]{3, 3L, 3.0d, 3.0d, "3", true, "cde", Arrays.asList(7, 8, 9), Arrays.asList("g", "h", "i"), Arrays.asList(7.0d, 8.0d, 9.0d), Arrays.asList(7.0d, 8.0d, 9.0d), "new_col_value_test"}
                        )
                ),

                new SqlTestCase("call get('func_name')(t1, 'new_col', get('col_value')) like t4",
                        Arrays.asList(
                                new Object[]{1, 1L, 1.0d, 1.0d, "1", true, "abc", Arrays.asList(1, 2, 3), Arrays.asList("a", "b", "c"), Arrays.asList(1.0d, 2.0d, 3.0d), Arrays.asList(1.0d, 2.0d, 3.0d), "new_col_value_test"},
                                new Object[]{2, 2L, 2.0d, 2.0d, "2", false, "bcd", Arrays.asList(4, 5, 6), Arrays.asList("d", "e", "f"), Arrays.asList(4.0d, 5.0d, 6.0d), Arrays.asList(4.0d, 5.0d, 6.0d), "new_col_value_test"},
                                new Object[]{3, 3L, 3.0d, 3.0d, "3", true, "cde", Arrays.asList(7, 8, 9), Arrays.asList("g", "h", "i"), Arrays.asList(7.0d, 8.0d, 9.0d), Arrays.asList(7.0d, 8.0d, 9.0d), "new_col_value_test"}
                        )
                ),
                new SqlTestCase("call get('func_name')(t1, 'new_col', get('col_value')) like t4 async"),
                new SqlTestCase(
                        "cache table t5 as call get('func_name')(t1, 'new_col', get('col_value')) like t4",
                        Arrays.<Object[]>asList(
                                new Object[]{"t5", 3L}
                        )
                ),

                new SqlTestCase("cache table tmp as select 1 as id, '1' as name"),
                new SqlTestCase("call get('func_name')(tmp, 'new_col', get('col_value')) like function 'test_add_col'",
                        Arrays.<Object[]>asList(
                                new Object[]{1, "1", "new_col_value_test"}
                        )
                ),
                new SqlTestCase("call get('func_name')(tmp, 'new_col', get('col_value')) like function 'test_add_col' async"),
                new SqlTestCase(
                        "cache table t6 as call get('func_name')(tmp, 'new_col', get('col_value')) like function 'test_add_col'",
                        Arrays.<Object[]>asList(
                                new Object[]{"t6", 1L}
                        )
                ),

                new SqlTestCase("call empty_fun()"),
                new SqlTestCase("call string_arg_fun('test_arg')"),
                new SqlTestCase("call string_arg_fun(get('col_name'))"),
                new SqlTestCase("call context_fun('col_name')")
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }

    public static class TestEmptyFun {
        public void evaluate() {

        }
    }

    public static class TestStringArgFun {
        public void evaluate(String arg) {
            System.out.println(arg);
        }
    }

    public static class TestContextFun {
        public void evaluate(ExecuteContext context, String argName) {
            String argValue = context.getVariable(argName);
            if (argValue != null) {
                System.out.println(argValue);
            }
        }
    }
}
