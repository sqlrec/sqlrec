package com.sqlrec;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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
        CalciteSchemaFactory.setGlobalSchema(schema);

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

                new SqlTestCase("call get_or_default('func_name', 'add_col')(tmp, 'new_col', get('col_value')) like function 'test_add_col'",
                        Arrays.<Object[]>asList(
                                new Object[]{1, "1", "new_col_value_test"}
                        )
                ),
                new SqlTestCase("call get_or_default('func_name', 'add_col')(tmp, 'new_col', get('col_value')) like function 'test_add_col' async"),
                new SqlTestCase(
                        "cache table t7 as call get_or_default('func_name', 'add_col')(tmp, 'new_col', get('col_value')) like function 'test_add_col'",
                        Arrays.<Object[]>asList(
                                new Object[]{"t7", 1L}
                        )
                ),
                new SqlTestCase(
                        "cache table t8 as call get_or_default('non_existing_func', 'add_col')(tmp, 'new_col', get('col_value')) like function 'test_add_col'",
                        Arrays.<Object[]>asList(
                                new Object[]{"t8", 1L}
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

    @Test
    public void testOverloadAndVarArgs() throws Exception {
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "overload_fun", TestOverloadFun.class);
        JavaFunctionUtils.registerTableFunction("default", "varargs_fun", TestVarArgsFun.class);
        JavaFunctionUtils.registerTableFunction("default", "mixed_overload_fun", TestMixedOverloadFun.class);
        JavaFunctionUtils.registerTableFunction("default", "varargs_with_table_fun", TestVarArgsWithTableFun.class);
        JavaFunctionUtils.registerTableFunction("default", "varargs_table_fun", TestVarArgsTableFun.class);
        JavaFunctionUtils.registerTableFunction("default", "string_then_tables_fun", TestStringThenTablesFun.class);
        JavaFunctionUtils.registerTableFunction("default", "ambiguous_fun", TestAmbiguousFun.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table t1 as select * from myTable",
                        Arrays.<Object[]>asList(new Object[]{"t1", 3L})
                ),

                new SqlTestCase(
                        "cache table r1 as call overload_fun()",
                        Arrays.<Object[]>asList(new Object[]{"r1", 1L})
                ),
                new SqlTestCase(
                        "select * from r1",
                        Collections.singletonList(new Object[]{"no_args"})
                ),
                new SqlTestCase(
                        "cache table r2 as call overload_fun('arg1')",
                        Arrays.<Object[]>asList(new Object[]{"r2", 1L})
                ),
                new SqlTestCase(
                        "select * from r2",
                        Collections.singletonList(new Object[]{"one_arg: arg1"})
                ),
                new SqlTestCase(
                        "cache table r3 as call overload_fun('arg1', 'arg2')",
                        Arrays.<Object[]>asList(new Object[]{"r3", 1L})
                ),
                new SqlTestCase(
                        "select * from r3",
                        Collections.singletonList(new Object[]{"two_args: arg1, arg2"})
                ),

                new SqlTestCase(
                        "cache table r4 as call varargs_fun()",
                        Arrays.<Object[]>asList(new Object[]{"r4", 1L})
                ),
                new SqlTestCase(
                        "select * from r4",
                        Collections.singletonList(new Object[]{"varargs: []"})
                ),
                new SqlTestCase(
                        "cache table r5 as call varargs_fun('arg1')",
                        Arrays.<Object[]>asList(new Object[]{"r5", 1L})
                ),
                new SqlTestCase(
                        "select * from r5",
                        Collections.singletonList(new Object[]{"varargs: [arg1]"})
                ),
                new SqlTestCase(
                        "cache table r6 as call varargs_fun('arg1', 'arg2')",
                        Arrays.<Object[]>asList(new Object[]{"r6", 1L})
                ),
                new SqlTestCase(
                        "select * from r6",
                        Collections.singletonList(new Object[]{"varargs: [arg1, arg2]"})
                ),
                new SqlTestCase(
                        "cache table r7 as call varargs_fun('arg1', 'arg2', 'arg3')",
                        Arrays.<Object[]>asList(new Object[]{"r7", 1L})
                ),
                new SqlTestCase(
                        "select * from r7",
                        Collections.singletonList(new Object[]{"varargs: [arg1, arg2, arg3]"})
                ),

                new SqlTestCase(
                        "cache table r8 as call mixed_overload_fun(t1)",
                        Arrays.<Object[]>asList(new Object[]{"r8", 1L})
                ),
                new SqlTestCase(
                        "select * from r8",
                        Collections.singletonList(new Object[]{"table_only"})
                ),
                new SqlTestCase(
                        "cache table r9 as call mixed_overload_fun(t1, 'extra_arg')",
                        Arrays.<Object[]>asList(new Object[]{"r9", 1L})
                ),
                new SqlTestCase(
                        "select * from r9",
                        Collections.singletonList(new Object[]{"table_and_string: extra_arg"})
                ),
                new SqlTestCase(
                        "cache table r10 as call mixed_overload_fun('arg1', 'arg2')",
                        Arrays.<Object[]>asList(new Object[]{"r10", 1L})
                ),
                new SqlTestCase(
                        "select * from r10",
                        Collections.singletonList(new Object[]{"two_strings: arg1, arg2"})
                ),

                new SqlTestCase(
                        "cache table r11 as call varargs_with_table_fun(t1)",
                        Arrays.<Object[]>asList(new Object[]{"r11", 1L})
                ),
                new SqlTestCase(
                        "select * from r11",
                        Collections.singletonList(new Object[]{"table_with_varargs: []"})
                ),
                new SqlTestCase(
                        "cache table r12 as call varargs_with_table_fun(t1, 'arg1')",
                        Arrays.<Object[]>asList(new Object[]{"r12", 1L})
                ),
                new SqlTestCase(
                        "select * from r12",
                        Collections.singletonList(new Object[]{"table_with_varargs: [arg1]"})
                ),
                new SqlTestCase(
                        "cache table r13 as call varargs_with_table_fun(t1, 'arg1', 'arg2')",
                        Arrays.<Object[]>asList(new Object[]{"r13", 1L})
                ),
                new SqlTestCase(
                        "select * from r13",
                        Collections.singletonList(new Object[]{"table_with_varargs: [arg1, arg2]"})
                ),

                new SqlTestCase("set var1=value1"),
                new SqlTestCase("set var2=value2"),
                new SqlTestCase(
                        "cache table r14 as call overload_fun(get('var1'))",
                        Arrays.<Object[]>asList(new Object[]{"r14", 1L})
                ),
                new SqlTestCase(
                        "select * from r14",
                        Collections.singletonList(new Object[]{"one_arg: value1"})
                ),
                new SqlTestCase(
                        "cache table r15 as call overload_fun(get('var1'), get('var2'))",
                        Arrays.<Object[]>asList(new Object[]{"r15", 1L})
                ),
                new SqlTestCase(
                        "select * from r15",
                        Collections.singletonList(new Object[]{"two_args: value1, value2"})
                ),
                new SqlTestCase(
                        "cache table r16 as call varargs_fun(get('var1'), get('var2'))",
                        Arrays.<Object[]>asList(new Object[]{"r16", 1L})
                ),
                new SqlTestCase(
                        "select * from r16",
                        Collections.singletonList(new Object[]{"varargs: [value1, value2]"})
                ),

                new SqlTestCase(
                        "cache table r17 as call overload_fun(get_or_default('var1', 'default_val'))",
                        Arrays.<Object[]>asList(new Object[]{"r17", 1L})
                ),
                new SqlTestCase(
                        "select * from r17",
                        Collections.singletonList(new Object[]{"one_arg: value1"})
                ),
                new SqlTestCase(
                        "cache table r18 as call overload_fun(get_or_default('non_existing_var', 'fallback_value'))",
                        Arrays.<Object[]>asList(new Object[]{"r18", 1L})
                ),
                new SqlTestCase(
                        "select * from r18",
                        Collections.singletonList(new Object[]{"one_arg: fallback_value"})
                ),
                new SqlTestCase(
                        "cache table r19 as call varargs_fun(get_or_default('var1', 'default1'), get_or_default('unknown_var', 'default2'))",
                        Arrays.<Object[]>asList(new Object[]{"r19", 1L})
                ),
                new SqlTestCase(
                        "select * from r19",
                        Collections.singletonList(new Object[]{"varargs: [value1, default2]"})
                ),

                new SqlTestCase(
                        "cache table tmp1 as select 1 as id, 'a' as name",
                        Arrays.<Object[]>asList(new Object[]{"tmp1", 1L})
                ),
                new SqlTestCase(
                        "cache table tmp2 as select 2 as id, 'b' as name",
                        Arrays.<Object[]>asList(new Object[]{"tmp2", 1L})
                ),
                new SqlTestCase(
                        "cache table tmp3 as select 3 as id, 'c' as name",
                        Arrays.<Object[]>asList(new Object[]{"tmp3", 1L})
                ),

                new SqlTestCase(
                        "cache table r20 as call varargs_table_fun(t1)",
                        Arrays.<Object[]>asList(new Object[]{"r20", 1L})
                ),
                new SqlTestCase(
                        "select * from r20",
                        Collections.singletonList(new Object[]{"tables: 1"})
                ),
                new SqlTestCase(
                        "cache table r21 as call varargs_table_fun(t1, tmp1)",
                        Arrays.<Object[]>asList(new Object[]{"r21", 1L})
                ),
                new SqlTestCase(
                        "select * from r21",
                        Collections.singletonList(new Object[]{"tables: 2"})
                ),
                new SqlTestCase(
                        "cache table r22 as call varargs_table_fun(t1, tmp1, tmp2)",
                        Arrays.<Object[]>asList(new Object[]{"r22", 1L})
                ),
                new SqlTestCase(
                        "select * from r22",
                        Collections.singletonList(new Object[]{"tables: 3"})
                ),
                new SqlTestCase(
                        "cache table r23 as call varargs_table_fun(t1, tmp1, tmp2, tmp3)",
                        Arrays.<Object[]>asList(new Object[]{"r23", 1L})
                ),
                new SqlTestCase(
                        "select * from r23",
                        Collections.singletonList(new Object[]{"tables: 4"})
                ),

                new SqlTestCase(
                        "cache table r24 as call string_then_tables_fun('prefix_a', t1)",
                        Arrays.<Object[]>asList(new Object[]{"r24", 1L})
                ),
                new SqlTestCase(
                        "select * from r24",
                        Collections.singletonList(new Object[]{"prefix_a: 1 tables"})
                ),
                new SqlTestCase(
                        "cache table r25 as call string_then_tables_fun('prefix_b', t1, tmp1)",
                        Arrays.<Object[]>asList(new Object[]{"r25", 1L})
                ),
                new SqlTestCase(
                        "select * from r25",
                        Collections.singletonList(new Object[]{"prefix_b: 2 tables"})
                ),
                new SqlTestCase(
                        "cache table r26 as call string_then_tables_fun('prefix_c', t1, tmp1, tmp2)",
                        Arrays.<Object[]>asList(new Object[]{"r26", 1L})
                ),
                new SqlTestCase(
                        "select * from r26",
                        Collections.singletonList(new Object[]{"prefix_c: 3 tables"})
                ),

                new SqlTestCase(
                        "call ambiguous_fun('arg1')",
                        null,
                        new RuntimeException()
                )
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

    public static class TestOverloadFun {
        public CacheTable evaluate() {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"no_args"});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }

        public CacheTable evaluate(String arg) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"one_arg: " + arg});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }

        public CacheTable evaluate(String arg1, String arg2) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"two_args: " + arg1 + ", " + arg2});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }
    }

    public static class TestVarArgsFun {
        public CacheTable evaluate(String... args) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"varargs: " + Arrays.toString(args)});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }
    }

    public static class TestMixedOverloadFun {
        public CacheTable evaluate(CacheTable table) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"table_only"});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }

        public CacheTable evaluate(CacheTable table, String arg) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"table_and_string: " + arg});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }

        public CacheTable evaluate(String arg1, String arg2) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"two_strings: " + arg1 + ", " + arg2});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }
    }

    public static class TestVarArgsWithTableFun {
        public CacheTable evaluate(CacheTable table, String... args) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"table_with_varargs: " + Arrays.toString(args)});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }
    }

    public static class TestVarArgsTableFun {
        public CacheTable evaluate(CacheTable... tables) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"tables: " + (tables != null ? tables.length : 0)});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }
    }

    public static class TestStringThenTablesFun {
        public CacheTable evaluate(String prefix, CacheTable... tables) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{prefix + ": " + (tables != null ? tables.length : 0) + " tables"});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }
    }

    public static class TestAmbiguousFun {
        public CacheTable evaluate(String arg) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"single_arg: " + arg});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }

        public CacheTable evaluate(String... args) {
            List<Object[]> data = new ArrayList<>();
            data.add(new Object[]{"varargs: " + Arrays.toString(args)});
            return new CacheTable("output", Linq4j.asEnumerable(data), createStringField("result"));
        }
    }

    private static List<RelDataTypeField> createStringField(String name) {
        return Collections.singletonList(
                new RelDataTypeFieldImpl(name, 0, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR))
        );
    }
}
