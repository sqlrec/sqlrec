package com.sqlrec;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.Const;
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
        JavaFunctionUtils.registerTableFunction("default", "fun1", Integer.TYPE); // avoid find function in hms
        JavaFunctionUtils.registerTableFunction("default", "fun2", Integer.TYPE); // avoid find function in hms
        JavaFunctionUtils.registerTableFunction("default", "fun3", Integer.TYPE); // avoid find function in hms

        testSqlFunctionCompile(schema);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table t1 as SELECT * FROM myTable",
                        Arrays.<Object[]>asList(new Object[]{"t1", 3L})
                ),
                new SqlTestCase(
                        "call fun1(t1)",
                        Arrays.<Object[]>asList(
                                new Object[]{"Bob", 5L},
                                new Object[]{"Alice", 5L},
                                new Object[]{"Charlie", 5L})
                ),
                new SqlTestCase(
                        "call fun1(t1) async",
                        null
                ),
                new SqlTestCase(
                        "cache table t3 as call fun1(t1)",
                        Arrays.<Object[]>asList(new Object[]{"t3", 3L})
                ),
                new SqlTestCase(
                        "select * from t3",
                        Arrays.<Object[]>asList(
                                new Object[]{"Bob", 5L},
                                new Object[]{"Alice", 5L},
                                new Object[]{"Charlie", 5L})
                ),
                new SqlTestCase(
                        "call fun2(t1)",
                        null
                ),
                new SqlTestCase(
                        "call fun2(t1) async",
                        null
                ),
                new SqlTestCase(
                        "cache table t4 as call fun3(t1)",
                        Arrays.<Object[]>asList(new Object[]{"t4", 15L})
                ),
                new SqlTestCase(
                        "select * from t4",
                        Arrays.<Object[]>asList(
                                new Object[]{"Bob", 5L},
                                new Object[]{"Bob", 5L},
                                new Object[]{"Bob", 5L},
                                new Object[]{"Bob", 5L},
                                new Object[]{"Bob", 5L},
                                new Object[]{"Alice", 5L},
                                new Object[]{"Alice", 5L},
                                new Object[]{"Alice", 5L},
                                new Object[]{"Alice", 5L},
                                new Object[]{"Alice", 5L},
                                new Object[]{"Charlie", 5L},
                                new Object[]{"Charlie", 5L},
                                new Object[]{"Charlie", 5L},
                                new Object[]{"Charlie", 5L},
                                new Object[]{"Charlie", 5L})
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
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
                "return r");
        new CompileManager().compileSqlFunction("fun1", sqlList);

        List<String> sqlList2 = Arrays.asList(
                "create sql function fun2",
                "define input table input1(id int, name string)",
                "cache table t1 as SELECT NAME, count(*) as cnt FROM input1 where ID > 1 group by NAME",
                "return");
        new CompileManager().compileSqlFunction("fun2", sqlList2);

        List<String> sqlList3 = Arrays.asList(
                "create sql function fun3",
                "define input table input1(id int, name string)",
                "cache table t2 as call fun1(input1)",
                "cache table t3 as call fun1(input1)",
                "cache table t4 as call fun1(input1)",
                "cache table t5 as call fun1(input1)",
                "cache table t6 as call fun1(input1)",
                "cache table r as SELECT * from t2 union all SELECT * from t3 union all SELECT * from t4 union all SELECT * from t5 union all SELECT * from t6",
                "return r");
        new CompileManager().compileSqlFunction("fun3", sqlList3);
    }
}
