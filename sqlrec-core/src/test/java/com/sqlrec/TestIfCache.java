package com.sqlrec;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.utils.JavaFunctionUtils;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

public class TestIfCache {

    private CalciteSchema schema;

    @BeforeEach
    public void setup() {
        schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new MyTable());
            }
        });
    }

    @Test
    public void testIfConditionTrue() throws Exception {
        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "IF (SELECT true) THEN (cache table t1 as SELECT * FROM myTable) ELSE (cache table t1 as SELECT 1 as id, 'xx' as name)",
                        Arrays.<Object[]>asList(new Object[]{"t1", 3L})
                ),
                new SqlTestCase(
                        "select * from t1",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfConditionFalse() throws Exception {
        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "IF (SELECT false) THEN (cache table t1 as SELECT * FROM myTable) ELSE (cache table t1 as SELECT 1 as id, 'x' as name)",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                new SqlTestCase(
                        "select * from t1",
                        Arrays.<Object[]>asList(new Object[]{1, "x"})
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfConditionFalseNoElseWithExistingTable() throws Exception {
        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table t1 as SELECT * FROM myTable",
                        Arrays.<Object[]>asList(new Object[]{"t1", 3L})
                ),
                new SqlTestCase(
                        "IF (SELECT false) THEN (cache table t1 as SELECT 1 as id, 'x' as name)"
                ),
                new SqlTestCase(
                        "select * from t1",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfConditionFalseNoElseWithoutExistingTable() throws Exception {
        SqlTestCase sqlTestCase = new SqlTestCase(
                "IF (SELECT false) THEN (cache table t1 as SELECT 1 as id, 'x' as name)"
        );
        sqlTestCase.expectedException = new RuntimeException("must contain same table when no else statement in if sql");
        sqlTestCase.test(schema);
    }

    @Test
    public void testIfTimeinNoTimeout() throws Exception {
        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "IF TIMEIN (SELECT 10000) THEN (cache table t1 as SELECT * FROM myTable) ELSE (cache table t1 as SELECT 1 as id, 'x' as name)",
                        Arrays.<Object[]>asList(new Object[]{"t1", 3L})
                ),
                new SqlTestCase(
                        "select * from t1",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfTimeinZero() throws Exception {
        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "IF TIMEIN (SELECT 0) THEN (cache table t1 as SELECT * FROM myTable) ELSE (cache table t1 as SELECT 1 as id, 'x' as name)",
                        Arrays.<Object[]>asList(new Object[]{"t1", 3L})
                ),
                new SqlTestCase(
                        "select * from t1",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfTimeinNegative() throws Exception {
        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "IF TIMEIN (SELECT -1) THEN (cache table t1 as SELECT * FROM myTable) ELSE (cache table t1 as SELECT 1 as id, 'x' as name)",
                        Arrays.<Object[]>asList(new Object[]{"t1", 3L})
                ),
                new SqlTestCase(
                        "select * from t1",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfTimeinNoElseClause() throws Exception {
        SqlTestCase sqlTestCase = new SqlTestCase(
                "IF TIMEIN (SELECT 1000) THEN (cache table t1 as SELECT * FROM myTable)"
        );
        sqlTestCase.expectedException = new RuntimeException("must contain else clause when in timein mode");
        sqlTestCase.test(schema);
    }

    @Test
    public void testIfConditionMultipleRows() throws Exception {
        SqlTestCase sqlTestCase = new SqlTestCase(
                "IF (SELECT id FROM myTable) THEN (cache table t1 as SELECT 1 as id)"
        );
        sqlTestCase.expectedException = new RuntimeException("condition must return exactly one row");
        sqlTestCase.test(schema);
    }

    @Test
    public void testIfConditionMultipleColumns() throws Exception {
        SqlTestCase sqlTestCase = new SqlTestCase(
                "IF (SELECT id, name FROM myTable where id = 1) THEN (cache table t1 as SELECT 1 as id)"
        );
        sqlTestCase.expectedException = new RuntimeException("condition must return exactly one column");
        sqlTestCase.test(schema);
    }

    @Test
    public void testIfConditionNonBoolean() throws Exception {
        SqlTestCase sqlTestCase = new SqlTestCase(
                "IF (SELECT 123) THEN (cache table t1 as SELECT 1 as id)"
        );
        sqlTestCase.expectedException = new RuntimeException("condition must return a boolean value");
        sqlTestCase.test(schema);
    }

    @Test
    public void testIfTimeinNonNumeric() throws Exception {
        SqlTestCase sqlTestCase = new SqlTestCase(
                "IF TIMEIN (SELECT true) THEN (cache table t1 as SELECT 1 as id) ELSE (cache table t1 as SELECT 2 as id)"
        );
        sqlTestCase.expectedException = new RuntimeException("condition must return a numeric value for timein mode");
        sqlTestCase.test(schema);
    }

    @Test
    public void testIfTableSchemaMismatch() throws Exception {
        SqlTestCase sqlTestCase = new SqlTestCase(
                "IF (SELECT true) THEN (cache table t1 as SELECT 1 as id) ELSE (cache table t1 as SELECT 'x' as name)"
        );
        sqlTestCase.expectedException = new RuntimeException("desired field type not equal to given field type");
        sqlTestCase.test(schema);
    }

    @Test
    public void testIfTableNameMismatch() throws Exception {
        SqlTestCase sqlTestCase = new SqlTestCase(
                "IF (SELECT true) THEN (cache table t1 as SELECT 1 as id) ELSE (cache table t2 as SELECT 1 as id)"
        );
        sqlTestCase.expectedException = new RuntimeException("thenClause and elseClause must have the same table name");
        sqlTestCase.test(schema);
    }

    @Test
    public void testIfInSqlFunction() throws Exception {
        List<String> funSqlList = Arrays.asList(
                "create sql function if_test_func",
                "define input table input1(id int, name string)",
                "IF (SELECT count(*) > 1 FROM input1) THEN (cache table result1 as SELECT * FROM input1) ELSE (cache table result1 as SELECT 0 as id, 'empty' as name)",
                "return result1"
        );
        new CompileManager().compileSqlFunction("if_test_func", funSqlList);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table input_data as SELECT * FROM myTable",
                        Arrays.<Object[]>asList(new Object[]{"input_data", 3L})
                ),
                new SqlTestCase(
                        "call if_test_func(input_data)",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfInSqlFunctionWithEmptyInput() throws Exception {
        List<String> funSqlList = Arrays.asList(
                "create sql function if_test_func2",
                "define input table input1(id int, name string)",
                "IF (SELECT count(*) > 1 FROM input1) THEN (cache table result1 as SELECT * FROM input1) ELSE (cache table result1 as SELECT 0 as id, 'empty' as name)",
                "return result1"
        );
        new CompileManager().compileSqlFunction("if_test_func2", funSqlList);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table empty_table as SELECT 1 as id, 'test' as name",
                        Arrays.<Object[]>asList(new Object[]{"empty_table", 1L})
                ),
                new SqlTestCase(
                        "call if_test_func2(empty_table)",
                        Arrays.<Object[]>asList(new Object[]{0, "empty"})
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfWithExistingCacheTableDifferentSchema() throws Exception {
        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table t1 as SELECT 1 as id, 'x' as name",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                new SqlTestCase(
                        "IF (SELECT false) THEN (cache table t1 as SELECT * FROM myTable)"
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfWithConditionExpression() throws Exception {
        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table config as SELECT true as use_cache",
                        Arrays.<Object[]>asList(new Object[]{"config", 1L})
                ),
                new SqlTestCase(
                        "IF (SELECT use_cache FROM config) THEN (cache table t1 as SELECT * FROM myTable) ELSE (cache table t1 as SELECT 1 as id, 'x' as name)",
                        Arrays.<Object[]>asList(new Object[]{"t1", 3L})
                ),
                new SqlTestCase(
                        "select * from t1",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfTimeinWithFunction() throws Exception {
        List<String> slowFuncSql = Arrays.asList(
                "create sql function slow_func",
                "define input table input1(id int, name string)",
                "cache table result1 as SELECT * FROM input1",
                "return result1"
        );
        new CompileManager().compileSqlFunction("slow_func", slowFuncSql);

        List<String> ifFuncSql = Arrays.asList(
                "create sql function if_timein_func",
                "define input table input1(id int, name string)",
                "IF TIMEIN (SELECT 5000) THEN (" +
                        "cache table result1 as call slow_func(input1)" +
                        ") ELSE (" +
                        "cache table result1 as SELECT 0 as id, 'timeout' as name" +
                        ")",
                "return result1"
        );
        new CompileManager().compileSqlFunction("if_timein_func", ifFuncSql);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table input_data as SELECT * FROM myTable",
                        Arrays.<Object[]>asList(new Object[]{"input_data", 3L})
                ),
                new SqlTestCase(
                        "call if_timein_func(input_data)",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfTimeinWithJavaUDFTimeout() throws Exception {
        JavaFunctionUtils.registerTableFunction("default", "slow_func_udf", SlowFunction.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table input_data as SELECT * FROM myTable",
                        Arrays.<Object[]>asList(new Object[]{"input_data", 3L})
                ),
                new SqlTestCase(
                        "IF TIMEIN (SELECT 100) THEN (" +
                                "cache table result1 as call slow_func_udf(input_data)" +
                                ") ELSE (" +
                                "cache table result1 as SELECT 0 as id, 'timeout' as name" +
                                ")",
                        Arrays.<Object[]>asList(new Object[]{"result1", 1L})
                ),
                new SqlTestCase(
                        "select * from result1",
                        Arrays.<Object[]>asList(
                                new Object[]{0, "timeout"}
                        )
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    @Test
    public void testIfTimeinWithJavaUDFNoTimeout() throws Exception {
        JavaFunctionUtils.registerTableFunction("default", "fast_func_udf", SlowFunction.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table input_data as SELECT * FROM myTable",
                        Arrays.<Object[]>asList(new Object[]{"input_data", 3L})
                ),
                new SqlTestCase(
                        "IF TIMEIN (SELECT 5000) THEN (" +
                                "cache table result1 as call fast_func_udf(input_data)" +
                                ") ELSE (" +
                                "cache table result1 as SELECT 0 as id, 'timeout' as name" +
                                ")",
                        Arrays.<Object[]>asList(new Object[]{"result1", 3L})
                ),
                new SqlTestCase(
                        "select * from result1",
                        Arrays.asList(
                                new Object[]{1, "Alice"},
                                new Object[]{2, "Bob"},
                                new Object[]{3, "Charlie"}
                        )
                )
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {
        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, "Alice"},
                    {2, "Bob"},
                    {3, "Charlie"}
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("ID", SqlTypeName.INTEGER)
                    .add("NAME", SqlTypeName.CHAR)
                    .build();
        }
    }

    public static class SlowFunction {
        private static final long DEFAULT_DELAY_MS = 200;

        public CacheTable eval(CacheTable input) {
            try {
                Thread.sleep(DEFAULT_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            Enumerable<Object[]> enumerable = input.scan(null);
            List<Object[]> newData = new ArrayList<>();
            if (enumerable != null) {
                for (Object[] data : enumerable) {
                    newData.add(data);
                }
            }

            return new CacheTable("output", Linq4j.asEnumerable(newData), input.getDataFields());
        }
    }
}
