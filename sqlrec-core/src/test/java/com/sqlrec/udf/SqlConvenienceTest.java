package com.sqlrec.udf;

import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.udf.config.FunctionConfigs;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlConvenienceTest {
    private CalciteSchema root;
    private ExecuteContextImpl context;

    @BeforeEach
    void setUp() throws Exception {
        root = CalciteSchema.createRootSchema(false);
        var schema = root.add("default", new AbstractSchema());
        context = new ExecuteContextImpl(root);
        CalciteSchemaFactory.setGlobalSchema(root);
        JavaFunctionUtils.registerTableFunction("default", "typed_arguments_test", TypedArguments.class);
        JavaFunctionUtils.registerTableFunction("default", "numeric_varargs_test", NumericVarargs.class);
        for (String name : List.of("array_contains", "array_contains_all", "array_contains_any")) {
            UdfManager.addFunction(schema, name, FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS.get(name));
        }
        var fields = List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        root.add("items", new CacheTable("items", Linq4j.asEnumerable(
                List.of(new Object[]{1}, new Object[]{2}, new Object[]{3})), fields));
        root.add("excluded", new CacheTable("excluded", Linq4j.singletonEnumerable(new Object[]{2}), fields));
    }

    @AfterEach
    void cleanUp() {
        CalciteSchemaFactory.setGlobalSchema(null);
    }

    @Test
    void arrayPredicatesCompareNumbersAcrossSqlTypes() throws Exception {
        assertArrayEquals(new Object[]{true, true, true, false}, execute("SELECT "
                + "array_contains(ARRAY[CAST(1 AS BIGINT)], 1), "
                + "array_contains_all(ARRAY[CAST(1 AS BIGINT)], ARRAY[1]), "
                + "array_contains_any(ARRAY[CAST(1 AS BIGINT)], ARRAY[1]), "
                + "array_contains(ARRAY[CAST(9007199254740993 AS BIGINT)], "
                + "CAST(9007199254740992 AS DOUBLE))").get(0));
    }

    @Test
    void callSelectsNumericAndBooleanOverloads() throws Exception {
        for (String[] testCase : new String[][]{
                {"1", "int:1"}, {"-1", "int:-1"}, {"2147483648", "long:2147483648"},
                {"0.5", "double:0.5"}, {"TRUE", "boolean:true"}, {"'1'", "string:1"}}) {
            assertArrayEquals(new Object[]{testCase[1]},
                    execute("CALL typed_arguments_test(" + testCase[0] + ")").get(0));
        }
    }

    @Test
    void getDefaultsDetermineCallArgumentTypes() throws Exception {
        context.setVariable("number", "7");
        context.setVariable("flag", "TrUe");
        assertArrayEquals(new Object[]{"int:7"}, execute("CALL typed_arguments_test(GET('number', 0))").get(0));
        assertArrayEquals(new Object[]{"int:17"}, execute("CALL typed_arguments_test(GET('missing', 17))").get(0));
        assertArrayEquals(new Object[]{"boolean:true"}, execute("CALL typed_arguments_test(GET('flag', false))").get(0));
        context.setVariable("number", "invalid");
        RuntimeException failure = assertThrows(RuntimeException.class,
                () -> execute("CALL typed_arguments_test(GET('number', 0))"));
        assertInstanceOf(NumberFormatException.class, failure.getCause());
    }

    @Test
    void supportsContextInjectionNumericVarargsAndWidening() throws Exception {
        assertArrayEquals(new Object[]{"2:[1.0, 0.5]"},
                execute("CALL numeric_varargs_test(2, 1, 0.5)").get(0));
        assertNull(execute("CALL sleep(0)"));
        assertNull(execute("CALL sleep(GET('missing_sleep', 0))"));
        assertThrows(RuntimeException.class, () -> execute("CALL sleep('0')"));
        assertThrows(RuntimeException.class, () -> execute("CALL truncate_table(items, 0.5)"));
    }

    @Test
    void addColInfersColumnTypesFromConstants() throws Exception {
        Object[][] cases = {
                {"50", SqlTypeName.INTEGER, 50}, {"2147483648", SqlTypeName.BIGINT, 2147483648L},
                {"0.5", SqlTypeName.DOUBLE, 0.5d}, {"false", SqlTypeName.BOOLEAN, false},
                {"'0.5'", SqlTypeName.VARCHAR, "0.5"}
        };
        for (Object[] testCase : cases) {
            String table = "typed_" + testCase[1];
            execute("CACHE TABLE " + table + " AS CALL add_col(items, 'extra', " + testCase[0] + ")");
            var plan = NormalSqlCompiler.getNormalSqlBindable("SELECT extra FROM " + table, root, "default");
            assertEquals(testCase[1], plan.getReturnDataFields().get(0).getType().getSqlTypeName());
            var rows = plan.bind(root, context).toList();
            assertEquals(3, rows.size());
            for (Object[] row : rows) assertArrayEquals(new Object[]{testCase[2]}, row);
        }
    }

    @Test
    void addColAcceptsTypedGetArguments() throws Exception {
        context.setVariable("boost", "1.25");
        context.setVariable("enabled", "true");
        for (Object[] testCase : new Object[][]{
                {"GET('boost', 0.5)", 1.25d}, {"GET('enabled', false)", true}, {"GET('missing', 10)", 10}}) {
            var rows = execute("CALL add_col(items, 'extra', " + testCase[0] + ")");
            assertEquals(3, rows.size());
            for (int i = 0; i < rows.size(); i++) {
                assertArrayEquals(new Object[]{i + 1, testCase[1]}, rows.get(i));
            }
        }
    }

    @Test
    void selfDedupRunsThroughCallAndPreservesInputOrder() throws Exception {
        root.add("duplicates", new CacheTable("duplicates", Linq4j.asEnumerable(
                List.of(new Object[]{2}, new Object[]{1}, new Object[]{2}, new Object[]{3})),
                List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER))));
        var rows = execute("CALL dedup(duplicates, 'ID')");
        assertEquals(List.of(2, 1, 3), rows.stream().map(row -> row[0]).toList());
    }

    @Test
    void mergeAndRuleFunctionsAcceptNumericLimits() throws Exception {
        context.setVariable("merge_limit", "2");
        var rows = execute("CALL weighted_merge('ID', '1,1', GET('merge_limit', 3), items, excluded)");
        assertEquals(List.of(1, 2), rows.stream().map(row -> row[0]).toList());
        execute("CACHE TABLE rules AS SELECT 3 AS window_size, 1 AS window_start, 1 AS window_num, "
                + "'id' AS diversity_column, CAST(NULL AS VARCHAR) AS diversity_value, "
                + "'<' AS op, 1 AS diversity_num, 1.0 AS weight LIMIT 0");
        rows = execute("CALL rule_diversity(items, rules, 2)");
        assertEquals(List.of(1, 2), rows.stream().map(row -> row[0]).toList());
    }

    @Test
    void tableFunctionShorthandsExecuteThroughCall() throws Exception {
        for (String sql : List.of("CALL truncate_table(items, 2)", "CALL truncate_table(items, 0, 2)")) {
            var rows = execute(sql);
            assertEquals(List.of(1, 2), rows.stream().map(row -> row[0]).toList());
        }
        var rows = execute("CALL dedup(items, excluded, 'id')");
        assertEquals(List.of(1, 3), rows.stream().map(row -> row[0]).toList());
    }

    @Test
    void jsonTablePreservesBigintThroughSqlAndWidensMixedNumbers() throws Exception {
        execute("CACHE TABLE json_items AS CALL json_to_table("
                + "'[{\"id\":9007199254740993,\"ids\":[9007199254740993],\"score\":1},"
                + "{\"id\":9223372036854775807,\"ids\":[9223372036854775807],\"score\":0.5}]')");
        var plan = NormalSqlCompiler.getNormalSqlBindable("SELECT id, ids, score FROM json_items", root, "default");
        var fields = plan.getReturnDataFields();
        assertEquals(SqlTypeName.BIGINT, fields.get(0).getType().getSqlTypeName());
        assertEquals(SqlTypeName.BIGINT, fields.get(1).getType().getComponentType().getSqlTypeName());
        assertEquals(SqlTypeName.DOUBLE, fields.get(2).getType().getSqlTypeName());
        var rows = plan.bind(root, context).toList();
        assertArrayEquals(new Object[]{9007199254740993L, List.of(9007199254740993L), 1.0d}, rows.get(0));
        assertArrayEquals(new Object[]{Long.MAX_VALUE, List.of(Long.MAX_VALUE), 0.5d}, rows.get(1));
        assertArrayEquals(new Object[]{9007199254740993L}, execute(
                "SELECT id FROM json_items WHERE id = CAST(9007199254740993 AS BIGINT)").get(0));
    }

    private List<Object[]> execute(String sql) throws Exception {
        var plan = new CompileManager().compileSql(CompileManager.parseSql(sql), root, "default", sql);
        var result = plan.bind(root, context);
        return result == null ? null : result.toList();
    }

    private static CacheTable result(String value) {
        return new CacheTable("output", Linq4j.singletonEnumerable(new Object[]{value}),
                List.of(DataTypeUtils.getRelDataTypeField("value", 0, SqlTypeName.VARCHAR)));
    }

    public static class TypedArguments {
        public CacheTable evaluate(int value) { return result("int:" + value); }
        public CacheTable evaluate(long value) { return result("long:" + value); }
        public CacheTable evaluate(double value) { return result("double:" + value); }
        public CacheTable evaluate(boolean value) { return result("boolean:" + value); }
        public CacheTable evaluate(String value) { return result("string:" + value); }
    }

    public static class NumericVarargs {
        public CacheTable evaluate(ReadonlyContext context, Integer count, Double... values) {
            assertNotNull(context);
            return result(count + ":" + Arrays.toString(values));
        }
    }
}
