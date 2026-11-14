package com.sqlrec.udf;

import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.compiler.CompileManager;
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
    void tableFunctionShorthandsExecuteThroughCall() throws Exception {
        for (String sql : List.of("CALL truncate_table(items, 2)", "CALL truncate_table(items, 0, 2)")) {
            var rows = execute(sql);
            assertEquals(List.of(1, 2), rows.stream().map(row -> row[0]).toList());
        }
        var rows = execute("CALL dedup(items, excluded, 'id')");
        assertEquals(List.of(1, 3), rows.stream().map(row -> row[0]).toList());
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
