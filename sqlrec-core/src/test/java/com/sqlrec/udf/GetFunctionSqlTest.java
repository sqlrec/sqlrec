package com.sqlrec.udf;

import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.udf.scalar.GetFunction;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GetFunctionSqlTest {
    private CalciteSchema root;
    private ExecuteContextImpl context;

    @BeforeEach
    void setUp() throws Exception {
        root = CalciteSchema.createRootSchema(false);
        CalciteSchema schema = root.add("default", new AbstractSchema());
        UdfManager.addFunction(schema, "get", GetFunction.class.getName());
        context = new ExecuteContextImpl(root);
        context.setVariable("number", "123");
        context.setVariable("large", "2147483648");
        context.setVariable("rate", "1.25");
        context.setVariable("flag", "TrUe");
        context.setVariable("text", "value");
    }

    @Test
    void convertsPresentVariablesToDefaultTypes() throws Exception {
        assertResult("SELECT `get`('number', 0), `get`('large', CAST(0 AS BIGINT)), "
                        + "`get`('rate', CAST(0 AS DOUBLE)), `get`('flag', false), "
                        + "`get`('text', 'fallback'), `get`('number')",
                new Object[]{123, 2147483648L, 1.25d, true, "value", "123"},
                SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.DOUBLE,
                SqlTypeName.BOOLEAN, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
    }

    @Test
    void returnsTypedDefaultsWhenVariableIsMissing() throws Exception {
        assertResult("SELECT `get`('missing', 50), `get`('missing', CAST(1000 AS BIGINT)), "
                        + "`get`('missing', CAST(0.5 AS DOUBLE)), `get`('missing', false), "
                        + "`get`('missing', 'fallback'), `get`('missing')",
                new Object[]{50, 1000L, 0.5d, false, "fallback", null},
                SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.DOUBLE,
                SqlTypeName.BOOLEAN, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
    }

    @Test
    void typedNullDefaultsDetermineNullableReturnTypes() throws Exception {
        Object[][] cases = {
                {SqlTypeName.INTEGER, "number", 123},
                {SqlTypeName.BIGINT, "large", 2147483648L},
                {SqlTypeName.DOUBLE, "rate", 1.25d},
                {SqlTypeName.BOOLEAN, "flag", true},
                {SqlTypeName.VARCHAR, "text", "value"}
        };
        for (Object[] testCase : cases) {
            SqlTypeName type = (SqlTypeName) testCase[0];
            String defaultValue = "CAST(NULL AS " + type + ")";
            assertResult("SELECT `get`('" + testCase[1] + "', " + defaultValue + "), "
                            + "`get`('missing', " + defaultValue + ")",
                    new Object[]{testCase[2], null}, type, type);
        }
    }

    @Test
    void invalidValuesAndIntegerOverflowFailInsteadOfUsingDefaults() throws Exception {
        for (String defaultValue : List.of("0", "CAST(0 AS BIGINT)", "CAST(0 AS DOUBLE)", "false")) {
            var plan = NormalSqlCompiler.getNormalSqlBindable(
                    "SELECT `get`('invalid', " + defaultValue + ")", root, "default");
            for (String value : List.of("invalid", "")) {
                context.setVariable("invalid", value);
                assertThrows(IllegalArgumentException.class, () -> plan.bind(root, context).toList());
            }
        }
        for (String[] testCase : new String[][]{
                {"0", "2147483648"}, {"CAST(0 AS BIGINT)", "9223372036854775808"}}) {
            context.setVariable("invalid", testCase[1]);
            var plan = NormalSqlCompiler.getNormalSqlBindable(
                    "SELECT `get`('invalid', " + testCase[0] + ")", root, "default");
            assertThrows(NumberFormatException.class, () -> plan.bind(root, context).toList());
        }
    }

    @Test
    void supportsTypedResultsInExpressionsAndReadsEachExecutionContext() throws Exception {
        var plan = NormalSqlCompiler.getNormalSqlBindable(
                "SELECT `get`('number', 0) + 1 FROM (VALUES (0)) AS t(n) "
                        + "WHERE `get`('flag', false)", root, "default");
        assertArrayEquals(new Object[]{124}, plan.bind(root, context).toList().get(0));
        context.setVariable("number", "456");
        assertArrayEquals(new Object[]{457}, plan.bind(root, context).toList().get(0));
        context.setVariable("flag", "FaLsE");
        assertTrue(plan.bind(root, context).toList().isEmpty());
    }

    private void assertResult(String sql, Object[] expected, SqlTypeName... types) throws Exception {
        var plan = NormalSqlCompiler.getNormalSqlBindable(sql, root, "default");
        assertEquals(List.of(types), plan.getReturnDataFields().stream()
                .map(field -> field.getType().getSqlTypeName()).toList());
        assertTrue(plan.getReturnDataFields().stream().allMatch(field -> field.getType().isNullable()));
        var rows = plan.bind(root, context).toList();
        assertEquals(1, rows.size());
        assertArrayEquals(expected, rows.get(0));
    }
}
