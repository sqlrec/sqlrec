package com.sqlrec.utils;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.sql.parser.SqlGetVariable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JavaFunctionMethodMatcherTest {
    private static final SqlParserPos POS = SqlParserPos.ZERO;

    @Test
    void selectsExactPrimitiveAndBoxedTypes() throws Exception {
        assertMatch(PrimitiveOverloads.class, List.of(number("1")), int.class);
        assertMatch(PrimitiveOverloads.class, List.of(number("2147483648")), long.class);
        assertMatch(PrimitiveOverloads.class, List.of(number("0.5")), double.class);
        assertMatch(PrimitiveOverloads.class, List.of(SqlLiteral.createBoolean(true, POS)), boolean.class);
        assertMatch(PrimitiveOverloads.class, List.of(text("value")), String.class);
        assertMatch(BoxedArguments.class, List.of(number("1"), number("2147483648"),
                number("0.5"), SqlLiteral.createBoolean(false, POS)),
                Integer.class, Long.class, Double.class, Boolean.class);
    }

    @Test
    void classifiesSignedIntegerBoundariesAndApproximateNumbers() throws Exception {
        for (String value : List.of("-2147483648", "2147483647")) {
            assertMatch(PrimitiveOverloads.class, List.of(number(value)), int.class);
        }
        for (String value : List.of("-2147483649", "2147483648", "-9223372036854775808", "9223372036854775807")) {
            assertMatch(PrimitiveOverloads.class, List.of(number(value)), long.class);
        }
        assertMatch(PrimitiveOverloads.class, List.of(SqlLiteral.createApproxNumeric("1e2", POS)), double.class);
    }

    @Test
    void prefersLongWideningOverDoubleAndAllowsDoubleWhenNeeded() throws Exception {
        assertMatch(WideningOverloads.class, List.of(number("1")), long.class);
        assertMatch(WideningOverloads.class, List.of(number("2147483648")), long.class);
        assertMatch(WideningOverloads.class, List.of(number("0.5")), double.class);
        assertMatch(DoubleOnly.class, List.of(number("1")), double.class);
        assertMatch(DoubleOnly.class, List.of(number("2147483648")), double.class);
    }

    @Test
    void rejectsNarrowingAndStringOrBooleanCoercion() {
        for (SqlNode input : List.of(number("2147483648"), number("0.5"), text("1"),
                SqlLiteral.createBoolean(true, POS))) {
            assertThrows(RuntimeException.class,
                    () -> JavaFunctionMethodMatcher.selectEvalMethod(IntOnly.class, List.of(input)));
        }
    }

    @Test
    void matchesTableIdentifiersSeparatelyFromStrings() throws Exception {
        assertMatch(TableAndString.class, List.of(new SqlIdentifier("items", POS)), CacheTable.class);
        assertMatch(TableAndString.class, List.of(text("items")), String.class);
        assertThrows(RuntimeException.class,
                () -> JavaFunctionMethodMatcher.selectEvalMethod(TableAndString.class, List.of(number("1"))));
    }

    @Test
    void usesGetDefaultTypesAndTreatsGetWithoutDefaultAsString() throws Exception {
        assertMatch(PrimitiveOverloads.class,
                List.of(new SqlGetVariable(POS, text("key"))), String.class);
        Object[][] cases = {
                {text("fallback"), String.class}, {number("1"), int.class},
                {number("2147483648"), long.class}, {number("0.5"), double.class},
                {SqlLiteral.createBoolean(false, POS), boolean.class}
        };
        for (Object[] testCase : cases) {
            var variable = new SqlGetVariable(POS, text("key"), (SqlLiteral) testCase[0]);
            assertMatch(PrimitiveOverloads.class, List.of(variable), (Class<?>) testCase[1]);
        }
    }

    @Test
    void skipsInjectedContextsAtAnyParameterPosition() throws Exception {
        assertMatch(WithContexts.class, List.of(text("value"), new SqlIdentifier("items", POS), number("1")),
                ExecuteContext.class, String.class, ReadonlyContext.class, CacheTable.class, Integer.class);
        assertMatch(ContextOnly.class, List.of(), ExecuteContext.class, ReadonlyContext.class);
    }

    @Test
    void matchesEmptyAndPopulatedVarargsAndChecksEveryElement() throws Exception {
        assertMatch(NumericVarargs.class, List.of(text("prefix")), ReadonlyContext.class, String.class, double[].class);
        assertMatch(NumericVarargs.class, List.of(text("prefix"), number("1"), number("0.5")),
                ReadonlyContext.class, String.class, double[].class);
        assertThrows(RuntimeException.class,
                () -> JavaFunctionMethodMatcher.selectEvalMethod(NumericVarargs.class, List.of()));
        assertThrows(RuntimeException.class,
                () -> JavaFunctionMethodMatcher.selectEvalMethod(NumericVarargs.class,
                        List.of(text("prefix"), number("1"), text("invalid"))));
        assertMatch(TableVarargs.class, List.of(new SqlIdentifier("a", POS), new SqlIdentifier("b", POS)),
                CacheTable[].class);
    }

    @Test
    void rejectsWrongArgumentCountsAndMissingEvaluateMethods() {
        for (List<SqlNode> inputs : List.of(List.<SqlNode>of(), List.<SqlNode>of(number("1"), number("2")))) {
            RuntimeException error = assertThrows(RuntimeException.class,
                    () -> JavaFunctionMethodMatcher.selectEvalMethod(IntOnly.class, inputs));
            assertTrue(error.getMessage().contains("found 0 evaluate method(s)"));
        }
        RuntimeException error = assertThrows(RuntimeException.class,
                () -> JavaFunctionMethodMatcher.selectEvalMethod(NoPublicEvaluate.class, List.of(text("value"))));
        assertTrue(error.getMessage().contains("at least one evaluate method"));
    }

    @Test
    void rejectsEquallyGoodOverloadsInsteadOfDependingOnReflectionOrder() {
        RuntimeException error = assertThrows(RuntimeException.class,
                () -> JavaFunctionMethodMatcher.selectEvalMethod(AmbiguousOverloads.class, List.of(text("value"))));
        assertTrue(error.getMessage().contains("found 2 evaluate method(s)"));
    }

    @Test
    void supportsInheritedMethodsAndIgnoresBridgeMethods() throws Exception {
        assertMatch(InheritedFunction.class, List.of(number("1")), int.class);
        var method = JavaFunctionMethodMatcher.selectEvalMethod(StringResult.class, List.of(text("value")));
        assertFalse(method.isBridge());
        assertEquals(String.class, method.getReturnType());
    }

    @Test
    void selectsWithoutConstructingOrExecutingTheFunction() throws Exception {
        assertMatch(ClassOnlyFunction.class, List.of(number("1")), int.class);
    }

    private static void assertMatch(Class<?> functionClass, List<SqlNode> inputs, Class<?>... parameters) throws Exception {
        assertEquals(functionClass.getMethod("evaluate", parameters),
                JavaFunctionMethodMatcher.selectEvalMethod(functionClass, inputs));
    }

    private static SqlLiteral number(String value) {
        return SqlLiteral.createExactNumeric(value, POS);
    }

    private static SqlLiteral text(String value) {
        return SqlLiteral.createCharString(value, POS);
    }

    public static class PrimitiveOverloads {
        public void evaluate(int value) { }
        public void evaluate(long value) { }
        public void evaluate(double value) { }
        public void evaluate(boolean value) { }
        public void evaluate(String value) { }
    }

    public static class BoxedArguments {
        public void evaluate(Integer count, Long large, Double ratio, Boolean enabled) { }
    }

    public static class WideningOverloads {
        public void evaluate(long value) { }
        public void evaluate(double value) { }
    }

    public static class DoubleOnly {
        public void evaluate(double value) { }
    }

    public static class IntOnly {
        public void evaluate(int value) { }
    }

    public static class TableAndString {
        public void evaluate(CacheTable table) { }
        public void evaluate(String value) { }
    }

    public static class WithContexts {
        public void evaluate(ExecuteContext context, String value, ReadonlyContext readonly, CacheTable table, Integer count) { }
    }

    public static class ContextOnly {
        public void evaluate(ExecuteContext context, ReadonlyContext readonly) { }
    }

    public static class NumericVarargs {
        public void evaluate(ReadonlyContext context, String prefix, double... values) { }
    }

    public static class TableVarargs {
        public void evaluate(CacheTable... tables) { }
    }

    public static class NoPublicEvaluate {
        private void evaluate(String value) { }
    }

    public static class AmbiguousOverloads {
        public void evaluate(String value) { }
        public void evaluate(String... values) { }
    }

    public static class InheritedFunction extends IntOnly { }

    public static class GenericResult<T> {
        public T evaluate(String value) { return null; }
    }

    public static class StringResult extends GenericResult<String> {
        @Override
        public String evaluate(String value) { return value; }
    }

    public static class ClassOnlyFunction {
        public ClassOnlyFunction(int value) { throw new AssertionError("Must not construct"); }
        public void evaluate(int value) { throw new AssertionError("Must not execute"); }
    }
}
