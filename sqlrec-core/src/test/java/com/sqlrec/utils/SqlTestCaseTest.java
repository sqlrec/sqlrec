package com.sqlrec.utils;

import com.sqlrec.common.config.Consts;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.BindableInterface;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SqlTestCaseTest {
    @Test
    void rejectsMissingExceptionsAndWrongExceptionTypes() {
        SqlTestCase missing = failingCase(new RuntimeException("expected failure"));
        assertTrue(assertThrows(AssertionError.class,
                () -> execute(missing, Linq4j.emptyEnumerable())).getMessage().contains("Expected SQL execution to fail"));

        IllegalStateException actual = new IllegalStateException("invalid input");
        AssertionError mismatch = assertThrows(AssertionError.class,
                () -> execute(failingCase(new IllegalArgumentException()), delayedFailure(actual, new AtomicBoolean())));
        assertSame(actual, mismatch.getCause());
        assertTrue(mismatch.getMessage().contains(IllegalArgumentException.class.getName()));
    }

    @Test
    void rejectsWrongMessagesEvenWhenTheExceptionTypeMatches() {
        RuntimeException actual = new RuntimeException("returned false");
        AssertionError mismatch = assertThrows(AssertionError.class,
                () -> execute(failingCase(new RuntimeException("no rows")),
                        delayedFailure(actual, new AtomicBoolean())));
        assertTrue(mismatch.getMessage().contains("no rows"));
        assertSame(actual, mismatch.getCause());
    }

    @Test
    void checksFailuresDuringRowConsumptionAndClosesTheEnumerator() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        execute(failingCase(new RuntimeException("invalid feature")),
                delayedFailure(new RuntimeException("query failed",
                        new IllegalArgumentException("invalid feature: price")), closed));
        assertTrue(closed.get());
    }

    @Test
    void checksTheRootCauseTypeAndMessage() throws Exception {
        RuntimeException actual = new RuntimeException("query failed",
                new IllegalArgumentException("invalid feature: price"));
        execute(failingCase(new RuntimeException("query failed"))
                        .setExpectedRootCause(new IllegalArgumentException("price")),
                delayedFailure(actual, new AtomicBoolean()));

        for (Throwable wrongRoot : List.of(new IllegalStateException("price"),
                new IllegalArgumentException("missing column"))) {
            assertThrows(AssertionError.class, () -> execute(
                    failingCase(new RuntimeException()).setExpectedRootCause(wrongRoot),
                    delayedFailure(actual, new AtomicBoolean())));
        }
        // A matching wrapper message cannot stand in for the root cause's message.
        assertThrows(AssertionError.class, () -> execute(
                new SqlTestCase("SELECT 1").setExpectedRootCause(new IllegalArgumentException("query failed")),
                delayedFailure(actual, new AtomicBoolean())));
    }

    @Test
    void reportsUnexpectedLazyFailuresWithTheirOriginalCause() {
        RuntimeException actual = new RuntimeException("read failed");
        AssertionError failure = assertThrows(AssertionError.class, () -> execute(new SqlTestCase("SELECT 1"),
                delayedFailure(actual, new AtomicBoolean())));
        assertSame(actual, failure.getCause());
        assertTrue(failure.getMessage().contains("SELECT 1"));
    }

    @Test
    void stillChecksReturnedRows() throws Exception {
        List<Object[]> rows = List.<Object[]>of(new Object[]{1});
        execute(new SqlTestCase("SELECT 1", rows), Linq4j.asEnumerable(rows));
        assertThrows(AssertionError.class, () -> execute(
                new SqlTestCase("SELECT 1", List.<Object[]>of(new Object[]{2})), Linq4j.asEnumerable(rows)));
    }

    @Test
    void checksPhysicalPlansEvenWhenThereIsNoLogicalPlan() {
        SqlTestCase testCase = new SqlTestCase("SELECT 1");
        testCase.expectedPhysicalPlan = "expected plan";
        AssertionError failure = assertThrows(AssertionError.class,
                () -> execute(testCase, Linq4j.emptyEnumerable()));
        assertTrue(failure.getMessage().contains("Physical plan mismatch"));
        assertTrue(failure.getMessage().contains("Actual:\nnull"));
    }

    @Test
    void planAndExceptionChecksWorkWithJvmAssertionsDisabled() throws Exception {
        Class<?> checker = new ClassLoader(SqlTestCase.class.getClassLoader()) {
            Class<?> loadWithoutAssertions() throws Exception {
                setClassAssertionStatus(SqlTestCase.class.getName(), false);
                try (InputStream input = SqlTestCase.class.getResourceAsStream("SqlTestCase.class")) {
                    byte[] bytes = input.readAllBytes();
                    return defineClass(SqlTestCase.class.getName(), bytes, 0, bytes.length);
                }
            }
        }.loadWithoutAssertions();
        assertFalse(checker.desiredAssertionStatus());
        for (String field : List.of("expectedLogicalPlan", "expectedPhysicalPlan", "expectedJavaExpression")) {
            Object testCase = checker.getConstructor(String.class).newInstance("SELECT 1");
            checker.getField(field).set(testCase, "expected");
            InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                    () -> checker.getMethod("checkBindable", BindableInterface.class)
                            .invoke(testCase, mock(BindableInterface.class)));
            assertInstanceOf(AssertionError.class, failure.getCause());
        }
        Object testCase = checker.getConstructor(String.class, List.class, Exception.class)
                .newInstance("SELECT 1", null, new RuntimeException());
        InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                () -> checker.getMethod("test", CalciteSchema.class)
                        .invoke(testCase, CalciteSchema.createRootSchema(false)));
        assertInstanceOf(AssertionError.class, failure.getCause());
        assertTrue(failure.getCause().getMessage().contains("Expected SQL execution to fail"));
    }

    private SqlTestCase failingCase(Exception expected) {
        return new SqlTestCase("SELECT 1", null, expected).setDebugOutput(false);
    }

    private void execute(SqlTestCase testCase, Enumerable<Object[]> rows) throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        BindableInterface plan = mock(BindableInterface.class);
        when(plan.bind(same(schema), any())).thenReturn(rows);
        try (var construction = mockConstruction(CompileManager.class, (compiler, context) ->
                when(compiler.compileSql(any(), same(schema), eq(Consts.DEFAULT_SCHEMA_NAME), eq(testCase.sql)))
                        .thenReturn(plan))) {
            testCase.test(schema);
        }
    }

    private Enumerable<Object[]> delayedFailure(RuntimeException failure, AtomicBoolean closed) {
        return new AbstractEnumerable<>() {
            @Override public Enumerator<Object[]> enumerator() {
                return new Enumerator<>() {
                    @Override public Object[] current() { throw new IllegalStateException("No current row"); }
                    @Override public boolean moveNext() { throw failure; }
                    @Override public void reset() { throw new UnsupportedOperationException(); }
                    @Override public void close() { closed.set(true); }
                };
            }
        };
    }
}
