package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IfBindableCancelTest {

    private abstract static class TestBindable extends BindableInterface {
        @Override
        public List<RelDataTypeField> getReturnDataFields() {
            return Collections.singletonList(
                    DataTypeUtils.getRelDataTypeField("col1", 0, SqlTypeName.VARCHAR)
            );
        }

        @Override
        public boolean isParallelizable() {
            return true;
        }

        @Override
        public Set<String> getReadTables() {
            return Collections.emptySet();
        }

        @Override
        public Set<String> getWriteTables() {
            return Collections.emptySet();
        }
    }

    /**
     * Simulates a long-running task: ignores thread interrupts and keeps polling the context cancellation flag.
     */
    private static class PollingBindable extends TestBindable {
        final AtomicBoolean cancelledObserved = new AtomicBoolean(false);
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicBoolean executed = new AtomicBoolean(false);

        @Override
        public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
            executed.set(true);
            try {
                while (!context.isCancelled()) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        // ignore interrupts, keep polling the cancellation flag
                    }
                }
                cancelledObserved.set(true);
                throw new RuntimeException("cancelled observed");
            } finally {
                finished.countDown();
            }
        }
    }

    private static CalciteBindable condition(Object... rows) {
        List<Object[]> rowList = new ArrayList<>();
        for (Object row : rows) {
            rowList.add(new Object[]{row});
        }
        return new CalciteBindable(
                new HashMap<>(),
                dataContext -> Linq4j.asEnumerable(rowList),
                null, null, null, null, null
        );
    }

    private static CacheTableBindable cacheClause(String tableName, BindableInterface inner) {
        return new CacheTableBindable(tableName, inner);
    }

    private static TestBindable returningRow() {
        return new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[]{"x"});
                return Linq4j.asEnumerable(rows);
            }
        };
    }

    // ==================== timein mode: timeout/failure cancels the then subtree ====================

    @Test
    public void testTimeinTimeoutFallsBackToElseAndCancelsThenSubtree() throws Exception {
        PollingBindable thenInner = new PollingBindable();
        AtomicBoolean elseExecuted = new AtomicBoolean(false);
        TestBindable elseInner = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                elseExecuted.set(true);
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[]{"x"});
                return Linq4j.asEnumerable(rows);
            }
        };

        IfBindable ifBindable = new IfBindable(
                condition(100L),
                cacheClause("t", thenInner),
                cacheClause("t", elseInner),
                true
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        long startTime = System.currentTimeMillis();
        Enumerable<Object[]> result = ifBindable.bind(schema, context);
        long duration = System.currentTimeMillis() - startTime;

        assertTrue(duration < 5000, "should fall back quickly, took " + duration + "ms");
        assertTrue(elseExecuted.get(), "else clause should execute after timeout");
        // the result is the else branch CacheTableBindable's [table_name, count] row
        List<Object[]> rows = new ArrayList<>();
        result.forEach(rows::add);
        assertEquals("t", rows.get(0)[0]);

        // the then subtree should observe the cancellation and exit promptly
        assertTrue(thenInner.finished.await(5, TimeUnit.SECONDS),
                "then subtree task should exit shortly after cancellation");
        assertTrue(thenInner.cancelledObserved.get(), "then subtree should observe cancellation");
        // else uses the original context, unaffected by the then cancellation
        assertFalse(context.isCancelled());
    }

    @Test
    public void testTimeinThenExceptionFallsBackToElse() {
        AtomicBoolean thenExecuted = new AtomicBoolean(false);
        AtomicBoolean elseExecuted = new AtomicBoolean(false);

        TestBindable thenInner = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                thenExecuted.set(true);
                throw new RuntimeException("then boom");
            }
        };
        TestBindable elseInner = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                elseExecuted.set(true);
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[]{"x"});
                return Linq4j.asEnumerable(rows);
            }
        };

        IfBindable ifBindable = new IfBindable(
                condition(5000L),
                cacheClause("t", thenInner),
                cacheClause("t", elseInner),
                true
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        Enumerable<Object[]> result = ifBindable.bind(schema, context);

        assertTrue(thenExecuted.get());
        assertTrue(elseExecuted.get(), "else clause should execute after then failure");
        List<Object[]> rows = new ArrayList<>();
        result.forEach(rows::add);
        assertEquals("t", rows.get(0)[0]);
        assertEquals(1L, rows.get(0)[1]);
    }

    @Test
    public void testTimeinCancelledWhileWaitingRethrowsWithoutFallback() throws Exception {
        // the then branch observes cancellation (polling thenContext): after the ancestor cancels, then fails;
        // the If must not fall back to else, but abort by throwing
        PollingBindable thenInner = new PollingBindable();
        AtomicBoolean elseExecuted = new AtomicBoolean(false);
        TestBindable elseInner = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                elseExecuted.set(true);
                return Linq4j.emptyEnumerable();
            }
        };

        IfBindable ifBindable = new IfBindable(
                condition(30000L),
                cacheClause("t", thenInner),
                cacheClause("t", elseInner),
                true
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        Thread canceller = new Thread(() -> {
            sleepQuietly(100);
            context.cancel();
        });
        canceller.start();

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        try {
            RuntimeException ex = assertThrows(RuntimeException.class, () -> ifBindable.bind(schema, context));
            assertTrue(ex.getMessage().contains("cancelled"));
        } finally {
            canceller.join(5000);
        }
        assertFalse(elseExecuted.get(), "else clause should not execute when subtree cancelled");
        assertTrue(thenInner.finished.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testTimeinCancelledWhileWaitingTimeoutBranchRethrows() throws Exception {
        // the then branch does not observe cancellation (uninterruptible long sleep): when the timeout branch
        // fires and finds the context cancelled, it also skips the fallback and aborts by throwing
        AtomicBoolean thenExecuted = new AtomicBoolean(false);
        AtomicBoolean elseExecuted = new AtomicBoolean(false);
        TestBindable thenInner = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                thenExecuted.set(true);
                sleepIgnoringInterrupts(10000);
                return Linq4j.emptyEnumerable();
            }
        };
        TestBindable elseInner = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                elseExecuted.set(true);
                return Linq4j.emptyEnumerable();
            }
        };

        IfBindable ifBindable = new IfBindable(
                condition(200L),
                cacheClause("t", thenInner),
                cacheClause("t", elseInner),
                true
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        Thread canceller = new Thread(() -> {
            sleepQuietly(100);
            context.cancel();
        });
        canceller.start();

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        try {
            RuntimeException ex = assertThrows(RuntimeException.class, () -> ifBindable.bind(schema, context));
            assertTrue(ex.getMessage().contains("cancelled"));
        } finally {
            canceller.join(5000);
        }
        assertTrue(thenExecuted.get());
        assertFalse(elseExecuted.get(), "else clause should not execute when subtree cancelled");
    }

    @Test
    public void testTimeinZeroTimeoutExecutesThenDirectly() {
        AtomicBoolean thenExecuted = new AtomicBoolean(false);
        AtomicBoolean elseExecuted = new AtomicBoolean(false);
        TestBindable thenInner = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                thenExecuted.set(true);
                return Linq4j.emptyEnumerable();
            }
        };
        TestBindable elseInner = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                elseExecuted.set(true);
                return Linq4j.emptyEnumerable();
            }
        };

        IfBindable ifBindable = new IfBindable(
                condition(0L),
                cacheClause("t", thenInner),
                cacheClause("t", elseInner),
                true
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        ifBindable.bind(schema, context);

        assertTrue(thenExecuted.get());
        assertFalse(elseExecuted.get());
    }

    // ==================== condition mode (regression) ====================

    @Test
    public void testConditionTrueExecutesThen() {
        AtomicBoolean thenExecuted = new AtomicBoolean(false);
        AtomicBoolean elseExecuted = new AtomicBoolean(false);
        TestBindable thenInner = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                thenExecuted.set(true);
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[]{"x"});
                return Linq4j.asEnumerable(rows);
            }
        };
        TestBindable elseInner = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                elseExecuted.set(true);
                return Linq4j.emptyEnumerable();
            }
        };

        IfBindable ifBindable = new IfBindable(
                condition(Boolean.TRUE),
                cacheClause("t", thenInner),
                cacheClause("t", elseInner),
                false
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        Enumerable<Object[]> result = ifBindable.bind(schema, context);

        assertTrue(thenExecuted.get());
        assertFalse(elseExecuted.get());
        List<Object[]> rows = new ArrayList<>();
        result.forEach(rows::add);
        assertEquals(1, rows.size());
        assertEquals("t", rows.get(0)[0]);
        assertEquals(1L, rows.get(0)[1]);
    }

    @Test
    public void testConditionFalseWithoutElseRegistersEmptyTable() {
        IfBindable ifBindable = new IfBindable(
                condition(Boolean.FALSE),
                cacheClause("t", returningRow()),
                null,
                false
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        Enumerable<Object[]> result = ifBindable.bind(schema, context);

        assertEquals(0, result.count());
        CacheTable table = SchemaUtils.tryGetCacheTable("t", schema);
        assertTrue(table != null && table.scan(null).count() == 0,
                "empty cache table should be registered for then clause");
    }

    @Test
    public void testConditionMustReturnSingleRow() {
        IfBindable ifBindable = new IfBindable(
                condition(Boolean.TRUE, Boolean.FALSE),
                cacheClause("t", returningRow()),
                null,
                false
        );
        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> ifBindable.bind(schema, context));
        assertEquals("condition must return exactly one row", ex.getMessage());
    }

    @Test
    public void testConditionMustReturnBoolean() {
        IfBindable ifBindable = new IfBindable(
                condition("not a boolean"),
                cacheClause("t", returningRow()),
                null,
                false
        );
        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> ifBindable.bind(schema, context));
        assertEquals("condition must return a boolean value", ex.getMessage());
    }

    @Test
    public void testTimeinConditionMustReturnNumeric() {
        IfBindable ifBindable = new IfBindable(
                condition("not a number"),
                cacheClause("t", returningRow()),
                cacheClause("t", returningRow()),
                true
        );
        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> ifBindable.bind(schema, context));
        assertEquals("condition must return a numeric value for timein mode", ex.getMessage());
    }

    @Test
    public void testTimeinRequiresElseClause() {
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new IfBindable(condition(100L), cacheClause("t", returningRow()), null, true));
        assertTrue(ex.getMessage().contains("must contain else clause when in timein mode"));
    }

    @Test
    public void testNestedIfInThenClauseRejected() {
        IfBindable nested = new IfBindable(
                condition(Boolean.TRUE),
                cacheClause("inner_t", returningRow()),
                null,
                false
        );
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new IfBindable(condition(Boolean.TRUE), nested, null, false));
        assertEquals("if statement cannot be nested in then or else clause", ex.getMessage());
    }

    @Test
    public void testNestedIfInElseClauseRejected() {
        IfBindable nested = new IfBindable(
                condition(Boolean.TRUE),
                cacheClause("inner_t", returningRow()),
                null,
                false
        );
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new IfBindable(condition(Boolean.TRUE), cacheClause("t", returningRow()), nested, false));
        assertEquals("if statement cannot be nested in then or else clause", ex.getMessage());
    }

    // ==================== helpers ====================

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepIgnoringInterrupts(long millis) {
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(Math.max(1, deadline - System.currentTimeMillis()));
            } catch (InterruptedException e) {
                // ignore interrupts, sleep the full duration
            }
        }
    }
}
