package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.schema.ConcurrentCalciteSchema;
import com.sqlrec.utils.ExecutorServiceUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IfNodeTimeoutTest {
    private static final List<RelDataTypeField> FIELDS = List.of(
            DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));

    private abstract static class Source extends BindableInterface {
        @Override public List<RelDataTypeField> getReturnDataFields() { return FIELDS; }
        @Override public boolean isParallelizable() { return true; }
        @Override public Set<String> getReadTables() { return Set.of(); }
        @Override public Set<String> getWriteTables() { return Set.of(); }
    }

    @Test
    void ordinaryIfAndTimeinPublishTheSelectedBranch() {
        assertSelectedBranch(true, false, false, 1);
        assertSelectedBranch(false, false, false, 2);
        assertSelectedBranch(5000L, true, false, 1);
        assertSelectedBranch(0L, true, false, 2);
        assertSelectedBranch(5000L, true, true, 2);
    }

    private void assertSelectedBranch(Object value, boolean timein, boolean primaryFails, int expected) {
        CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
        Source primary = new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                if (primaryFails) throw new IllegalStateException("primary failed");
                return rows(1);
            }
        };
        ExecuteContextImpl parent = context(schema, 1000);
        Enumerable<Object[]> result = conditional(condition(value), primary, constant(2), timein).bind(schema, parent);
        assertArrayEquals(new Object[]{"output", 1L}, result.single());
        assertEquals(expected, storedValue(schema));
        assertFalse(parent.isCancelled());
    }

    @Test
    void falseIfWithoutElseCreatesEmptyCacheOrPreservesExistingCache() {
        for (boolean existing : List.of(false, true)) {
            CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
            CacheTable previous = new CacheTable("output", rows(9), FIELDS);
            if (existing) schema.add("output", previous);
            AtomicBoolean primaryRan = new AtomicBoolean();
            Source primary = new Source() {
                @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                    primaryRan.set(true);
                    return rows(1);
                }
            };
            conditional(condition(false), primary, null, false).bind(schema, context(schema, 1000));
            CacheTable result = (CacheTable) schema.getTable("output", false).getTable();
            assertFalse(primaryRan.get());
            if (existing) assertSame(previous, result);
            else assertEquals(0, result.scan(null).count());
        }
    }

    @Test
    void outerTimeoutDiscardsLatePrimaryWithoutRunningElse() throws Exception {
        for (boolean timein : List.of(false, true)) {
            CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
            CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
            AtomicReference<Thread> worker = new AtomicReference<>();
            AtomicReference<ExecuteContext> child = new AtomicReference<>();
            AtomicBoolean elseRan = new AtomicBoolean();
            Source primary = new Source() {
                @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                    worker.set(Thread.currentThread());
                    child.set(c);
                    started.countDown();
                    await(release);
                    return rows(1);
                }
            };
            Source fallback = new Source() {
                @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                    elseRan.set(true);
                    return rows(2);
                }
            };
            ExecuteContextImpl parent = context(schema, 200);
            try {
                RuntimeException failure = assertThrows(RuntimeException.class,
                        () -> conditional(condition(timein ? 5000L : true), primary, fallback, timein).bind(schema, parent));
                assertTrue(failure.getMessage().contains("timeout"));
                assertTrue(started.await(2, TimeUnit.SECONDS));
                assertTrue(child.get().isCancelled());
                assertFalse(elseRan.get());
                assertFalse(parent.isCancelled());
                schema.add("output", new CacheTable("output", rows(9), FIELDS));
                release.countDown();
                worker.get().join(2000);
                assertFalse(worker.get().isAlive());
                assertEquals(9, storedValue(schema));
            } finally {
                release.countDown();
                if (worker.get() != null) worker.get().join(2000);
            }
        }
    }

    @Test
    void timeinTimeoutCanFallbackWithinTheLongerNodeBudgetWithoutLateOverwrite() throws Exception {
        CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        Source primary = new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                worker.set(Thread.currentThread());
                await(release);
                return rows(1);
            }
        };
        try {
            conditional(condition(100L), primary, constant(2), true).bind(schema, context(schema, 2000));
            assertEquals(2, storedValue(schema));
            release.countDown();
            worker.get().join(2000);
            assertFalse(worker.get().isAlive());
            assertEquals(2, storedValue(schema));
        } finally {
            release.countDown();
            if (worker.get() != null) worker.get().join(2000);
        }
    }

    @Test
    void timeinPublicationFailureStillRunsElseUnderTheNodeBudget() {
        CalciteSchema schema = spy(ConcurrentCalciteSchema.createRootSchema());
        doAnswer(invocation -> {
            CacheTable table = invocation.getArgument(1);
            if ((int) table.scan(null).first()[0] == 1) throw new IllegalStateException("publication failed");
            return invocation.callRealMethod();
        }).when(schema).add(eq("output"), any(Table.class));

        conditional(condition(5000L), constant(1), constant(2), true).bind(schema, context(schema, 1000));
        assertEquals(2, storedValue(schema));
    }

    @Test
    void conditionAndElseConsumeTheSameNodeBudget() throws Exception {
        CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
        CountDownLatch conditionStarted = new CountDownLatch(1), releaseCondition = new CountDownLatch(1);
        CountDownLatch elseStarted = new CountDownLatch(1), releaseElse = new CountDownLatch(1);
        AtomicReference<Thread> conditionWorker = new AtomicReference<>(), elseWorker = new AtomicReference<>();
        CalciteBindable condition = new CalciteBindable(Map.of(), data -> {
            conditionWorker.set(Thread.currentThread());
            conditionStarted.countDown();
            await(releaseCondition);
            return rows(0);
        }, null, null, null, null, null);
        Source fallback = new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                elseWorker.set(Thread.currentThread());
                elseStarted.countDown();
                await(releaseElse);
                return rows(2);
            }
        };
        ExecuteContextImpl parent = context(schema, 1000);
        CompletableFuture<?> call = ExecutorServiceUtils.submit(
                () -> conditional(condition, constant(1), fallback, true).bind(schema, parent));
        try {
            assertTrue(conditionStarted.await(2, TimeUnit.SECONDS));
            // Spend most of the node budget in the condition, then keep ELSE blocked.
            assertThrows(TimeoutException.class, () -> call.get(600, TimeUnit.MILLISECONDS));
            releaseCondition.countDown();
            assertTrue(elseStarted.await(2, TimeUnit.SECONDS));
            ExecutionException failure = assertThrows(ExecutionException.class, () -> call.get(700, TimeUnit.MILLISECONDS));
            assertTrue(failure.getCause().getMessage().contains("timeout"));
            assertFalse(parent.isCancelled());
            assertNull(schema.getTable("output", false));
        } finally {
            releaseCondition.countDown();
            releaseElse.countDown();
            if (conditionWorker.get() != null) conditionWorker.get().join(2000);
            if (elseWorker.get() != null) elseWorker.get().join(2000);
        }
    }

    private static BindableInterface conditional(CalciteBindable condition, Source primary, Source fallback, boolean timein) {
        return ProxyAllBindable.wrap(new IfBindable(condition, new CacheTableBindable("output", primary),
                fallback == null ? null : new CacheTableBindable("output", fallback), timein));
    }

    private static CalciteBindable condition(Object value) {
        return new CalciteBindable(Map.of(), data -> Linq4j.singletonEnumerable(new Object[]{value}),
                null, null, null, null, null);
    }

    private static Source constant(int value) {
        return new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) { return rows(value); }
        };
    }

    private static ExecuteContextImpl context(CalciteSchema schema, long timeout) {
        ExecuteContextImpl context = new ExecuteContextImpl(schema);
        context.setVariable("NODE_EXEC_TIMEOUT", Long.toString(timeout));
        return context;
    }

    private static Enumerable<Object[]> rows(int value) {
        return Linq4j.singletonEnumerable(new Object[]{value});
    }

    private static Object storedValue(CalciteSchema schema) {
        return ((CacheTable) schema.getTable("output", false).getTable()).scan(null).first()[0];
    }

    private static void await(CountDownLatch gate) {
        try {
            if (!gate.await(5, TimeUnit.SECONDS)) throw new AssertionError("gate timeout");
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(failure);
        }
    }
}
