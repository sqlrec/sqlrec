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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CacheNodeExecutionTest {
    private static final List<RelDataTypeField> FIELDS = List.of(
            DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));

    private abstract static class Source extends BindableInterface {
        @Override public List<RelDataTypeField> getReturnDataFields() { return FIELDS; }
        @Override public boolean isParallelizable() { return true; }
        @Override public Set<String> getReadTables() { return Set.of(); }
        @Override public Set<String> getWriteTables() { return Set.of(); }
    }

    @Test
    void successfulTimedCachePublishesWithoutTriggeringRecovery() {
        CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
        BindableInterface proxy = recoverable(new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) { return rows(); }
        });
        ExecuteContextImpl context = context(schema, 5000);
        assertArrayEquals(new Object[]{"output", 1L}, proxy.bind(schema, context).single());
        assertEquals(1, storedRows(schema));
        assertFalse(context.isCancelled());
    }

    @Test
    void lateComputationCannotOverwriteTheRecoveredEmptyTable() throws Exception {
        CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        AtomicReference<ExecuteContext> child = new AtomicReference<>();
        BindableInterface proxy = recoverable(new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                worker.set(Thread.currentThread());
                child.set(c);
                started.countDown();
                await(release); // Deliberately ignores cancellation while computing.
                return rows();
            }
        });
        ExecuteContextImpl context = context(schema, 100);
        try {
            assertArrayEquals(new Object[]{"output", 0L}, proxy.bind(schema, context).single());
            assertTrue(started.await(2, TimeUnit.SECONDS));
            assertTrue(child.get().isCancelled());
            assertEquals(0, storedRows(schema));
            release.countDown();
            worker.get().join(2000);
            assertFalse(worker.get().isAlive());
            assertEquals(0, storedRows(schema));
            assertFalse(context.isCancelled());
        } finally {
            release.countDown();
            if (worker.get() != null) worker.get().join(2000);
        }
    }

    @Test
    void publicationFailureStillRecoversAndCancelsOnlyTheFailedNode() {
        CalciteSchema schema = spy(ConcurrentCalciteSchema.createRootSchema());
        doAnswer(invocation -> {
            CacheTable table = invocation.getArgument(1);
            if (table.scan(null).any()) throw new IllegalStateException("publication failed");
            return invocation.callRealMethod();
        }).when(schema).add(eq("output"), any(Table.class));
        AtomicReference<ExecuteContext> child = new AtomicReference<>();
        BindableInterface proxy = recoverable(new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                child.set(c);
                return rows();
            }
        });
        ExecuteContextImpl context = context(schema, 5000);

        assertArrayEquals(new Object[]{"output", 0L}, proxy.bind(schema, context).single());
        assertEquals(0, storedRows(schema));
        assertTrue(child.get().isCancelled());
        assertFalse(context.isCancelled());
    }

    @Test
    void failedNodeCancelsItsBackgroundWorkBeforeRecoveryWithOrWithoutTimeout() throws Exception {
        for (long timeout : new long[]{0, 5000}) {
            for (boolean timeoutAble : new boolean[]{false, true}) {
                CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
                AtomicReference<ExecuteContext> child = new AtomicReference<>();
                AtomicReference<CompletableFuture<?>> background = new AtomicReference<>();
                CountDownLatch release = new CountDownLatch(1);
                BindableInterface proxy = recoverable(new Source() {
                    @Override public boolean isTimeoutAble(CalciteSchema s, ExecuteContext c) { return timeoutAble; }
                    @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                        child.set(c);
                        background.set(ExecutorServiceUtils.submit(() -> {
                            await(release);
                            if (!c.isCancelled()) c.setVariable("background_write", "unexpected");
                            return null;
                        }));
                        throw new IllegalStateException("failed after starting background work");
                    }
                });
                ExecuteContextImpl context = context(schema, timeout);
                try {
                    assertArrayEquals(new Object[]{"output", 0L}, proxy.bind(schema, context).single());
                    assertTrue(child.get().isCancelled());
                    assertFalse(context.isCancelled());
                    release.countDown();
                    background.get().get(2, TimeUnit.SECONDS);
                    assertNull(context.getVariable("background_write"));

                    ProxyAllBindable.wrap(new SetBindable("next_node", "ran")).bind(schema, context);
                    assertEquals("ran", context.getVariable("next_node"));
                } finally {
                    release.countDown();
                    if (background.get() != null) background.get().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static BindableInterface recoverable(Source source) {
        CacheTableBindable cache = new CacheTableBindable("output", source);
        cache.setIgnoreException(true);
        return ProxyAllBindable.wrap(cache);
    }

    private static ExecuteContextImpl context(CalciteSchema schema, long timeout) {
        ExecuteContextImpl context = new ExecuteContextImpl(schema);
        context.setVariable("NODE_EXEC_TIMEOUT", Long.toString(timeout));
        context.setVariable("IGNORE_UNION_EXCEPTION", "true");
        return context;
    }

    private static Enumerable<Object[]> rows() {
        return Linq4j.singletonEnumerable(new Object[]{1});
    }

    private static long storedRows(CalciteSchema schema) {
        return ((CacheTable) schema.getTable("output", false).getTable()).scan(null).count();
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
