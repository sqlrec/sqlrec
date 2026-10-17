package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.ConcurrentCalciteSchema;
import com.sqlrec.utils.ExecutorServiceUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PartitionExecutorTest {
    private static final List<RelDataTypeField> FIELDS = List.of(
            DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
    private CalciteSchema schema;
    private final PartitionExecutor executor = new PartitionExecutor(
            "input", SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));

    private abstract static class Source extends BindableInterface {
        @Override public List<RelDataTypeField> getReturnDataFields() { return FIELDS; }
        @Override public boolean isParallelizable() { return true; }
        @Override public Set<String> getReadTables() { return Set.of("input"); }
        @Override public Set<String> getWriteTables() { return Set.of(); }
    }

    @BeforeEach
    void setUp() {
        CalciteSchemaFactory.setGlobalSchema(ConcurrentCalciteSchema.createRootSchema());
        schema = ConcurrentCalciteSchema.createRootSchema();
        schema.add("input", new CacheTable("input",
                Linq4j.asEnumerable(new Object[][]{{0}, {1}}), FIELDS));
    }

    @AfterEach
    void tearDown() {
        CalciteSchemaFactory.setGlobalSchema(null);
    }

    @Test
    void partitionsPublishAndCancelUsingTheirOwnSchemas() throws Exception {
        Source source = new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                assertNotSame(schema, s);
                CacheTableBindable.publish(s, c, new CacheTable("partition_output",
                        Linq4j.singletonEnumerable(new Object[]{id(s)}), FIELDS));
                if (id(s) == 1) throw new IllegalStateException("discard this partition");
                return Linq4j.singletonEnumerable(new Object[]{id(s)});
            }
        };
        ExecuteContextImpl parent = context(true);
        synchronized (schema) {
            assertArrayEquals(new Object[]{0}, ExecutorServiceUtils.submit(() -> executor.execute(schema, parent, source))
                    .get(5, TimeUnit.SECONDS).single());
        }
        assertNull(schema.getTable("partition_output", false));
        assertFalse(parent.isCancelled());
    }

    @Test
    void strictFailureReturnsBeforeAnEarlierBlockedPartitionFinishes() throws Exception {
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1), finished = new CountDownLatch(1);
        AtomicReference<ExecuteContext> child = new AtomicReference<>();
        IllegalStateException original = new IllegalStateException("later partition failed");
        Source source = new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                if (id(s) == 1) {
                    await(started);
                    throw original;
                }
                child.set(c);
                started.countDown();
                try {
                    await(release); // Intentionally does not cooperate with cancellation.
                    return Linq4j.singletonEnumerable(new Object[]{0});
                } finally {
                    finished.countDown();
                }
            }
        };
        ExecuteContextImpl parent = context(false);
        CompletableFuture<?> call = ExecutorServiceUtils.submit(() -> executor.execute(schema, parent, source));
        try {
            ExecutionException thrown = assertThrows(ExecutionException.class, () -> call.get(2, TimeUnit.SECONDS));
            assertEquals("Partition execution failed", thrown.getCause().getMessage());
            assertSame(original, thrown.getCause().getCause());
            assertEquals(1L, finished.getCount());
            assertTrue(child.get().isCancelled());
            assertFalse(parent.isCancelled());
        } finally {
            release.countDown();
            assertTrue(finished.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void partialFailureStillWaitsForAndKeepsTheSuccessfulPartition() throws Exception {
        CountDownLatch started = new CountDownLatch(1), failed = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicReference<ExecuteContext> child = new AtomicReference<>();
        Source source = new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                if (id(s) == 1) {
                    await(started);
                    failed.countDown();
                    throw new IllegalStateException("discard this partition");
                }
                child.set(c);
                started.countDown();
                await(release);
                return Linq4j.singletonEnumerable(new Object[]{0});
            }
        };
        ExecuteContextImpl parent = context(true);
        CompletableFuture<Enumerable<Object[]>> call = ExecutorServiceUtils.submit(
                () -> executor.execute(schema, parent, source));
        try {
            assertTrue(failed.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> call.get(100, TimeUnit.MILLISECONDS));
            assertFalse(child.get().isCancelled());
            release.countDown();
            assertArrayEquals(new Object[]{0}, call.get(2, TimeUnit.SECONDS).single());
            assertFalse(parent.isCancelled());
        } finally {
            release.countDown();
            call.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void interruptingWaitCancelsOnlyThePartitionScopeAndRestoresInterrupt() throws Exception {
        for (boolean ignoreFailures : List.of(false, true)) {
            assertInterruptedWait(ignoreFailures);
        }
    }

    private void assertInterruptedWait(boolean ignoreFailures) throws Exception {
        CountDownLatch started = new CountDownLatch(2), release = new CountDownLatch(1), finished = new CountDownLatch(2);
        AtomicReference<ExecuteContext> child = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Source source = new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                child.set(c);
                started.countDown();
                try {
                    await(release);
                    return Linq4j.emptyEnumerable();
                } finally {
                    finished.countDown();
                }
            }
        };
        ExecuteContextImpl parent = context(ignoreFailures);
        Thread caller = Thread.ofVirtual().start(() -> {
            try {
                executor.execute(schema, parent, source);
            } catch (RuntimeException exception) {
                failure.set(exception);
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(2000);
            assertFalse(caller.isAlive());
            assertInstanceOf(InterruptedException.class, failure.get().getCause());
            assertTrue(interrupted.get());
            assertTrue(child.get().isCancelled());
            assertFalse(parent.isCancelled());
        } finally {
            release.countDown();
            caller.interrupt();
            caller.join(2000);
            assertTrue(finished.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void ignoredPartitionFailureCancelsOnlyItsOwnBackgroundWorkIncludingLazyFailures() throws Exception {
        for (boolean lazyFailure : List.of(false, true)) {
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<ExecuteContext> succeeded = new AtomicReference<>(), failed = new AtomicReference<>();
            AtomicReference<CompletableFuture<?>> background = new AtomicReference<>();
            Source source = new Source() {
                @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                    if (id(s) == 0) {
                        succeeded.set(c);
                        return Linq4j.singletonEnumerable(new Object[]{0});
                    }
                    failed.set(c);
                    background.set(ExecutorServiceUtils.submit(() -> {
                        await(release);
                        if (!c.isCancelled()) c.setVariable("failed_partition_write", "unexpected");
                        return null;
                    }));
                    if (lazyFailure) {
                        return Linq4j.singletonEnumerable(new Object[]{1}).select(row -> {
                            throw new IllegalStateException("lazy partition failure");
                        });
                    }
                    throw new IllegalStateException("partition failure");
                }
            };
            ExecuteContextImpl parent = context(true);
            try {
                assertArrayEquals(new Object[]{0}, executor.execute(schema, parent, source).single());
                assertNotSame(succeeded.get(), failed.get());
                assertTrue(failed.get().isCancelled());
                assertFalse(succeeded.get().isCancelled());
                assertFalse(parent.isCancelled());
                release.countDown();
                background.get().get(2, TimeUnit.SECONDS);
                assertNull(parent.getVariable("failed_partition_write"));
            } finally {
                release.countDown();
                if (background.get() != null) background.get().get(5, TimeUnit.SECONDS);
            }
        }
    }

    private static int id(CalciteSchema schema) {
        return (int) ((CacheTable) schema.getTable("input", false).getTable()).scan(null).first()[0];
    }

    private ExecuteContextImpl context(boolean ignoreFailures) {
        ExecuteContextImpl context = new ExecuteContextImpl(schema);
        context.setVariable("IGNORE_PARTITION_EXCEPTION", Boolean.toString(ignoreFailures));
        return context;
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
