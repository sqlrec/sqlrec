package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.utils.ExecutorServiceUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlFunctionBindableCancelTest {

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

    private static SqlFunctionBindable newFunction(List<BindableInterface> bindableList) throws Exception {
        bindableList.add(new ReturnBindable((BindableInterface) null));
        SqlFunctionBindable sqlFunctionBindable = new SqlFunctionBindable(
                Collections.emptyList(), bindableList, null
        );
        sqlFunctionBindable.setFunName("test_fn");
        sqlFunctionBindable.init();
        return sqlFunctionBindable;
    }

    @Test
    public void testParallelSuccessUnaffected() throws Exception {
        AtomicBoolean firstExecuted = new AtomicBoolean(false);
        AtomicBoolean secondExecuted = new AtomicBoolean(false);
        ProxyAllBindable nodeA = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                firstExecuted.set(true);
                return Linq4j.emptyEnumerable();
            }
        });
        ProxyAllBindable nodeB = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                secondExecuted.set(true);
                return Linq4j.emptyEnumerable();
            }
        });

        SqlFunctionBindable sqlFunctionBindable = newFunction(new ArrayList<>(List.of(nodeA, nodeB)));
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        ExecuteContextImpl context = new ExecuteContextImpl(schema);

        Enumerable<Object[]> result = sqlFunctionBindable.bind(schema, context);

        assertTrue(firstExecuted.get());
        assertTrue(secondExecuted.get());
        assertFalse(context.isCancelled());
        // explicit empty RETURN produces a null result
        assertNull(result);
    }

    @Test
    public void testParallelNodeFailureCancelsSiblingNodes() throws Exception {
        CountDownLatch siblingStarted = new CountDownLatch(1);
        AtomicBoolean siblingCancelledObserved = new AtomicBoolean(false);
        CountDownLatch siblingFinished = new CountDownLatch(1);

        // node B: starts first (unblocks node A), then polls its own context waiting for cancellation
        ProxyAllBindable nodeB = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                siblingStarted.countDown();
                try {
                    while (!context.isCancelled()) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            // ignore interrupts, keep polling the cancellation flag
                        }
                    }
                    siblingCancelledObserved.set(true);
                } finally {
                    siblingFinished.countDown();
                }
                throw new RuntimeException("node b cancelled");
            }
        });
        // node A: waits until B starts, then fails immediately
        ProxyAllBindable nodeA = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                try {
                    assertTrue(siblingStarted.await(5, TimeUnit.SECONDS), "node b should start first");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                throw new RuntimeException("node a failed");
            }
        });

        SqlFunctionBindable sqlFunctionBindable = newFunction(new ArrayList<>(List.of(nodeA, nodeB)));
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        ExecuteContextImpl context = new ExecuteContextImpl(schema);

        assertThrows(RuntimeException.class, () -> sqlFunctionBindable.bind(schema, context));

        // sibling node B should observe the cancellation (instead of running to natural completion)
        assertTrue(siblingFinished.await(5, TimeUnit.SECONDS),
                "sibling node should exit shortly after failure");
        assertTrue(siblingCancelledObserved.get(), "sibling node should observe cancellation");
        // the outer context must not be cancelled, to avoid affecting outer sibling nodes
        assertFalse(context.isCancelled());
    }

    @Test
    public void testSerialFailureCancelsNestedAsyncTasks() throws Exception {
        Boolean originalParallelism = SqlRecConfigs.PARALLELISM_EXEC.getDefaultValue();
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(false);
        try {
            CountDownLatch zombieFinished = new CountDownLatch(1);
            AtomicBoolean zombieCancelledObserved = new AtomicBoolean(false);
            AtomicBoolean secondExecuted = new AtomicBoolean(false);

            // node 1: spawns a background task (simulating a leftover async task inside the node), then fails
            ProxyAllBindable node1 = new ProxyAllBindable(new TestBindable() {
                @Override
                public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                    ExecutorServiceUtils.getExecutorService().submit(() -> {
                        while (!context.isCancelled()) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                        zombieCancelledObserved.set(true);
                        zombieFinished.countDown();
                    });
                    throw new RuntimeException("node 1 failed");
                }
            });
            ProxyAllBindable node2 = new ProxyAllBindable(new TestBindable() {
                @Override
                public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                    secondExecuted.set(true);
                    return Linq4j.emptyEnumerable();
                }
            });

            SqlFunctionBindable sqlFunctionBindable = newFunction(new ArrayList<>(List.of(node1, node2)));
            CalciteSchema schema = CalciteSchema.createRootSchema(false);
            ExecuteContextImpl context = new ExecuteContextImpl(schema);

            assertThrows(RuntimeException.class, () -> sqlFunctionBindable.bind(schema, context));

            assertFalse(secondExecuted.get(), "serial mode: later nodes should not run after failure");
            // node 1's leftover background task should observe the functionContext cancellation
            assertTrue(zombieFinished.await(5, TimeUnit.SECONDS),
                    "nested async task should exit shortly after function failure");
            assertTrue(zombieCancelledObserved.get(), "nested async task should observe cancellation");
        } finally {
            SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(originalParallelism);
        }
    }

    @Test
    public void testSerialErrorCancelsOnlyTheFailedFunctionContext() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        boolean originalParallelism = SqlRecConfigs.PARALLELISM_EXEC.getDefaultValue();
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(false);
        try {
            AtomicReference<ExecuteContext> failedContext = new AtomicReference<>();
            BindableInterface node = new TestBindable() {
                @Override
                public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                    failedContext.set(context);
                    throw new AssertionError("failed child function");
                }
            };
            SqlFunctionBindable function = newFunction(new ArrayList<>(List.of(node)));
            ExecuteContextImpl caller = new ExecuteContextImpl(schema);

            assertThrows(AssertionError.class,
                    () -> function.bind(schema, caller));
            assertTrue(failedContext.get().isCancelled());
            assertFalse(caller.isCancelled());
        } finally {
            SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(originalParallelism);
        }
    }

    @Test
    public void testCancelledBeforeBindAbortsAllNodes() throws Exception {
        AtomicBoolean firstExecuted = new AtomicBoolean(false);
        AtomicBoolean secondExecuted = new AtomicBoolean(false);
        ProxyAllBindable nodeA = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                firstExecuted.set(true);
                return Linq4j.emptyEnumerable();
            }
        });
        ProxyAllBindable nodeB = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                secondExecuted.set(true);
                return Linq4j.emptyEnumerable();
            }
        });

        SqlFunctionBindable sqlFunctionBindable = newFunction(new ArrayList<>(List.of(nodeA, nodeB)));
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        ExecuteContextImpl context = new ExecuteContextImpl(schema);
        context.cancel();

        assertThrows(RuntimeException.class, () -> sqlFunctionBindable.bind(schema, context));
        assertFalse(firstExecuted.get());
        assertFalse(secondExecuted.get());
    }

    @Test
    void parallelFailureReturnsWhileAnUncooperativeSiblingIsStillBlocked() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        boolean originalParallelism = SqlRecConfigs.PARALLELISM_EXEC.getDefaultValue();
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(true);
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1), finished = new CountDownLatch(1);
        AtomicReference<ExecuteContext> runningContext = new AtomicReference<>();
        BindableInterface blocked = ProxyAllBindable.wrap(new TestBindable() {
            @Override public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                runningContext.set(context);
                started.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("release timeout");
                    return Linq4j.emptyEnumerable();
                } catch (InterruptedException failure) {
                    throw new RuntimeException(failure);
                } finally {
                    finished.countDown();
                }
            }
        });
        List<BindableInterface> nodes = new ArrayList<>(List.of(blocked));
        nodes.add(ProxyAllBindable.wrap(new TestBindable() {
            @Override public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                try {
                    if (!started.await(5, TimeUnit.SECONDS)) throw new AssertionError("start timeout");
                } catch (InterruptedException failure) {
                    throw new RuntimeException(failure);
                }
                throw new IllegalStateException("failed sibling");
            }
        }));
        SqlFunctionBindable function = newFunction(nodes);
        ExecuteContextImpl request = new ExecuteContextImpl(schema);
        request.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "0");
        CompletableFuture<?> call = CompletableFuture.runAsync(
                () -> function.bind(schema, request));
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS));
            ExecutionException failure = assertThrows(ExecutionException.class, () -> call.get(2, TimeUnit.SECONDS));
            assertEquals("failed sibling", ExecutorServiceUtils.unwrap(failure).getCause().getMessage());
            assertEquals(1, finished.getCount(), "the failed call must return before the blocked node finishes");
            assertTrue(runningContext.get().isCancelled());
            assertFalse(request.isCancelled());
        } finally {
            release.countDown();
            try {
                assertTrue(finished.await(5, TimeUnit.SECONDS));
            } finally {
                SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(originalParallelism);
            }
        }
    }
}
