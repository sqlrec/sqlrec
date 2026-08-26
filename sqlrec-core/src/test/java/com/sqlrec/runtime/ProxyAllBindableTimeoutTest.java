package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProxyAllBindableTimeoutTest {

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

    @Test
    public void testTimeoutAppliesToNonCacheNodeAndCancelsOnlyChildContext() throws Exception {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "100");
        AtomicBoolean cancellationObserved = new AtomicBoolean(false);
        CountDownLatch finished = new CountDownLatch(1);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(registry);

        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext childContext) {
                try {
                    while (!childContext.isCancelled()) {
                        Thread.sleep(10);
                    }
                    cancellationObserved.set(true);
                    return Linq4j.emptyEnumerable();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    cancellationObserved.set(childContext.isCancelled());
                    finished.countDown();
                }
            }
        });
        proxy.setName("slow_node");

        try {
            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> proxy.bind(CalciteSchema.createRootSchema(false), context));

            assertEquals("Node slow_node execution timeout", exception.getMessage());
            assertTrue(finished.await(5, TimeUnit.SECONDS));
            assertTrue(cancellationObserved.get());
            assertFalse(context.isCancelled(), "node timeout must not cancel the parent context");

            Timer timer = registry.find(Consts.METRICS_NODE_EXEC_DURATION)
                    .tags("name", "slow_node", "status", "timeout")
                    .timer();
            assertNotNull(timer);
            assertEquals(1L, timer.count());
        } finally {
            MetricsUtils.getCompositeMeterRegistry().remove(registry);
        }
    }

    @Test
    public void testExecutionSucceedsWithinTimeoutOnExecutorThread() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "5000");
        Thread callerThread = Thread.currentThread();
        AtomicReference<Thread> executionThread = new AtomicReference<>();

        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                executionThread.set(Thread.currentThread());
                return Linq4j.singletonEnumerable(new Object[]{"ok"});
            }
        });

        Enumerable<Object[]> result = proxy.bind(CalciteSchema.createRootSchema(false), context);

        assertEquals(1L, result.count());
        assertNotSame(callerThread, executionThread.get());
    }

    @Test
    public void testZeroTimeoutExecutesInline() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "0");
        AtomicReference<Thread> executionThread = new AtomicReference<>();

        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                executionThread.set(Thread.currentThread());
                return Linq4j.emptyEnumerable();
            }
        });

        proxy.bind(CalciteSchema.createRootSchema(false), context);

        assertSame(Thread.currentThread(), executionThread.get());
    }

    @Test
    public void testTimeoutIsSkippedWhenDelegateIsNotTimeoutAble() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "50");
        AtomicReference<Thread> executionThread = new AtomicReference<>();

        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                executionThread.set(Thread.currentThread());
                return Linq4j.emptyEnumerable();
            }

            @Override
            public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
                return false;
            }
        });

        Enumerable<Object[]> result = proxy.bind(CalciteSchema.createRootSchema(false), context);

        assertNotNull(result);
        assertSame(Thread.currentThread(), executionThread.get());
    }

    @Test
    public void testRuntimeExceptionFromTimedExecutionPreservesCause() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "5000");
        IllegalStateException delegateFailure = new IllegalStateException("delegate failed");

        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                throw delegateFailure;
            }
        });
        proxy.setName("failed_node");

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> proxy.bind(CalciteSchema.createRootSchema(false), context));

        assertEquals("Node failed_node execution failed", exception.getMessage());
        assertSame(delegateFailure, exception.getCause());
    }

    @Test
    public void testRegularExceptionOnTimedCacheNodeIsRecovered() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "5000");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(registry);

        TestBindable delegate = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                throw new IllegalStateException("recoverable failure");
            }

            @Override
            public String getCacheTableName() {
                return "recovered_cache";
            }

            @Override
            public List<RelDataTypeField> getCacheTableDataFields() {
                return getReturnDataFields();
            }
        };
        delegate.setIgnoreException(true);
        ProxyAllBindable proxy = new ProxyAllBindable(delegate);
        proxy.setName("recovered_cache_node");
        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        try {
            Enumerable<Object[]> result = proxy.bind(schema, context);
            List<Object[]> rows = new ArrayList<>();
            result.forEach(rows::add);

            assertEquals(1, rows.size());
            assertEquals("recovered_cache", rows.get(0)[0]);
            assertEquals(0L, rows.get(0)[1]);
            assertNotNull(schema.getTable("recovered_cache", false));

            Counter counter = registry.find(Consts.METRICS_CACHE_TABLE_IGNORE_EXCEPTION)
                    .tag("name", "recovered_cache_node")
                    .counter();
            assertNotNull(counter);
            assertEquals(1.0, counter.count());
        } finally {
            MetricsUtils.getCompositeMeterRegistry().remove(registry);
        }
    }

    @Test
    public void testErrorFromTimedExecutionPropagatesUnwrapped() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "5000");
        AssertionError delegateFailure = new AssertionError("fatal delegate failure");

        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                throw delegateFailure;
            }
        });

        AssertionError exception = assertThrows(AssertionError.class,
                () -> proxy.bind(CalciteSchema.createRootSchema(false), context));

        assertSame(delegateFailure, exception);
    }

    @Test
    public void testCheckedThrowableFromTimedExecutionIsWrapped() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "5000");
        Exception delegateFailure = new Exception("checked delegate failure");

        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                return ProxyAllBindableTimeoutTest
                        .<RuntimeException, Enumerable<Object[]>>sneakyThrow(delegateFailure);
            }
        });
        proxy.setName("checked_failure_node");

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> proxy.bind(CalciteSchema.createRootSchema(false), context));

        assertEquals("Node checked_failure_node execution failed", exception.getMessage());
        assertSame(delegateFailure, exception.getCause());
    }

    @Test
    public void testInterruptedWaitRestoresInterruptAndCancelsChildContext() throws Exception {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "10000");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean cancellationObserved = new AtomicBoolean(false);
        AtomicBoolean interruptRestored = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext childContext) {
                started.countDown();
                try {
                    while (!childContext.isCancelled() && release.getCount() > 0) {
                        Thread.sleep(10);
                    }
                    return Linq4j.emptyEnumerable();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    cancellationObserved.set(childContext.isCancelled());
                    finished.countDown();
                }
            }
        });
        proxy.setName("interrupted_node");

        Thread caller = new Thread(() -> {
            try {
                proxy.bind(CalciteSchema.createRootSchema(false), context);
            } catch (Throwable e) {
                failure.set(e);
                interruptRestored.set(Thread.currentThread().isInterrupted());
            }
        });

        try {
            caller.start();
            assertTrue(started.await(5, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(5000);

            assertFalse(caller.isAlive());
            assertNotNull(failure.get());
            assertEquals("Node interrupted_node execution failed", failure.get().getMessage());
            assertTrue(interruptRestored.get());
            assertTrue(finished.await(5, TimeUnit.SECONDS));
            assertTrue(cancellationObserved.get());
            assertFalse(context.isCancelled());
        } finally {
            release.countDown();
            caller.interrupt();
            caller.join(5000);
        }
    }

    @Test
    public void testIgnoreExceptionDoesNotHideTimeoutForNonCacheNode() throws Exception {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "100");
        CountDownLatch finished = new CountDownLatch(1);

        TestBindable delegate = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext childContext) {
                try {
                    while (!childContext.isCancelled()) {
                        Thread.sleep(10);
                    }
                    return Linq4j.emptyEnumerable();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    finished.countDown();
                }
            }
        };
        delegate.setIgnoreException(true);
        ProxyAllBindable proxy = new ProxyAllBindable(delegate);
        proxy.setName("non_cache_node");

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> proxy.bind(CalciteSchema.createRootSchema(false), context));

        assertEquals("Node non_cache_node execution timeout", exception.getMessage());
        assertTrue(finished.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testIgnoreTimeoutRequiresCompleteCacheMetadata() throws Exception {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "100");
        CountDownLatch finished = new CountDownLatch(1);

        TestBindable delegate = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext childContext) {
                try {
                    while (!childContext.isCancelled()) {
                        Thread.sleep(10);
                    }
                    return Linq4j.emptyEnumerable();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    finished.countDown();
                }
            }

            @Override
            public String getCacheTableName() {
                return "incomplete_cache";
            }
        };
        delegate.setIgnoreException(true);
        ProxyAllBindable proxy = new ProxyAllBindable(delegate);
        proxy.setName("incomplete_cache_node");
        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> proxy.bind(schema, context));

        assertEquals("Node incomplete_cache_node execution timeout", exception.getMessage());
        assertNull(schema.getTable("incomplete_cache", false));
        assertTrue(finished.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testCancellationDuringTimeoutRecoveryPreventsCacheWrite() throws Exception {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "100");
        CountDownLatch finished = new CountDownLatch(1);

        TestBindable delegate = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext childContext) {
                try {
                    while (!childContext.isCancelled()) {
                        Thread.sleep(10);
                    }
                    return Linq4j.emptyEnumerable();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    finished.countDown();
                }
            }

            @Override
            public String getCacheTableName() {
                return "cancelled_cache";
            }

            @Override
            public List<RelDataTypeField> getCacheTableDataFields() {
                return getReturnDataFields();
            }

            @Override
            public String getSql() {
                context.cancel();
                return "CACHE TABLE cancelled_cache AS SELECT col1";
            }
        };
        delegate.setIgnoreException(true);
        ProxyAllBindable proxy = new ProxyAllBindable(delegate);
        proxy.setName("cancelled_cache_node");
        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> proxy.bind(schema, context));

        assertEquals("node cancelled_cache_node execution cancelled", exception.getMessage());
        assertTrue(context.isCancelled());
        assertNull(schema.getTable("cancelled_cache", false));
        assertTrue(finished.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testAncestorCancellationWhileWaitingIsReportedAsCancellation() throws Exception {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "100");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(registry);

        TestBindable delegate = new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                started.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                    return Linq4j.emptyEnumerable();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        };
        delegate.setIgnoreException(true);
        ProxyAllBindable proxy = new ProxyAllBindable(delegate);
        proxy.setName("ancestor_cancelled_node");

        Thread canceller = new Thread(() -> {
            try {
                if (started.await(5, TimeUnit.SECONDS)) {
                    context.cancel();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        try {
            canceller.start();
            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> proxy.bind(CalciteSchema.createRootSchema(false), context));

            assertTrue(exception.getMessage().contains("Task execution timeout"));
            assertFalse(exception.getMessage().contains("Node ancestor_cancelled_node execution timeout"));
            assertNotNull(registry.find(Consts.METRICS_NODE_CANCELLED)
                    .tag("name", "ancestor_cancelled_node")
                    .counter());
            assertNotNull(registry.find(Consts.METRICS_NODE_EXEC_DURATION)
                    .tags("name", "ancestor_cancelled_node", "status", "cancelled")
                    .timer());
        } finally {
            release.countDown();
            canceller.join(5000);
            MetricsUtils.getCompositeMeterRegistry().remove(registry);
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable, R> R sneakyThrow(Throwable throwable) throws E {
        throw (E) throwable;
    }
}
