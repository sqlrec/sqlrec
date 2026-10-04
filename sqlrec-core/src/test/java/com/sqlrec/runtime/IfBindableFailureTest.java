package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.common.utils.SilenceLoggers;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class IfBindableFailureTest {
    @Test
    void interruptedWorkerDoesNotInterruptCallerOrRunElse() {
        for (boolean wrapped : new boolean[]{false, true}) {
            InterruptedException interruption = new InterruptedException("worker interrupted");
            AtomicReference<ExecuteContext> thenContext = new AtomicReference<>();
            AtomicBoolean elseRan = new AtomicBoolean();
            IfBindable bindable = timein(5000, context -> {
                thenContext.set(context);
                Thread.currentThread().interrupt();
                return IfBindableFailureTest.<RuntimeException>fail(
                        wrapped ? new CompletionException(interruption) : interruption);
            }, context -> {
                elseRan.set(true);
                return row(2);
            });
            ExecuteContextImpl parent = new ExecuteContextImpl().createFunctionContext();
            try {
                assertFalse(Thread.currentThread().isInterrupted());
                RuntimeException failure = assertThrows(RuntimeException.class,
                        () -> bindable.bind(CalciteSchema.createRootSchema(false), parent));

                assertSame(interruption, failure.getCause());
                assertFalse(Thread.currentThread().isInterrupted());
                assertTrue(thenContext.get().isCancelled());
                assertFalse(elseRan.get());
                assertFalse(parent.isCancelled());
                assertFalse(parent.hasReturnedFromFunction());
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void nonPositiveTimeinExecutesOnlyElseInlineAndRecordsDirectFallback() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(registry);
        try {
            for (long timeout : new long[]{0, -1}) {
                ExecuteContextImpl parent = new ExecuteContextImpl().createFunctionContext();
                Thread caller = Thread.currentThread();
                IfBindable bindable = timein(timeout, context -> {
                    throw new AssertionError("non-positive TIMEIN must not start THEN");
                }, context -> {
                    assertSame(parent, context);
                    assertSame(caller, Thread.currentThread());
                    return row(2);
                });
                String name = "direct_timein_" + timeout;
                bindable.setName(name);

                Enumerable<Object[]> result = bindable.bind(CalciteSchema.createRootSchema(false), parent);

                assertEquals(2, result.single()[0]);
                assertSame(result, parent.getFunctionReturnResult());
                assertFalse(parent.isCancelled());
                assertFalse(Thread.currentThread().isInterrupted());
                assertEquals(1.0, registry.get(Consts.METRICS_IF_CACHE_DIRECT_FALLBACK)
                        .tag("name", name).counter().count());
                assertNull(registry.find(Consts.METRICS_IF_CACHE_TIMEOUT).tag("name", name).counter());
                assertNull(registry.find(Consts.METRICS_IF_CACHE_EXCEPTION_FALLBACK).tag("name", name).counter());
            }
        } finally {
            MetricsUtils.getCompositeMeterRegistry().remove(registry);
            registry.close();
        }
    }

    @Test
    void nonPositiveTimeinPropagatesElseFailureWithoutStartingThen() {
        for (long timeout : new long[]{0, -1}) {
            RuntimeException failure = new RuntimeException("ELSE failed");
            IfBindable bindable = timein(timeout, context -> {
                throw new AssertionError("non-positive TIMEIN must not start THEN");
            }, context -> {
                throw failure;
            });
            ExecuteContextImpl parent = new ExecuteContextImpl().createFunctionContext();

            assertSame(failure, assertThrows(RuntimeException.class,
                    () -> bindable.bind(CalciteSchema.createRootSchema(false), parent)));
            assertFalse(parent.hasReturnedFromFunction());
        }
    }

    @Test
    @SilenceLoggers(IfBindable.class)
    void waitingTimeoutRecordsTimeoutInsteadOfDirectFallback() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(registry);
        CountDownLatch release = new CountDownLatch(1);
        try {
            IfBindable bindable = timein(100, context -> {
                try {
                    release.await();
                    return row(1);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(failure);
                }
            }, context -> row(2));
            String name = "waiting_timein_timeout_metric";
            bindable.setName(name);
            ExecuteContextImpl parent = new ExecuteContextImpl().createFunctionContext();

            Enumerable<Object[]> result = bindable.bind(CalciteSchema.createRootSchema(false), parent);

            assertEquals(2, result.single()[0]);
            assertSame(result, parent.getFunctionReturnResult());
            assertFalse(parent.isCancelled());
            assertEquals(1.0, registry.get(Consts.METRICS_IF_CACHE_TIMEOUT).tag("name", name).counter().count());
            assertNull(registry.find(Consts.METRICS_IF_CACHE_DIRECT_FALLBACK).tag("name", name).counter());
            assertNull(registry.find(Consts.METRICS_IF_CACHE_EXCEPTION_FALLBACK).tag("name", name).counter());
        } finally {
            release.countDown();
            MetricsUtils.getCompositeMeterRegistry().remove(registry);
            registry.close();
        }
    }

    @Test
    void nonPositiveTimeinDoesNotRunEitherBranchAfterAncestorCancellation() {
        for (long timeout : new long[]{0, -1}) {
            ExecuteContextImpl ancestor = new ExecuteContextImpl();
            ExecuteContextImpl parent = ancestor.createFunctionContext();
            ancestor.cancel();
            IfBindable bindable = timein(timeout, context -> {
                throw new AssertionError("cancelled TIMEIN must not start THEN");
            }, context -> {
                throw new AssertionError("cancelled TIMEIN must not start ELSE");
            });

            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> bindable.bind(CalciteSchema.createRootSchema(false), parent));

            assertTrue(failure.getMessage().contains("cancelled"));
            assertFalse(parent.hasReturnedFromFunction());
        }
    }

    @Test
    @SilenceLoggers(IfBindable.class)
    void internalTimeoutCancelsThenBeforeFallbackAndUsesExceptionMetric() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(registry);
        try {
            for (boolean wrapped : new boolean[]{false, true}) {
                AtomicReference<ExecuteContextImpl> thenContext = new AtomicReference<>();
                IfBindable bindable = timein(5000, context -> {
                    ExecuteContextImpl child = (ExecuteContextImpl) context;
                    thenContext.set(child);
                    child.returnFromFunction(row(1));
                    TimeoutException failure = new TimeoutException("internal I/O timeout");
                    return IfBindableFailureTest.<RuntimeException>fail(
                            wrapped ? new CompletionException(failure) : failure);
                }, context -> {
                    assertTrue(thenContext.get().isCancelled(), "cancel THEN before starting ELSE");
                    assertFalse(context.isCancelled());
                    return row(2);
                });
                String name = "internal_timeout_wrapped_" + wrapped;
                bindable.setName(name);
                ExecuteContextImpl parent = new ExecuteContextImpl().createFunctionContext();

                Enumerable<Object[]> result = bindable.bind(CalciteSchema.createRootSchema(false), parent);

                assertEquals(2, result.single()[0]);
                assertSame(result, parent.getFunctionReturnResult());
                assertFalse(parent.isCancelled());
                assertEquals(1.0, registry.get(Consts.METRICS_IF_CACHE_EXCEPTION_FALLBACK)
                        .tag("name", name).counter().count());
                assertNull(registry.find(Consts.METRICS_IF_CACHE_DIRECT_FALLBACK).tag("name", name).counter());
                assertNull(registry.find(Consts.METRICS_IF_CACHE_TIMEOUT).tag("name", name).counter());
            }
        } finally {
            MetricsUtils.getCompositeMeterRegistry().remove(registry);
            registry.close();
        }
    }

    private static IfBindable timein(long timeout,
                                    Function<ExecuteContext, Enumerable<Object[]>> thenAction,
                                    Function<ExecuteContext, Enumerable<Object[]>> elseAction) {
        CalciteBindable condition = new CalciteBindable(new HashMap<>(), ignored -> row(timeout),
                null, null, null, null, null);
        return new IfBindable(condition, new ReturnBindable(action(thenAction)),
                new ReturnBindable(action(elseAction)), true);
    }

    private static BindableInterface action(Function<ExecuteContext, Enumerable<Object[]>> action) {
        return new BindableInterface() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                return action.apply(context);
            }

            @Override
            public List<RelDataTypeField> getReturnDataFields() {
                return List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
            }

            @Override
            public boolean isParallelizable() {
                return true;
            }

            @Override
            public Set<String> getReadTables() {
                return Set.of();
            }

            @Override
            public Set<String> getWriteTables() {
                return Set.of();
            }
        };
    }

    private static Enumerable<Object[]> row(Object value) {
        return Linq4j.singletonEnumerable(new Object[]{value});
    }

    // Generated or custom nodes can throw checked failures despite bind's signature.
    @SuppressWarnings("unchecked")
    private static <T extends Throwable> Enumerable<Object[]> fail(Throwable failure) throws T {
        throw (T) failure;
    }
}
