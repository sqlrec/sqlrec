package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProxyAllBindableCancelTest {

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
     * An Enumerable that fails on materialization (count/iterator), used to verify
     * that results are not materialized after cancellation.
     */
    private static class FailOnMaterializeEnumerable extends AbstractEnumerable<Object[]> {
        @Override
        public Enumerator<Object[]> enumerator() {
            throw new AssertionError("result should not be materialized after cancellation");
        }
    }

    @Test
    public void testCancelledBeforeStartThrowsWithoutExecutingDelegate() {
        AtomicBoolean executed = new AtomicBoolean(false);
        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                executed.set(true);
                return Linq4j.emptyEnumerable();
            }
        });
        proxy.setName("cancelled_node");

        ExecuteContextImpl context = new ExecuteContextImpl();
        context.cancel();

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> proxy.bind(schema, context));
        assertTrue(ex.getMessage().contains("cancelled before start"));
        assertFalse(executed.get(), "delegate should not execute when context already cancelled");
    }

    @Test
    public void testCancelledDuringExecutionThrowsAndSkipsMaterialization() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext ctx) {
                // simulate the context being cancelled by an ancestor during execution
                context.cancel();
                return new FailOnMaterializeEnumerable();
            }
        });
        proxy.setName("mid_cancel_node");

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> proxy.bind(schema, context));
        assertTrue(ex.getMessage().contains("mid_cancel_node"));
        assertTrue(ex.getMessage().contains("cancelled"));
    }

    @Test
    public void testDelegateExceptionWhileCancelledPropagatesUnwrapped() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext ctx) {
                // simulate a child node failing after the context was cancelled by an ancestor
                context.cancel();
                throw new RuntimeException("boom from cancelled subtree");
            }
        });

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> proxy.bind(schema, context));
        // the cancelled path must not wrap into "Node xxx execution failed"
        assertEquals("boom from cancelled subtree", ex.getMessage());
    }

    @Test
    public void testDelegateRegularExceptionStillWrapped() {
        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                throw new RuntimeException("boom");
            }
        });
        proxy.setName("failed_node");

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> proxy.bind(schema, context));
        assertEquals("Node failed_node execution failed", ex.getMessage());
    }

    @Test
    public void testNormalExecutionUnaffected() {
        ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[]{"x"});
                return Linq4j.asEnumerable(rows);
            }
        });

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        Enumerable<Object[]> result = proxy.bind(schema, context);
        assertEquals(1, result.count());
    }

    @Test
    public void testCancelledMetricRecorded() {
        // CompositeMeterRegistry.count() is always 0 without a child registry; attach a SimpleMeterRegistry to read values
        SimpleMeterRegistry simple = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(simple);
        try {
            ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
                @Override
                public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                    return Linq4j.emptyEnumerable();
                }
            });
            proxy.setName("metric_cancel_node");

            ExecuteContextImpl context = new ExecuteContextImpl();
            context.cancel();

            CalciteSchema schema = CalciteSchema.createRootSchema(false);
            assertThrows(RuntimeException.class, () -> proxy.bind(schema, context));

            Counter counter = simple.find(Consts.METRICS_NODE_CANCELLED)
                    .tag("name", "metric_cancel_node")
                    .counter();
            assertNotNull(counter, "cancelled metric should be registered");
            assertTrue(counter.count() > 0, "cancelled metric should be incremented");
        } finally {
            MetricsUtils.getCompositeMeterRegistry().remove(simple);
        }
    }

    @Test
    public void testCancelledDuringExecutionAlsoRecordsMetric() {
        SimpleMeterRegistry simple = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(simple);
        try {
            ExecuteContextImpl context = new ExecuteContextImpl();
            ProxyAllBindable proxy = new ProxyAllBindable(new TestBindable() {
                @Override
                public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext ctx) {
                    context.cancel();
                    throw new RuntimeException("boom from cancelled subtree");
                }
            });
            proxy.setName("metric_cancel_node_delegate");

            CalciteSchema schema = CalciteSchema.createRootSchema(false);
            assertThrows(RuntimeException.class, () -> proxy.bind(schema, context));

            Counter counter = simple.find(Consts.METRICS_NODE_CANCELLED)
                    .tag("name", "metric_cancel_node_delegate")
                    .counter();
            assertNotNull(counter, "cancelled metric should be registered");
            assertTrue(counter.count() > 0, "cancelled metric should be incremented");
        } finally {
            MetricsUtils.getCompositeMeterRegistry().remove(simple);
        }
    }
}
