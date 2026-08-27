package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CacheTableBindableCancelTest {

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
     * Used to verify that after a timeout the child task observes the cancellation via the context and exits.
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

    @Test
    public void testTimeoutCancelsChildContext() throws Exception {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "100");

        PollingBindable inner = new PollingBindable();
        CacheTableBindable cacheTableBindable = new CacheTableBindable("t", inner);

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        long startTime = System.currentTimeMillis();
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> bindWithProxy(cacheTableBindable, schema, context));
        long duration = System.currentTimeMillis() - startTime;

        assertTrue(duration < 5000, "should fail fast on timeout, took " + duration + "ms");
        assertTrue(ex.getMessage().contains("timeout"), "unexpected message: " + ex.getMessage());

        // the child task should observe the cancellation and exit promptly
        assertTrue(inner.finished.await(5, TimeUnit.SECONDS),
                "inner task should exit shortly after cancellation");
        assertTrue(inner.cancelledObserved.get(), "inner task should observe cancellation");
    }

    @Test
    public void testCancelledDuringExecutionSkipsCacheTableAdd() {
        ExecuteContextImpl context = new ExecuteContextImpl();

        CacheTableBindable cacheTableBindable = new CacheTableBindable("t", new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext ctx) {
                // simulate returning normally after the context was cancelled by an ancestor during execution
                context.cancel();
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[]{"x"});
                return Linq4j.asEnumerable(rows);
            }
        });

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> cacheTableBindable.bind(schema, context));
        assertTrue(ex.getMessage().contains("cancelled"));
        assertNull(schema.getTable("t", false), "cancelled branch should not add cache table to schema");
    }

    @Test
    public void testIgnoreExceptionDoesNotSwallowCancellation() {
        ExecuteContextImpl context = new ExecuteContextImpl();

        CacheTableBindable cacheTableBindable = new CacheTableBindable("t", new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext ctx) {
                // simulate a child node failing after the context was cancelled by an ancestor
                context.cancel();
                throw new RuntimeException("inner failed while cancelled");
            }
        });
        cacheTableBindable.setIgnoreException(true);

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> bindWithProxy(cacheTableBindable, schema, context));
        assertEquals("inner failed while cancelled", ex.getMessage());
    }

    @Test
    public void testRegularExceptionRecoveryIsOwnedByProxy() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "0");

        CacheTableBindable cacheTableBindable = new CacheTableBindable("t", new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext ctx) {
                throw new RuntimeException("regular failure");
            }
        });
        cacheTableBindable.setIgnoreException(true);

        CalciteSchema directSchema = CalciteSchema.createRootSchema(false);
        RuntimeException directFailure = assertThrows(RuntimeException.class,
                () -> cacheTableBindable.bind(directSchema, context));
        assertEquals("regular failure", directFailure.getMessage());
        assertNull(directSchema.getTable("t", false));

        CalciteSchema proxySchema = CalciteSchema.createRootSchema(false);
        Enumerable<Object[]> result = bindWithProxy(cacheTableBindable, proxySchema, context);
        List<Object[]> rows = new ArrayList<>();
        result.forEach(rows::add);
        assertEquals(1, rows.size());
        assertEquals("t", rows.get(0)[0]);
        assertEquals(0L, rows.get(0)[1]);
        assertNotNull(proxySchema.getTable("t", false));
    }

    @Test
    public void testInnerExceptionThroughProxyTimeoutPathPreservesCause() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "5000");

        CacheTableBindable cacheTableBindable = new CacheTableBindable("t", new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext ctx) {
                throw new RuntimeException("inner boom");
            }
        });

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> bindWithProxy(cacheTableBindable, schema, context));
        assertEquals("Node test_node execution failed", ex.getMessage());
        assertEquals("inner boom", ex.getCause().getMessage());
    }

    @Test
    public void testAncestorCancelledPropagatesThroughTimeoutPath() throws Exception {
        // the ancestor cancels the context, the child task observes it on the timeout path and throws;
        // the exception should propagate as-is (not become "Task execution timeout")
        ExecuteContextImpl context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "10000");

        PollingBindable inner = new PollingBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext ctx) {
                executed.set(true);
                try {
                    // simulate the ancestor cancelling during execution
                    context.cancel();
                    while (!ctx.isCancelled()) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    cancelledObserved.set(true);
                    throw new RuntimeException("child observed cancellation");
                } finally {
                    finished.countDown();
                }
            }
        };
        CacheTableBindable cacheTableBindable = new CacheTableBindable("t", inner);

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> bindWithProxy(cacheTableBindable, schema, context));
        assertEquals("child observed cancellation", ex.getMessage());
        assertTrue(inner.finished.await(5, TimeUnit.SECONDS));
    }

    private Enumerable<Object[]> bindWithProxy(
            CacheTableBindable cacheTableBindable,
            CalciteSchema schema,
            ExecuteContext context
    ) {
        BindableInterface proxy = ProxyAllBindable.wrap(cacheTableBindable);
        proxy.setName("test_node");
        return proxy.bind(schema, context);
    }
}
