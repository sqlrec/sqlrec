package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.schema.ConcurrentCalciteSchema;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TimeinCacheCommitTest {
    private static final List<RelDataTypeField> FIELDS = List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));

    private abstract static class Source extends BindableInterface {
        @Override public List<RelDataTypeField> getReturnDataFields() { return FIELDS; }
        @Override public boolean isParallelizable() { return true; }
        @Override public Set<String> getReadTables() { return Set.of(); }
        @Override public Set<String> getWriteTables() { return Set.of(); }
    }

    @Test
    void aTimedOutComputationCannotChangeTheFallbackAfterItFinishes() throws Exception {
        CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        IfBindable conditional = conditional(100, new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                worker.set(Thread.currentThread());
                started.countDown();
                await(release); // Deliberately does not inspect cancellation.
                return rows(1);
            }
        }, constant(2));
        ExecuteContextImpl context = new ExecuteContextImpl(schema);
        try {
            conditional.bind(schema, context);
            assertTrue(started.await(2, TimeUnit.SECONDS));
            assertEquals(2, value(schema));
            release.countDown();
            worker.get().join(2000);
            assertFalse(worker.get().isAlive());
            assertEquals(2, value(schema));
            assertFalse(context.isCancelled());
        } finally {
            release.countDown();
            if (worker.get() != null) worker.get().join(2000);
        }
    }

    @Test
    void lazyEvaluationFailureRunsElseBeforePublishingAnyPrimaryData() {
        CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
        AtomicBoolean primaryVisible = new AtomicBoolean();
        Source lazy = new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) {
                return rows(1).select(row -> {
                    primaryVisible.set(s.getTable("output_table", false) != null);
                    throw new IllegalStateException("lazy failure");
                });
            }
        };
        conditional(5000, lazy, constant(2)).bind(schema, new ExecuteContextImpl(schema));
        assertFalse(primaryVisible.get());
        assertEquals(2, value(schema));
    }

    @Test
    void cachePublicationFailureStillRunsElse() {
        CalciteSchema schema = spy(ConcurrentCalciteSchema.createRootSchema());
        doAnswer(invocation -> {
            CacheTable table = invocation.getArgument(1);
            if ((int) table.scan(null).first()[0] == 1) throw new IllegalStateException("store failure");
            return invocation.callRealMethod();
        }).when(schema).add(eq("output_table"), any(Table.class));
        conditional(5000, constant(1), constant(2)).bind(schema, new ExecuteContextImpl(schema));
        assertEquals(2, value(schema));
    }

    private static IfBindable conditional(int timeout, Source primary, Source fallback) {
        CalciteBindable condition = new CalciteBindable(Map.of(), data -> rows(timeout), null, null, null, null, null);
        return new IfBindable(condition, new CacheTableBindable("output_table", primary),
                new CacheTableBindable("output_table", fallback), true);
    }
    private static Source constant(int value) {
        return new Source() {
            @Override public Enumerable<Object[]> bind(CalciteSchema s, ExecuteContext c) { return rows(value); }
        };
    }
    private static Enumerable<Object[]> rows(int value) { return Linq4j.singletonEnumerable(new Object[]{value}); }
    private static Object value(CalciteSchema schema) {
        return ((CacheTable) schema.getTable("output_table", false).getTable()).scan(null).first()[0];
    }
    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("gate timeout");
        } catch (InterruptedException failure) {
            throw new RuntimeException(failure);
        }
    }
}
