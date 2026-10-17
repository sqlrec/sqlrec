package com.sqlrec.runtime;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.schema.ConcurrentCalciteSchema;
import com.sqlrec.utils.ExecutorServiceUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CachePublicationTest {
    private static final List<RelDataTypeField> FIELDS = List.of(
            DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));

    @Test
    void contextRequiresASchemaAtConstruction() {
        assertThrows(NullPointerException.class, () -> new ExecuteContextImpl((CalciteSchema) null));
    }

    @Test
    void cancellationCannotPassAnInProgressPublicationCheck() throws Exception {
        for (boolean cancelParent : List.of(false, true)) {
            CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
            ExecuteContextImpl root = new ExecuteContextImpl(schema);
            ExecuteContextImpl node = root.clone();
            ExecuteContextImpl worker = spy(node.clone());
            CountDownLatch checked = new CountDownLatch(1), release = new CountDownLatch(1);
            CountDownLatch cancelling = new CountDownLatch(1);
            doAnswer(invocation -> {
                boolean cancelled = (boolean) invocation.callRealMethod();
                checked.countDown();
                await(release); // Pause after checking, before schema.add.
                return cancelled;
            }).when(worker).isCancelled();
            CompletableFuture<?> publication = ExecutorServiceUtils.submit(() -> {
                CacheTableBindable.publish(schema, worker, table(1));
                return null;
            });
            CompletableFuture<?> cancellation = null;
            try {
                assertTrue(checked.await(2, TimeUnit.SECONDS));
                cancellation = ExecutorServiceUtils.submit(() -> {
                    cancelling.countDown();
                    (cancelParent ? node : worker).cancel();
                    return null;
                });
                assertTrue(cancelling.await(2, TimeUnit.SECONDS));
                CompletableFuture<?> pending = cancellation;
                assertThrows(TimeoutException.class, () -> pending.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                publication.get(2, TimeUnit.SECONDS);
                cancellation.get(2, TimeUnit.SECONDS);

                // Recovery starts only after cancellation, so it must remain the last write.
                CacheTableBindable.publish(schema, root, table(2));
                assertTrue(worker.isCancelled());
                assertFalse(root.isCancelled());
                assertEquals(2, value(schema));
                assertThrows(RuntimeException.class,
                        () -> CacheTableBindable.publish(schema, worker, table(3)));
                assertEquals(2, value(schema));
            } finally {
                release.countDown();
                publication.get(5, TimeUnit.SECONDS);
                if (cancellation != null) cancellation.get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void cancelledContextCannotCreateOrReplaceAnEmptyTable() {
        for (boolean existing : List.of(false, true)) {
            CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
            if (existing) schema.add("output", table(9));
            ExecuteContextImpl context = new ExecuteContextImpl(schema);
            context.cancel();
            CacheTableBindable cache = new CacheTableBindable("output", new ReturnBindable("input", FIELDS));

            assertThrows(RuntimeException.class, () -> cache.ensureTable(schema, context));
            assertThrows(RuntimeException.class, () -> CacheTableBindable.publish(schema, context, table(0)));
            if (existing) assertEquals(9, value(schema));
            else assertNull(schema.getTable("output", false));
        }
    }

    @Test
    void isolatedSchemaCancellationDoesNotWaitForTheCallerSchema() throws Exception {
        CalciteSchema callerSchema = ConcurrentCalciteSchema.createRootSchema();
        CalciteSchema calleeSchema = ConcurrentCalciteSchema.createRootSchema();
        ExecuteContextImpl caller = new ExecuteContextImpl(callerSchema);
        ExecuteContextImpl callee = caller.clone(calleeSchema);

        synchronized (callerSchema) {
            ExecutorServiceUtils.submit(() -> {
                CacheTableBindable.publish(calleeSchema, callee, table(1));
                callee.cancel();
                return null;
            }).get(2, TimeUnit.SECONDS);
        }
        assertTrue(callee.isCancelled());
        assertFalse(caller.isCancelled());
        assertEquals(1, value(calleeSchema));
        assertNull(callerSchema.getTable("output", false));
    }

    @Test
    void returnIsolationKeepsTheSchemaSynchronizationBoundary() throws Exception {
        CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
        ExecuteContextImpl parent = new ExecuteContextImpl(schema);
        ExecuteContextImpl isolated = parent.createFunctionContext().createIsolatedReturnContext();
        CompletableFuture<?> cancellation;
        CountDownLatch started = new CountDownLatch(1);
        synchronized (schema) {
            cancellation = ExecutorServiceUtils.submit(() -> {
                started.countDown();
                isolated.cancel();
                return null;
            });
            assertTrue(started.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> cancellation.get(100, TimeUnit.MILLISECONDS));
        }
        cancellation.get(2, TimeUnit.SECONDS);
        assertTrue(isolated.isCancelled());
        assertFalse(parent.isCancelled());
    }

    @Test
    void isolatedSchemaKeepsAncestorCancellation() {
        CalciteSchema first = ConcurrentCalciteSchema.createRootSchema();
        CalciteSchema second = ConcurrentCalciteSchema.createRootSchema();
        ExecuteContextImpl parent = new ExecuteContextImpl(first);
        ExecuteContextImpl child = parent.clone(second);
        parent.cancel();
        assertTrue(child.isCancelled());
        assertThrows(RuntimeException.class, () -> CacheTableBindable.publish(second, child, table(1)));
    }

    private static CacheTable table(int value) {
        return new CacheTable("output", Linq4j.singletonEnumerable(new Object[]{value}), FIELDS);
    }

    private static Object value(CalciteSchema schema) {
        return ((CacheTable) schema.getTable("output", false).getTable()).scan(null).single()[0];
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("latch timeout");
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(failure);
        }
    }
}
