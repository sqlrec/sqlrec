package com.sqlrec.utils;

import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class ExecutorServiceUtilsTest {

    @Test
    public void testGetExecutorServiceNotNull() {
        ExecutorService executor = ExecutorServiceUtils.getExecutorService();

        assertNotNull(executor);
    }

    @Test
    public void testGetExecutorServiceSingleton() {
        ExecutorService e1 = ExecutorServiceUtils.getExecutorService();
        ExecutorService e2 = ExecutorServiceUtils.getExecutorService();

        assertSame(e1, e2);
    }

    @Test
    public void testExecutorServiceCanExecute() throws Exception {
        ExecutorService executor = ExecutorServiceUtils.getExecutorService();
        int[] counter = {0};

        executor.submit(() -> counter[0]++).get();

        assertEquals(1, counter[0]);
    }

    @Test
    public void testCacheRefreshExecutorServiceSingleton() {
        ExecutorService e1 = ExecutorServiceUtils.getCacheRefreshExecutorService();
        ExecutorService e2 = ExecutorServiceUtils.getCacheRefreshExecutorService();

        assertSame(e1, e2);
    }

    @Test
    void unwrapsOnlyAsyncWrappersAndKeepsOriginalFailure() {
        RuntimeException original = new RuntimeException("original", new IllegalArgumentException());
        Throwable wrapped = new CompletionException(new ExecutionException(new CompletionException(original)));

        assertSame(original, ExecutorServiceUtils.unwrap(wrapped));
        assertSame(original, ExecutorServiceUtils.unwrap(original));
        ExecutionException withoutCause = new ExecutionException(null);
        assertSame(withoutCause, ExecutorServiceUtils.unwrap(withoutCause));
    }

    @Test
    void nonPositiveTimeoutExecutesInlineAndPositiveTimeoutUsesExecutor() throws Exception {
        Thread caller = Thread.currentThread();
        ExecuteContextImpl context = new ExecuteContextImpl(CalciteSchema.createRootSchema(false));

        assertSame(caller, ExecutorServiceUtils.execute(Thread::currentThread, context, 0));
        assertSame(caller, ExecutorServiceUtils.execute(Thread::currentThread, context, -1));
        assertNotSame(caller, ExecutorServiceUtils.execute(Thread::currentThread, context, 5000));
        assertFalse(context.isCancelled());
    }

    @Test
    void timeoutCancelsOnlyChildScope() {
        ExecuteContextImpl parent = new ExecuteContextImpl(CalciteSchema.createRootSchema(false));
        ExecuteContextImpl child = parent.clone();
        CountDownLatch release = new CountDownLatch(1);
        try {
            TimeoutException failure = assertThrows(TimeoutException.class, () -> ExecutorServiceUtils.execute(() -> {
                await(release);
                return null;
            }, child, 100));

            assertEquals("Task execution timeout after 100ms", failure.getMessage());
            assertInstanceOf(TimeoutException.class, failure.getCause());
            assertTrue(child.isCancelled());
            assertFalse(parent.isCancelled());
        } finally {
            release.countDown();
        }
    }

    @Test
    void interruptedWaitRestoresInterruptAndCancelsOnlyChild() {
        ExecuteContextImpl parent = new ExecuteContextImpl(CalciteSchema.createRootSchema(false));
        ExecuteContextImpl child = parent.clone();
        CountDownLatch release = new CountDownLatch(1);
        try {
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, () -> ExecutorServiceUtils.execute(() -> {
                await(release);
                return null;
            }, child, 5000));

            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(child.isCancelled());
            assertFalse(parent.isCancelled());
        } finally {
            Thread.interrupted();
            release.countDown();
        }
    }

    @Test
    void dependentTaskWaitsForAllDependenciesAndSkipsFailedDependencies() {
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        AtomicBoolean ran = new AtomicBoolean();
        CompletableFuture<String> result = ExecutorServiceUtils.submitAfter(List.of(first, second), () -> {
            ran.set(true);
            return "done";
        });
        first.complete(null);
        assertFalse(result.isDone());
        assertFalse(ran.get());
        second.complete(null);
        assertEquals("done", result.join());

        RuntimeException failure = new RuntimeException("dependency failed");
        CompletableFuture<Void> failed = CompletableFuture.failedFuture(failure);
        ran.set(false);
        CompletableFuture<String> skipped = ExecutorServiceUtils.submitAfter(List.of(failed), () -> {
            ran.set(true);
            return "unexpected";
        });
        assertSame(failure, ExecutorServiceUtils.unwrap(assertThrows(CompletionException.class, skipped::join)));
        assertFalse(ran.get());
    }

    @Test
    void failedGroupWaitPreservesCauseAndCancelsOnlyChildScope() {
        ExecuteContextImpl parent = new ExecuteContextImpl(CalciteSchema.createRootSchema(false));
        ExecuteContextImpl child = parent.clone();
        IllegalStateException failure = new IllegalStateException("failed");
        CompletableFuture<Void> failed = CompletableFuture.failedFuture(failure);
        CompletableFuture<Void> succeeded = CompletableFuture.completedFuture(null);

        CompletionException thrown = assertThrows(CompletionException.class,
                () -> ExecutorServiceUtils.awaitAll(child, List.of(failed, succeeded)));

        assertSame(failure, ExecutorServiceUtils.unwrap(thrown));
        assertTrue(child.isCancelled());
        assertFalse(parent.isCancelled());
    }

    @Test
    void failedGroupStopsWaitingForAnUnfinishedSibling() throws Exception {
        ExecuteContextImpl parent = new ExecuteContextImpl(CalciteSchema.createRootSchema(false));
        ExecuteContextImpl child = parent.clone();
        CompletableFuture<Void> pending = new CompletableFuture<>();
        CompletableFuture<Void> failed = new CompletableFuture<>();
        IllegalStateException original = new IllegalStateException("first failure");
        CompletableFuture<Void> waiter = CompletableFuture.runAsync(
                () -> ExecutorServiceUtils.awaitAll(child, List.of(pending, failed)));
        try {
            failed.completeExceptionally(original);
            ExecutionException thrown = assertThrows(ExecutionException.class,
                    () -> waiter.get(2, TimeUnit.SECONDS));
            assertSame(original, ExecutorServiceUtils.unwrap(thrown));
            assertTrue(pending.isCancelled());
            assertTrue(child.isCancelled());
            assertFalse(parent.isCancelled());
        } finally {
            pending.cancel(true);
            failed.cancel(true);
        }
    }

    @Test
    void interruptedGroupWaitPreservesInterruptAndCancelsOnlyItsScope() throws Exception {
        ExecuteContextImpl parent = new ExecuteContextImpl(CalciteSchema.createRootSchema(false));
        ExecuteContextImpl child = parent.clone();
        CompletableFuture<Void> pending = new CompletableFuture<>();
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicReference<Throwable> cause = new AtomicReference<>();
        Thread waiter = Thread.ofVirtual().start(() -> {
            started.countDown();
            try {
                ExecutorServiceUtils.awaitAll(child, List.of(pending));
            } catch (CompletionException failure) {
                cause.set(ExecutorServiceUtils.unwrap(failure));
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS));
            waiter.interrupt();
            waiter.join(2000);
            assertFalse(waiter.isAlive());
            assertInstanceOf(InterruptedException.class, cause.get());
            assertTrue(interrupted.get());
            assertTrue(pending.isCancelled());
            assertTrue(child.isCancelled());
            assertFalse(parent.isCancelled());
        } finally {
            pending.cancel(true);
            waiter.interrupt();
            waiter.join(2000);
        }
    }

    @Test
    void successfulGroupWaitRequiresEveryNodeToComplete() throws Exception {
        ExecuteContextImpl context = new ExecuteContextImpl(CalciteSchema.createRootSchema(false));
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        CompletableFuture<Void> waiter = CompletableFuture.runAsync(
                () -> ExecutorServiceUtils.awaitAll(context, List.of(first, second)));
        try {
            first.complete(null);
            assertFalse(waiter.isDone());
            second.complete(null);
            waiter.get(2, TimeUnit.SECONDS);
            assertFalse(context.isCancelled());
            assertFalse(first.isCancelled());
            assertFalse(second.isCancelled());
        } finally {
            first.cancel(true);
            second.cancel(true);
        }
    }

    @Test
    void alreadyCancelledRequestsCannotPassEmptyOrAlreadyCompletedWaits() {
        for (List<CompletableFuture<Void>> futures : List.of(
                List.<CompletableFuture<Void>>of(), List.of(CompletableFuture.<Void>completedFuture(null)))) {
            ExecuteContextImpl parent = new ExecuteContextImpl(CalciteSchema.createRootSchema(false));
            parent.cancel();
            assertThrows(java.util.concurrent.CancellationException.class,
                    () -> ExecutorServiceUtils.awaitAll(parent.clone(), futures));
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(failure);
        }
    }
}
