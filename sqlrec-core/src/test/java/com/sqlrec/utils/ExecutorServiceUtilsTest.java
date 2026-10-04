package com.sqlrec.utils;

import com.sqlrec.runtime.ExecuteContextImpl;
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
        ExecuteContextImpl context = new ExecuteContextImpl();

        assertSame(caller, ExecutorServiceUtils.execute(Thread::currentThread, context, 0));
        assertSame(caller, ExecutorServiceUtils.execute(Thread::currentThread, context, -1));
        assertNotSame(caller, ExecutorServiceUtils.execute(Thread::currentThread, context, 5000));
        assertFalse(context.isCancelled());
    }

    @Test
    void timeoutCancelsOnlyChildScope() {
        ExecuteContextImpl parent = new ExecuteContextImpl();
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
        ExecuteContextImpl parent = new ExecuteContextImpl();
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
        ExecuteContextImpl parent = new ExecuteContextImpl();
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

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(failure);
        }
    }
}
