package com.sqlrec.utils;

import com.sqlrec.common.runtime.ExecuteContext;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Shared executors and async execution mechanics; callers own recovery and result handling. */
public final class ExecutorServiceUtils {
    private ExecutorServiceUtils() {
    }

    private static final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private static final ExecutorService cacheRefreshExecutorService =
            Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "cache-refresh");
                thread.setDaemon(true);
                return thread;
            });

    public static ExecutorService getExecutorService() {
        return executorService;
    }

    public static ExecutorService getCacheRefreshExecutorService() {
        return cacheRefreshExecutorService;
    }

    public static <T> CompletableFuture<T> submit(Supplier<T> task) {
        return CompletableFuture.supplyAsync(task, executorService);
    }

    public static <T> CompletableFuture<T> submitAfter(
            Collection<? extends CompletableFuture<?>> dependencies, Supplier<T> task) {
        if (dependencies.isEmpty()) {
            return submit(task);
        }
        return allOf(dependencies).thenApplyAsync(
                ignored -> task.get(), executorService);
    }

    /** Non-positive timeouts execute inline. Failed waits cancel only the supplied scope. */
    public static <T> T execute(Supplier<T> task, ExecuteContext context, long timeoutMillis)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (timeoutMillis <= 0) {
            return task.get();
        }
        CompletableFuture<T> future = submit(task);
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException failure) {
            cancel(context, List.of(future));
            Thread.currentThread().interrupt();
            throw failure;
        } catch (TimeoutException failure) {
            cancel(context, List.of(future));
            TimeoutException timeout = new TimeoutException(
                    "Task execution timeout after " + timeoutMillis + "ms");
            timeout.initCause(failure);
            throw timeout;
        } catch (ExecutionException | RuntimeException | Error failure) {
            cancel(context, List.of(future));
            throw failure;
        }
    }

    public static void awaitAll(ExecuteContext context, Collection<? extends CompletableFuture<?>> futures) {
        try {
            allOf(futures).join();
        } catch (RuntimeException | Error failure) {
            cancel(context, futures);
            throw failure;
        }
    }

    public static void cancel(ExecuteContext context, Collection<? extends Future<?>> futures) {
        // CompletableFuture cancellation alone does not stop running suppliers.
        // Tasks cooperate by observing this scope's cancellation flag.
        context.cancel();
        futures.forEach(future -> future.cancel(true));
    }

    public static Throwable unwrap(Throwable failure) {
        while ((failure instanceof ExecutionException || failure instanceof CompletionException)
                && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static CompletableFuture<Void> allOf(Collection<? extends CompletableFuture<?>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
    }
}
