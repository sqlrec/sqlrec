package com.sqlrec.utils;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CacheUtilsTest {
    @Test
    void refreshReceivesPreviousValue() {
        ManualTicker ticker = new ManualTicker();
        QueuedExecutor executor = new QueuedExecutor();
        LoadingCache<String, String> cache = CacheUtils.createRefreshCache(
                Duration.ofSeconds(1),
                executor,
                ticker,
                (key, oldValue) -> oldValue == null ? "initial" : oldValue + "_updated"
        );

        assertEquals("initial", cache.get("key"));
        ticker.advance(Duration.ofMillis(1100));
        assertEquals("initial", cache.get("key"));

        executor.runAll();
        assertEquals("initial_updated", cache.get("key"));
    }

    @Test
    void failedRefreshWaitsUntilNextInterval() {
        ManualTicker ticker = new ManualTicker();
        QueuedExecutor executor = new QueuedExecutor();
        AtomicInteger loadCount = new AtomicInteger();
        LoadingCache<String, String> cache = CacheUtils.createRefreshCache(
                Duration.ofSeconds(1),
                executor,
                ticker,
                key -> {
                    if (loadCount.incrementAndGet() == 1) {
                        return "value";
                    }
                    throw new RuntimeException("load failed");
                }
        );

        assertEquals("value", cache.get("key"));
        ticker.advance(Duration.ofMillis(1100));
        assertEquals("value", cache.get("key"));
        executor.runAll();

        ticker.advance(Duration.ofMillis(500));
        assertEquals("value", cache.get("key"));
        assertEquals(0, executor.size());

        ticker.advance(Duration.ofMillis(600));
        assertEquals("value", cache.get("key"));
        assertEquals(1, executor.size());
    }

    @Test
    void singleValueCacheDoesNotExposeInternalKey() {
        ManualTicker ticker = new ManualTicker();
        QueuedExecutor executor = new QueuedExecutor();
        CacheUtils.SingleValueCache<String> cache = CacheUtils.createSingleValueRefreshCache(
                Duration.ofSeconds(1),
                executor,
                ticker,
                oldValue -> oldValue == null ? "initial" : oldValue + "_updated"
        );

        assertEquals("initial", cache.get());
        ticker.advance(Duration.ofMillis(1100));
        assertEquals("initial", cache.get());

        executor.runAll();
        assertEquals("initial_updated", cache.get());
    }

    private static final class ManualTicker implements Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        private void advance(Duration duration) {
            nanos.addAndGet(duration.toNanos());
        }
    }

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        private int size() {
            return tasks.size();
        }

        private void runAll() {
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
            }
        }
    }
}
