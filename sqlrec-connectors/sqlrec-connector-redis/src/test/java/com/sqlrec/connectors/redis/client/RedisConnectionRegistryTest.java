package com.sqlrec.connectors.redis.client;

import com.sqlrec.common.utils.SilenceLoggers;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisConnectionRegistryTest {
    @Test
    void concurrentLookupsShareOneResourcePairAndInvalidateAllowsReconnection() throws Exception {
        RedisClient client = mock(RedisClient.class);
        StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
        AtomicInteger creations = new AtomicInteger();
        RedisConnectionRegistry<RedisClient, StatefulRedisConnection<byte[], byte[]>> registry =
                new RedisConnectionRegistry<>("test", url -> { creations.incrementAndGet(); return client; },
                        ignored -> connection);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<StatefulRedisConnection<byte[], byte[]>>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(() -> { start.await(); return registry.getConnection("url"); }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                assertSame(connection, future.get(5, TimeUnit.SECONDS));
            }
            assertEquals(1, creations.get());
            registry.invalidate("url");
            registry.invalidate("url");
            verify(connection).close();
            verify(client).shutdown();
            assertSame(connection, registry.getConnection("url"));
            assertEquals(2, creations.get());
        } finally {
            start.countDown();
            executor.shutdownNow();
            registry.invalidateAll();
        }
    }

    @Test
    @SilenceLoggers(RedisConnectionRegistry.class)
    void connectionFailureIsNotCachedAndCleanupFailureDoesNotHideTheCause() {
        RedisClient failedClient = mock(RedisClient.class);
        RedisClient healthyClient = mock(RedisClient.class);
        StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
        RuntimeException failure = new RuntimeException("connect failed");
        doThrow(new RuntimeException("shutdown failed")).when(failedClient).shutdown();
        AtomicInteger creations = new AtomicInteger();
        RedisConnectionRegistry<RedisClient, StatefulRedisConnection<byte[], byte[]>> registry =
                new RedisConnectionRegistry<>("test",
                        url -> creations.getAndIncrement() == 0 ? failedClient : healthyClient,
                        client -> { if (client == failedClient) throw failure; return connection; });

        assertSame(failure, assertThrows(RuntimeException.class, () -> registry.getConnection("url")));
        verify(failedClient).shutdown();
        assertSame(connection, registry.getConnection("url"));
        assertEquals(2, creations.get());
        registry.invalidateAll();
        verify(healthyClient).shutdown();
    }

    @Test
    @SilenceLoggers(RedisConnectionRegistry.class)
    void closeFailureStillShutsDownTheClientAndRemovesTheEntry() {
        RedisClient client = mock(RedisClient.class);
        StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
        doThrow(new RuntimeException("close failed")).when(connection).close();
        AtomicInteger creations = new AtomicInteger();
        RedisConnectionRegistry<RedisClient, StatefulRedisConnection<byte[], byte[]>> registry =
                new RedisConnectionRegistry<>("test", url -> { creations.incrementAndGet(); return client; },
                        ignored -> connection);

        registry.getConnection("url");
        registry.invalidate("url");
        verify(client).shutdown();
        registry.getConnection("url");
        assertEquals(2, creations.get());
        registry.invalidateAll();
    }
}
