package com.sqlrec.connectors.redis.client;

import com.sqlrec.common.utils.SilenceLoggers;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.resource.ClientResources;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisConnectionRegistryTest {
    @Test
    void sharedIoThreadsRemainUsableAfterTheCreatingJobCloses() throws Exception {
        ClassLoader owner = RedisConnectionRegistry.class.getClassLoader();
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        var job = FlinkUserCodeClassLoaders.parentFirst(new URL[0], owner, null, true);
        AtomicReference<ClientResources> clientResources = new AtomicReference<>();
        StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
        RedisConnectionRegistry<RedisClient, StatefulRedisConnection<byte[], byte[]>> registry =
                new RedisConnectionRegistry<>("classloader-test", (resources, url) -> {
                    RedisClient client = RedisClient.create(resources, url);
                    clientResources.set(client.getResources());
                    return client;
                }, ignored -> connection);
        try {
            Thread.currentThread().setContextClassLoader(job);
            registry.getConnection("redis://localhost:6379");
            assertSame(job, Thread.currentThread().getContextClassLoader());
            job.close();
            // Start an I/O thread lazily, even while the caller still carries the closed job loader.
            var io = clientResources.get().eventLoopGroupProvider().allocate(NioEventLoopGroup.class);
            ClassLoader workerLoader = io.submit(() -> {
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                assertNotNull(loader.getResource("com/sqlrec/common/schema/FieldSchema.class"));
                return loader;
            }).get(5, TimeUnit.SECONDS);
            assertSame(owner, workerLoader);
            assertSame(connection, registry.getConnection("redis://localhost:6379"));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
            registry.invalidateAll();
            job.close();
        }
    }

    @Test
    void clientCreationFailureReleasesOwnedThreadResources() {
        RuntimeException failure = new RuntimeException("create failed");
        AtomicReference<ClientResources> threadResources = new AtomicReference<>();
        RedisConnectionRegistry<RedisClient, StatefulRedisConnection<byte[], byte[]>> registry =
                new RedisConnectionRegistry<>("test", (resources, url) -> {
                    threadResources.set(resources);
                    throw failure;
                }, client -> fail("must not connect after client creation failed"));
        assertSame(failure, assertThrows(RuntimeException.class, () -> registry.getConnection("url")));
        assertTrue(threadResources.get().eventExecutorGroup().isShuttingDown());
    }

    @Test
    void concurrentLookupsShareOneResourcePairAndInvalidateAllowsReconnection() throws Exception {
        RedisClient client = mock(RedisClient.class);
        StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
        AtomicInteger creations = new AtomicInteger();
        RedisConnectionRegistry<RedisClient, StatefulRedisConnection<byte[], byte[]>> registry =
                new RedisConnectionRegistry<>("test", (resources, url) -> { creations.incrementAndGet(); return client; },
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
        List<ClientResources> threadResources = new ArrayList<>();
        RedisConnectionRegistry<RedisClient, StatefulRedisConnection<byte[], byte[]>> registry =
                new RedisConnectionRegistry<>("test",
                        (resources, url) -> {
                            threadResources.add(resources);
                            return creations.getAndIncrement() == 0 ? failedClient : healthyClient;
                        },
                        client -> { if (client == failedClient) throw failure; return connection; });

        assertSame(failure, assertThrows(RuntimeException.class, () -> registry.getConnection("url")));
        verify(failedClient).shutdown();
        assertTrue(threadResources.get(0).eventExecutorGroup().isShuttingDown());
        assertSame(connection, registry.getConnection("url"));
        assertEquals(2, creations.get());
        registry.invalidateAll();
        verify(healthyClient).shutdown();
        assertTrue(threadResources.get(1).eventExecutorGroup().isShuttingDown());
    }

    @Test
    @SilenceLoggers(RedisConnectionRegistry.class)
    void closeFailureStillShutsDownTheClientAndRemovesTheEntry() {
        RedisClient client = mock(RedisClient.class);
        StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
        doThrow(new RuntimeException("close failed")).when(connection).close();
        AtomicInteger creations = new AtomicInteger();
        RedisConnectionRegistry<RedisClient, StatefulRedisConnection<byte[], byte[]>> registry =
                new RedisConnectionRegistry<>("test", (resources, url) -> { creations.incrementAndGet(); return client; },
                        ignored -> connection);

        registry.getConnection("url");
        registry.invalidate("url");
        verify(client).shutdown();
        registry.getConnection("url");
        assertEquals(2, creations.get());
        registry.invalidateAll();
    }
}
