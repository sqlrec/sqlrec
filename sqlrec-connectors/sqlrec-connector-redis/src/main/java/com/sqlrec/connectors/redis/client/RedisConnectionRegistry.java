package com.sqlrec.connectors.redis.client;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Owns shared client/connection pairs for one Redis mode, keyed by URL. */
final class RedisConnectionRegistry<C extends AbstractRedisClient,
        S extends StatefulConnection<byte[], byte[]>> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisConnectionRegistry.class);
    private final String name;
    private final BiFunction<ClientResources, String, C> createClient;
    private final Function<C, S> connect;
    private final Map<String, Resources<C, S>> resources = new ConcurrentHashMap<>();

    RedisConnectionRegistry(String name, BiFunction<ClientResources, String, C> createClient, Function<C, S> connect) {
        this.name = name;
        this.createClient = createClient;
        this.connect = connect;
    }

    void registerShutdownHook() {
        Thread hook = new Thread(this::invalidateAll, name + "-shutdown");
        hook.setContextClassLoader(RedisConnectionRegistry.class.getClassLoader());
        Runtime.getRuntime().addShutdownHook(hook);
    }

    S getConnection(String url) {
        Resources<C, S> entry = resources.get(url);
        return entry == null ? createConnection(url) : entry.connection;
    }

    private synchronized S createConnection(String url) {
        Resources<C, S> entry = resources.get(url);
        if (entry == null) {
            ClientResources threadResources = createClientResources();
            C client = null;
            try {
                client = createClient.apply(threadResources, url);
                S connection = connect.apply(client);
                entry = new Resources<>(client, connection, threadResources);
                resources.put(url, entry);
            } catch (RuntimeException | Error failure) {
                // Never cache a half-created client; a later command can retry connection setup.
                shutdown(client, threadResources);
                throw failure;
            }
        }
        return entry.connection;
    }

    synchronized void invalidate(String url) {
        Resources<C, S> entry = resources.remove(url);
        if (entry == null) {
            return;
        }
        try {
            entry.connection.close();
        } catch (Exception e) {
            LOG.warn("Failed to close {} connection: {}", name, e.getMessage());
        }
        shutdown(entry.client, entry.threadResources);
    }

    synchronized void invalidateAll() {
        for (String url : resources.keySet().toArray(new String[0])) {
            invalidate(url);
        }
    }

    /** Test-only injection. Call invalidate first when replacing an existing entry. */
    synchronized void setConnectionForTest(String url, S connection, C client) {
        resources.put(url, new Resources<>(client, connection, null));
    }

    private static ClientResources createClientResources() {
        return DefaultClientResources.builder().threadFactoryProvider(poolName -> {
            ThreadFactory factory = new DefaultThreadFactory(poolName, true);
            return task -> {
                Thread thread = factory.newThread(task);
                // Shared Redis threads outlive individual Flink jobs, including lazily started threads.
                thread.setContextClassLoader(RedisConnectionRegistry.class.getClassLoader());
                return thread;
            };
        }).build();
    }

    private void shutdown(C client, ClientResources threadResources) {
        try {
            if (client != null) client.shutdown();
        } catch (Exception e) {
            LOG.warn("Failed to shut down {} client: {}", name, e.getMessage());
        }
        // Lettuce does not close externally supplied ClientResources when the client shuts down.
        try {
            if (threadResources != null) threadResources.shutdown();
        } catch (Exception e) {
            LOG.warn("Failed to shut down {} thread resources: {}", name, e.getMessage());
        }
    }

    private static final class Resources<C, S> {
        private final C client;
        private final S connection;
        private final ClientResources threadResources;

        private Resources(C client, S connection, ClientResources threadResources) {
            this.client = client;
            this.connection = connection;
            this.threadResources = threadResources;
        }
    }
}
