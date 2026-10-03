package com.sqlrec.connectors.redis.client;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Owns shared client/connection pairs for one Redis mode, keyed by URL. */
final class RedisConnectionRegistry<C extends AbstractRedisClient,
        S extends StatefulConnection<byte[], byte[]>> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisConnectionRegistry.class);
    private final String name;
    private final Function<String, C> createClient;
    private final Function<C, S> connect;
    private final Map<String, Resources<C, S>> resources = new ConcurrentHashMap<>();

    RedisConnectionRegistry(String name, Function<String, C> createClient, Function<C, S> connect) {
        this.name = name;
        this.createClient = createClient;
        this.connect = connect;
    }

    void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::invalidateAll, name + "-shutdown"));
    }

    S getConnection(String url) {
        Resources<C, S> entry = resources.get(url);
        return entry == null ? createConnection(url) : entry.connection;
    }

    private synchronized S createConnection(String url) {
        Resources<C, S> entry = resources.get(url);
        if (entry == null) {
            C client = createClient.apply(url);
            S connection;
            try {
                connection = connect.apply(client);
            } catch (RuntimeException failure) {
                // Never cache a half-created client; a later command can retry connection setup.
                shutdown(client);
                throw failure;
            }
            entry = new Resources<>(client, connection);
            resources.put(url, entry);
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
        shutdown(entry.client);
    }

    synchronized void invalidateAll() {
        for (String url : resources.keySet().toArray(new String[0])) {
            invalidate(url);
        }
    }

    /** Test-only injection. Call invalidate first when replacing an existing entry. */
    synchronized void setConnectionForTest(String url, S connection, C client) {
        resources.put(url, new Resources<>(client, connection));
    }

    private void shutdown(C client) {
        try {
            client.shutdown();
        } catch (Exception e) {
            LOG.warn("Failed to shut down {} client: {}", name, e.getMessage());
        }
    }

    private static final class Resources<C, S> {
        private final C client;
        private final S connection;

        private Resources(C client, S connection) {
            this.client = client;
            this.connection = connection;
        }
    }
}
