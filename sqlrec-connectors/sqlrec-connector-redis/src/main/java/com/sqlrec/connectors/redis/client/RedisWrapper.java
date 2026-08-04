package com.sqlrec.connectors.redis.client;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisWrapper implements AbstractRedisWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(RedisWrapper.class);
    static Map<String, RedisClient> redisClientMap = new ConcurrentHashMap<>();
    static Map<String, StatefulRedisConnection<byte[], byte[]>> connectionMap = new ConcurrentHashMap<>();

    static {
        // Close all shared clients/connections on JVM shutdown (the previous no-op close()
        // relied on this but never registered a hook).
        Runtime.getRuntime().addShutdownHook(new Thread(RedisWrapper::invalidateAll, "RedisWrapper-shutdown"));
    }

    private String url;

    @Override
    public void open(String url) {
        this.url = url;
    }

    private static synchronized void openRedisClient(String url) {
        if (connectionMap.containsKey(url)) {
            return;
        }

        RedisURI redisURI = RedisURI.create(url);
        RedisClient redisClient = RedisClient.create(redisURI);
        try {
            StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
            redisClientMap.put(url, redisClient);
            connectionMap.put(url, connection);
        } catch (RuntimeException e) {
            // connect() failed: shut down the half-created client to avoid leaking its
            // NioEventLoopGroup / file descriptors. The entry is not cached, so the next
            // attempt will create a fresh client.
            try {
                redisClient.shutdown();
            } catch (Exception shutdownEx) {
                LOG.warn("Failed to shut down RedisClient after connect failure: {}", shutdownEx.getMessage());
            }
            throw e;
        }
    }

    private RedisAsyncCommands<byte[], byte[]> getCommands() {
        if (!connectionMap.containsKey(url)) {
            openRedisClient(url);
        }
        return connectionMap.get(url).async();
    }

    @Override
    public void close() {
        // Connection pool is shared across all instances with the same URL.
        // Do not close the connection here as other instances may still be using it.
        // Use invalidate() to force-close a broken connection, or rely on the JVM shutdown hook.
    }

    @Override
    public void invalidate() {
        invalidate(url);
    }

    /**
     * Close and remove the shared client/connection cached for {@code url}. Idempotent.
     */
    public static synchronized void invalidate(String url) {
        StatefulRedisConnection<byte[], byte[]> connection = connectionMap.remove(url);
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Redis connection for {}: {}", url, e.getMessage());
            }
        }
        RedisClient redisClient = redisClientMap.remove(url);
        if (redisClient != null) {
            try {
                redisClient.shutdown();
            } catch (Exception e) {
                LOG.warn("Failed to shut down RedisClient for {}: {}", url, e.getMessage());
            }
        }
    }

    /**
     * Close and remove all cached clients/connections.
     */
    public static synchronized void invalidateAll() {
        for (String url : redisClientMap.keySet()) {
            invalidate(url);
        }
    }

    /**
     * Test-only: inject a mock connection and client for a given url.
     * Does not close existing entries; call {@link #invalidate(String)} beforehand if needed.
     */
    static void setConnectionForTest(
            String url,
            StatefulRedisConnection<byte[], byte[]> mockConn,
            RedisClient mockClient) {
        connectionMap.put(url, mockConn);
        redisClientMap.put(url, mockClient);
    }

    public RedisFuture<List<byte[]>> lrange(byte[] key, long start, long end) {
        return getCommands().lrange(key, start, end);
    }

    public RedisFuture<byte[]> get(byte[] key) {
        return getCommands().get(key);
    }

    public RedisFuture<List<KeyValue<byte[], byte[]>>> mget(byte[]... keys) {
        return getCommands().mget(keys);
    }

    public RedisFuture<String> set(byte[] key, byte[] value) {
        return getCommands().set(key, value);
    }

    public RedisFuture<Long> del(byte[] key) {
        return getCommands().del(key);
    }

    public RedisFuture<Long> lpush(byte[] key, byte[]... values) {
        return getCommands().lpush(key, values);
    }

    public RedisFuture<Long> lrem(byte[] key, byte[] value) {
        return getCommands().lrem(key, 0, value);
    }

    public RedisFuture<String> ltrim(byte[] key, long start, long stop) {
        return getCommands().ltrim(key, start, stop);
    }

    public RedisFuture<Boolean> expire(byte[] key, long seconds) {
        return getCommands().expire(key, seconds);
    }

    public RedisFuture<String> mset(Map<byte[], byte[]> kvMap) {
        return getCommands().mset(kvMap);
    }
}
