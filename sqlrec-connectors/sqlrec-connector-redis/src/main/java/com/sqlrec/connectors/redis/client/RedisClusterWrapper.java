package com.sqlrec.connectors.redis.client;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class RedisClusterWrapper implements AbstractRedisWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterWrapper.class);
    private static Map<String, RedisClusterClient> redisClientMap = new ConcurrentHashMap<>();
    private static Map<String, StatefulRedisClusterConnection<byte[], byte[]>> connectionMap = new ConcurrentHashMap<>();
    /** Locks serializing pipelined sections per url: autoFlush is connection-global. */
    private static final Map<String, Object> pipelineLockMap = new ConcurrentHashMap<>();

    static {
        // Close all shared clients/connections on JVM shutdown (the previous no-op close()
        // relied on this but never registered a hook).
        Runtime.getRuntime().addShutdownHook(new Thread(RedisClusterWrapper::invalidateAll, "RedisClusterWrapper-shutdown"));
    }

    private String url;

    @Override
    public void open(String url) {
        this.url = url;
    }

    private static synchronized void openRedisClusterClient(String url) {
        if (connectionMap.containsKey(url)) {
            return;
        }

        RedisClusterClient redisClient = RedisClusterClient.create(url);
        try {
            StatefulRedisClusterConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
            redisClientMap.put(url, redisClient);
            connectionMap.put(url, connection);
        } catch (RuntimeException e) {
            // connect() failed: shut down the half-created client to avoid leaking its
            // NioEventLoopGroup / file descriptors. The entry is not cached, so the next
            // attempt will create a fresh client.
            try {
                redisClient.shutdown();
            } catch (Exception shutdownEx) {
                LOG.warn("Failed to shut down RedisClusterClient after connect failure: {}", shutdownEx.getMessage());
            }
            throw e;
        }
    }

    private StatefulRedisClusterConnection<byte[], byte[]> getConnection() {
        if (!connectionMap.containsKey(url)) {
            openRedisClusterClient(url);
        }
        return connectionMap.get(url);
    }

    private RedisAdvancedClusterAsyncCommands<byte[], byte[]> getCommands() {
        return getConnection().async();
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
     * Close and remove the shared cluster client/connection cached for {@code url}. Idempotent.
     */
    public static synchronized void invalidate(String url) {
        StatefulRedisClusterConnection<byte[], byte[]> connection = connectionMap.remove(url);
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Redis cluster connection for {}: {}", url, e.getMessage());
            }
        }
        RedisClusterClient redisClient = redisClientMap.remove(url);
        if (redisClient != null) {
            try {
                redisClient.shutdown();
            } catch (Exception e) {
                LOG.warn("Failed to shut down RedisClusterClient for {}: {}", url, e.getMessage());
            }
        }
        pipelineLockMap.remove(url);
    }

    /**
     * Close and remove all cached clients/connections.
     */
    public static synchronized void invalidateAll() {
        for (String url : redisClientMap.keySet()) {
            invalidate(url);
        }
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

    @Override
    public RedisFuture<String> setex(byte[] key, byte[] value, long ttlSeconds) {
        // Cluster mode routes each SET (with its EX argument) to the key's slot,
        // so unlike MSET this works for keys spanning multiple slots.
        return getCommands().set(key, value, SetArgs.Builder.ex(ttlSeconds));
    }

    @Override
    public List<RedisFuture<?>> executePipelined(Supplier<List<RedisFuture<?>>> commandProducer) {
        StatefulRedisClusterConnection<byte[], byte[]> connection = getConnection();
        Object lock = pipelineLockMap.computeIfAbsent(url, k -> new Object());
        // autoFlush is global to the shared cluster connection, so pipelined sections
        // for the same url must not interleave: other threads' commands issued inside
        // this window are simply buffered and sent by our flushCommands().
        synchronized (lock) {
            connection.setAutoFlushCommands(false);
            try {
                List<RedisFuture<?>> futures = commandProducer.get();
                connection.flushCommands();
                return futures;
            } finally {
                connection.setAutoFlushCommands(true);
            }
        }
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
}
