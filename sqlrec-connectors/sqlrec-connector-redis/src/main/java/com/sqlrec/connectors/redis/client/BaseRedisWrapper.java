package com.sqlrec.connectors.redis.client;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

import java.util.List;

/** Shared lazy connection access and command forwarding for standalone and cluster Redis. */
public abstract class BaseRedisWrapper<C extends AbstractRedisClient,
        S extends StatefulConnection<byte[], byte[]>> {
    private final RedisConnectionRegistry<C, S> connections;
    private String url;

    BaseRedisWrapper(RedisConnectionRegistry<C, S> connections) {
        this.connections = connections;
    }

    protected abstract RedisClusterAsyncCommands<byte[], byte[]> commands(S connection);

    private RedisClusterAsyncCommands<byte[], byte[]> getCommands() {
        return commands(connections.getConnection(url));
    }

    public void open(String url) {
        this.url = url;
    }

    public void close() {
        // Connections are shared by URL. Only invalidation or JVM shutdown closes them.
    }

    /**
     * Close and discard the shared connection/client cached for this wrapper's URL,
     * so the next command re-opens a fresh connection. Intended to be called after a
     * connection-level failure so that subsequent calls recover instead of reusing a
     * broken connection.
     */
    public void invalidate() {
        connections.invalidate(url);
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

    /** SET with expiry in a single command/round trip. */
    public RedisFuture<String> setex(byte[] key, byte[] value, long ttlSeconds) {
        // One SET with EX per key also preserves cluster routing across slots.
        return getCommands().set(key, value, SetArgs.Builder.ex(ttlSeconds));
    }

    public RedisFuture<Long> del(byte[] key) {
        return getCommands().del(key);
    }

    public RedisFuture<Long> lpush(byte[] key, byte[]... values) {
        return getCommands().lpush(key, values);
    }

    public RedisFuture<Long> lrem(byte[] key, byte[] value) {
        return lrem(key, 0, value);
    }

    /** Remove at most {@code count} matching list entries; 0 removes all. */
    public RedisFuture<Long> lrem(byte[] key, long count, byte[] value) {
        return getCommands().lrem(key, count, value);
    }

    public RedisFuture<String> ltrim(byte[] key, long start, long stop) {
        return getCommands().ltrim(key, start, stop);
    }

    public RedisFuture<Boolean> expire(byte[] key, long seconds) {
        return getCommands().expire(key, seconds);
    }
}
