package com.sqlrec.connectors.redis.client;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisWrapper implements AbstractRedisWrapper {
    private static Map<String, RedisClient> redisClientMap = new ConcurrentHashMap<>();
    private static Map<String, StatefulRedisConnection<byte[], byte[]>> connectionMap = new ConcurrentHashMap<>();

    private String url;

    @Override
    public void open(String url) {
        this.url = url;
    }

    private static synchronized void openRedisClient(String url) {
        if (connectionMap.containsKey(url)) {
            return;
        }

        RedisClient redisClient = RedisClient.create(url);
        StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());

        redisClientMap.put(url, redisClient);
        connectionMap.put(url, connection);
    }

    private RedisAsyncCommands<byte[], byte[]> getCommands() {
        if (!connectionMap.containsKey(url)) {
            openRedisClient(url);
        }
        return connectionMap.get(url).async();
    }

    @Override
    public void close() {
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

    public RedisFuture<Long> lpush(byte[] key, byte[] value) {
        return getCommands().lpush(key, value);
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
