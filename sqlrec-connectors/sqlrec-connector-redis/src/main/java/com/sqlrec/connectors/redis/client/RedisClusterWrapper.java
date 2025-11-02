package com.sqlrec.connectors.redis.client;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.List;

public class RedisClusterWrapper implements AbstractRedisWrapper {
    RedisClusterClient redisClient;
    StatefulRedisClusterConnection<byte[], byte[]> connection;
    RedisAdvancedClusterAsyncCommands<byte[], byte[]> commands;

    @Override
    public void open(String url) {
        redisClient = RedisClusterClient.create(url);
        connection = redisClient.connect(new ByteArrayCodec());
        commands = connection.async();
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.close();
        }
        redisClient = null;
        connection = null;
        commands = null;
    }

    public RedisFuture<List<byte[]>> lrange(byte[] key, long start, long end) {
        return commands.lrange(key, start, end);
    }

    public RedisFuture<byte[]> get(byte[] key) {
        return commands.get(key);
    }

    public RedisFuture<List<KeyValue<byte[], byte[]>>> mget(byte[]... keys) {
        return commands.mget(keys);
    }

    public RedisFuture<String> set(byte[] key, byte[] value) {
        return commands.set(key, value);
    }

    public RedisFuture<Long> del(byte[] key) {
        return commands.del(key);
    }

    public RedisFuture<Long> lpush(byte[] key, byte[] value) {
        return commands.lpush(key, value);
    }

    public RedisFuture<Long> lrem(byte[] key, byte[] value) {
        return commands.lrem(key, 0, value);
    }

    public RedisFuture<Boolean> expire(byte[] key, long seconds) {
        return commands.expire(key, seconds);
    }
}
