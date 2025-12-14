package com.sqlrec.connectors.redis.client;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;

import java.util.List;

public interface AbstractRedisWrapper {
    void open(String url);

    void close();

    RedisFuture<List<byte[]>> lrange(byte[] key, long start, long end);

    RedisFuture<byte[]> get(byte[] key);

    RedisFuture<List<KeyValue<byte[], byte[]>>> mget(byte[]... keys);

    RedisFuture<String> set(byte[] key, byte[] value);

    RedisFuture<Long> del(byte[] key);

    RedisFuture<Long> lpush(byte[] key, byte[] value);

    RedisFuture<Long> lrem(byte[] key, byte[] value);

    RedisFuture<String> ltrim(byte[] key, long start, long stop);

    RedisFuture<Boolean> expire(byte[] key, long seconds);
}
