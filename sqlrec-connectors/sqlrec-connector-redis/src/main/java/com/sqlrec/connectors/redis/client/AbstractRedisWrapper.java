package com.sqlrec.connectors.redis.client;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;

import java.util.List;

public interface AbstractRedisWrapper {
    public void open(String url);
    public void close();
    RedisFuture<List<byte[]>> lrange(byte[] key, long start, long end);
    RedisFuture<byte[]> get(byte[] key);
    public RedisFuture<List<KeyValue<byte[], byte[]>>> mget(byte[]... keys);
    public RedisFuture<String> set(byte[] key, byte[] value);
    public RedisFuture<Long> del(byte[] key);
    public RedisFuture<Long> lpush(byte[] key, byte[] value);
    public RedisFuture<Long> lrem(byte[] key, byte[] value);
    public RedisFuture<Boolean> expire(byte[] key, long seconds);
}
