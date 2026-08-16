package com.sqlrec.connectors.redis.client;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public interface AbstractRedisWrapper {
    void open(String url);

    void close();

    /**
     * Close and discard the shared connection/client cached for this wrapper's URL,
     * so the next command re-opens a fresh connection. Intended to be called after a
     * connection-level failure so that subsequent calls recover instead of reusing a
     * broken connection.
     */
    void invalidate();

    RedisFuture<List<byte[]>> lrange(byte[] key, long start, long end);

    RedisFuture<byte[]> get(byte[] key);

    RedisFuture<List<KeyValue<byte[], byte[]>>> mget(byte[]... keys);

    RedisFuture<String> set(byte[] key, byte[] value);

    /**
     * SET key value EX ttlSeconds, i.e. set with expiry in a single command/round trip.
     */
    RedisFuture<String> setex(byte[] key, byte[] value, long ttlSeconds);

    RedisFuture<Long> del(byte[] key);

    RedisFuture<Long> lpush(byte[] key, byte[]... values);

    RedisFuture<Long> lrem(byte[] key, byte[] value);

    RedisFuture<String> ltrim(byte[] key, long start, long stop);

    RedisFuture<Boolean> expire(byte[] key, long seconds);

    /**
     * Issues the commands produced by {@code commandProducer} as one pipeline:
     * command flushing is suspended while the producer runs, then all buffered
     * commands are flushed to the network in one go instead of one TCP write per
     * command. The producer must only issue commands and never block. The caller
     * is responsible for awaiting the returned futures.
     */
    List<RedisFuture<?>> executePipelined(Supplier<List<RedisFuture<?>>> commandProducer);
}
