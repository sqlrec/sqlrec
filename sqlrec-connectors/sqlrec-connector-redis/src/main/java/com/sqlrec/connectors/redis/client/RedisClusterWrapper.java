package com.sqlrec.connectors.redis.client;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.SharePool;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisClusterWrapper implements AbstractRedisWrapper {
    private static Map<String, RedisClusterClient> redisClientMap = new ConcurrentHashMap<>();
    private static Map<String, SharePool<StatefulRedisClusterConnection<byte[], byte[]>>> poolMap = new ConcurrentHashMap<>();

    private String url;

    @Override
    public void open(String url) {
        this.url = url;
    }

    private static synchronized void openRedisClusterClient(String url) {
        if (poolMap.containsKey(url)) {
            return;
        }

        RedisClusterClient redisClient = RedisClusterClient.create(url);

        SharePool<StatefulRedisClusterConnection<byte[], byte[]>> pool = new SharePool<>(
                SqlRecConfigs.REDIS_POOL_SIZE.getValue(),
                () -> redisClient.connect(new ByteArrayCodec())
        );

        redisClientMap.put(url, redisClient);
        poolMap.put(url, pool);
    }

    private RedisAdvancedClusterAsyncCommands<byte[], byte[]> getCommands() {
        if (!poolMap.containsKey(url)) {
            openRedisClusterClient(url);
        }
        SharePool<StatefulRedisClusterConnection<byte[], byte[]>> pool = poolMap.get(url);
        StatefulRedisClusterConnection<byte[], byte[]> connection = pool.getObject();
        return connection.async();
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
