package com.sqlrec.connectors.redis.handler;

import com.sqlrec.connectors.redis.client.AbstractRedisWrapper;
import com.sqlrec.connectors.redis.client.RedisClusterWrapper;
import com.sqlrec.connectors.redis.client.RedisWrapper;
import com.sqlrec.connectors.redis.codec.AbstractCodec;
import com.sqlrec.connectors.redis.codec.JsonCodec;
import com.sqlrec.connectors.redis.codec.StringCodec;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RedisHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RedisHandler.class);
    private static final int TIMEOUT_SECONDS = 30;

    private AbstractRedisWrapper redisClient;
    private RedisConfig redisConfig;
    private AbstractCodec codec;
    private String keyPrefix;

    public RedisHandler(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    public void open() {
        redisClient = redisConfig.redisMode.equals(RedisOptions.CLUSTER_MODE)
                ? new RedisClusterWrapper() : new RedisWrapper();
        redisClient.open(redisConfig.url);
        codec = redisConfig.dataStructure.equals(RedisOptions.STRING_DATA_STRUCTURE)
                ? new StringCodec() : new JsonCodec();
        codec.init(redisConfig.fieldSchemas, redisConfig.primaryKeyIndex);
        keyPrefix = redisConfig.database + ":" + redisConfig.tableName + ":";
    }

    public void close() {
        if (redisClient != null) {
            redisClient.close();
            redisClient = null;
        }
    }

    public CompletableFuture<List<Object[]>> scan(String rowKey) {
        if (isListMode()) {
            return redisClient.lrange(getKeyBytes(rowKey), 0, -1)
                    .toCompletableFuture().thenApply(data -> decodeList(data, rowKey));
        }
        return redisClient.get(getKeyBytes(rowKey))
                .toCompletableFuture().thenApply(bytes -> {
                    if (bytes == null) {
                        return Collections.emptyList();
                    }
                    return Collections.singletonList(codec.decode(bytes, rowKey));
                });
    }

    public CompletableFuture<Map<String, List<Object[]>>> scan(Set<String> keySet) {
        if (isListMode()) {
            Map<String, CompletableFuture<List<byte[]>>> futureMap = keySet.stream()
                    .collect(Collectors.toMap(
                            key -> key,
                            key -> redisClient.lrange(getKeyBytes(key), 0, -1).toCompletableFuture()));
            return CompletableFuture.allOf(futureMap.values().toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        Map<String, List<Object[]>> result = new HashMap<>();
                        futureMap.forEach((key, f) -> result.put(key, decodeList(f.join(), key)));
                        return result;
                    });
        }

        byte[][] keys = keySet.stream().map(this::getKeyBytes).toArray(byte[][]::new);
        return redisClient.mget(keys).toCompletableFuture().thenApply(list -> {
            Map<String, List<Object[]>> result = new HashMap<>();
            if (list == null) {
                return result;
            }
            for (KeyValue<byte[], byte[]> kv : list) {
                if (kv != null && kv.hasValue()) {
                    String originKey = getOriginKey(kv.getKey());
                    result.computeIfAbsent(originKey, k -> new ArrayList<>())
                            .add(codec.decode(kv.getValue(), originKey));
                }
            }
            return result;
        });
    }

    public List<Object[]> decodeList(List<byte[]> datas, String primaryKey) {
        if (datas == null) {
            return Collections.emptyList();
        }
        List<Object[]> result = new ArrayList<>(datas.size());
        for (byte[] bytes : datas) {
            try {
                result.add(codec.decode(bytes, primaryKey));
            } catch (Exception e) {
                LOG.warn("Failed to decode data for key {}: {}", primaryKey, e.getMessage());
            }
        }
        return result;
    }

    public void delete(Object[] data) {
        byte[] key = getKey(data);
        try {
            if (isListMode()) {
                await(redisClient.lrem(key, codec.encode(data)));
            } else {
                await(redisClient.del(key));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete data from Redis", e);
        }
    }

    public void insert(Object[] data) {
        byte[] key = getKey(data);
        byte[] value = codec.encode(data);
        try {
            if (isListMode()) {
                await(redisClient.lpush(key, value));
                trimIfNeeded(key);
                await(redisClient.expire(key, redisConfig.ttl));
            } else {
                await(redisClient.set(key, value));
                await(redisClient.expire(key, redisConfig.ttl));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert data to Redis", e);
        }
    }

    public void batchInsert(Collection<? extends Object[]> dataList) {
        try {
            List<RedisFuture<?>> futures = new ArrayList<>();
            if (isListMode()) {
                Map<byte[], List<byte[]>> keyToValues = new LinkedHashMap<>();
                for (Object[] data : dataList) {
                    keyToValues.computeIfAbsent(getKey(data), k -> new ArrayList<>())
                            .add(codec.encode(data));
                }
                for (Map.Entry<byte[], List<byte[]>> entry : keyToValues.entrySet()) {
                    byte[] key = entry.getKey();
                    futures.add(redisClient.lpush(key, entry.getValue().toArray(new byte[0][])));
                    if (redisConfig.maxListSize != null && redisConfig.maxListSize > 0) {
                        futures.add(redisClient.ltrim(key, 0, redisConfig.maxListSize - 1));
                    }
                    futures.add(redisClient.expire(key, redisConfig.ttl));
                }
            } else {
                Map<byte[], byte[]> kvMap = new LinkedHashMap<>();
                for (Object[] data : dataList) {
                    kvMap.put(getKey(data), codec.encode(data));
                }
                futures.add(redisClient.mset(kvMap));
                for (byte[] key : kvMap.keySet()) {
                    futures.add(redisClient.expire(key, redisConfig.ttl));
                }
            }
            awaitAll(futures);
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch insert data to Redis", e);
        }
    }

    public void batchDelete(Collection<? extends Object[]> dataList) {
        try {
            List<RedisFuture<?>> futures = new ArrayList<>();
            if (isListMode()) {
                for (Object[] data : dataList) {
                    futures.add(redisClient.lrem(getKey(data), codec.encode(data)));
                }
            } else {
                for (Object[] data : dataList) {
                    futures.add(redisClient.del(getKey(data)));
                }
            }
            awaitAll(futures);
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch delete data from Redis", e);
        }
    }

    private boolean isListMode() {
        return redisConfig.dataStructure.equals(RedisOptions.LIST_DATA_STRUCTURE);
    }

    private void await(RedisFuture<?> future) throws Exception {
        future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void awaitAll(List<RedisFuture<?>> futures) throws Exception {
        CompletableFuture.allOf(futures.stream()
                        .map(RedisFuture::toCompletableFuture)
                        .toArray(CompletableFuture[]::new))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void trimIfNeeded(byte[] key) throws Exception {
        if (redisConfig.maxListSize != null && redisConfig.maxListSize > 0) {
            await(redisClient.ltrim(key, 0, redisConfig.maxListSize - 1));
        }
    }

    private byte[] getKey(Object[] data) {
        Object keyValue = data[redisConfig.primaryKeyIndex];
        if (keyValue == null) {
            throw new IllegalArgumentException("Primary key at index " + redisConfig.primaryKeyIndex + " is null");
        }
        return getKeyBytes(keyValue.toString());
    }

    private byte[] getKeyBytes(String rowKey) {
        return (keyPrefix + rowKey).getBytes(StandardCharsets.UTF_8);
    }

    private String getOriginKey(byte[] key) {
        return new String(key, StandardCharsets.UTF_8).substring(keyPrefix.length());
    }
}
