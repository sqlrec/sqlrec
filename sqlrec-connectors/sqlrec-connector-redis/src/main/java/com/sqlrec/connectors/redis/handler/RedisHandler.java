package com.sqlrec.connectors.redis.handler;

import com.sqlrec.connectors.redis.client.AbstractRedisWrapper;
import com.sqlrec.connectors.redis.client.RedisClusterWrapper;
import com.sqlrec.connectors.redis.client.RedisWrapper;
import com.sqlrec.connectors.redis.codec.AbstractCodec;
import com.sqlrec.connectors.redis.codec.JsonCodec;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RedisHandler {
    private AbstractRedisWrapper redisClient;
    private RedisConfig redisConfig;
    private AbstractCodec codec;

    public RedisHandler(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    public void open() {
        if (redisConfig.redisMode.equals(RedisOptions.CLUSTER_MODE)) {
            redisClient = new RedisClusterWrapper();
        } else {
            redisClient = new RedisWrapper();
        }
        redisClient.open(redisConfig.url);
        codec = new JsonCodec();
        codec.init(redisConfig.fieldSchemas);
    }

    public void close() {
        if (redisClient != null) {
            redisClient.close();
            redisClient = null;
        }
    }

    public CompletableFuture<List<Object[]>> scan(String rowKey) {
        if (redisConfig.dataStructure.equals(RedisOptions.LIST_DATA_STRUCTURE)) {
            RedisFuture<List<byte[]>> future = redisClient.lrange(getKeyBytes(rowKey), 0, -1);
            return future.toCompletableFuture().thenApply(this::decodeDatas);
        }

        RedisFuture<byte[]> future = redisClient.get(getKeyBytes(rowKey));
        return future.toCompletableFuture().thenApply(bytes -> {
            List<Object[]> list = new ArrayList<>();
            if (bytes != null) {
                list.add(codec.decode(bytes));
            }
            return list;
        });
    }

    public CompletableFuture<Map<String, List<Object[]>>> scan(Set<String> keySet) {
        if (redisConfig.dataStructure.equals(RedisOptions.LIST_DATA_STRUCTURE)) {
            Map<String, CompletableFuture<List<byte[]>>> futureMap = keySet.stream()
                    .collect(Collectors.toMap(
                            key -> key,
                            key -> redisClient.lrange(getKeyBytes(key), 0, -1).toCompletableFuture()));
            return CompletableFuture.allOf(futureMap.values().toArray(new CompletableFuture[0]))
                    .thenApply(x -> {
                        Map<String, List<Object[]>> result = new HashMap<>();
                        for (Map.Entry<String, CompletableFuture<List<byte[]>>> entry : futureMap.entrySet()) {
                            String key = entry.getKey();
                            List<byte[]> data = entry.getValue().join();
                            result.put(key, decodeDatas(data));
                        }
                        return result;
                    });
        }

        List<byte[]> keys = keySet.stream()
                .map(this::getKeyBytes)
                .collect(Collectors.toList());
        RedisFuture<List<KeyValue<byte[], byte[]>>> future = redisClient.mget(keys.toArray(new byte[0][]));
        return future.toCompletableFuture().thenApply(list -> {
            Map<String, List<Object[]>> result = new HashMap<>();
            if (list == null) {
                return result;
            }
            list.forEach(keyValue -> {
                if (keyValue.getValue() == null) {
                    return;
                }
                String originKey = getOriginKey(keyValue.getKey());
                result.computeIfAbsent(originKey, k -> new ArrayList<>())
                        .add(codec.decode(keyValue.getValue()));
            });
            return result;
        });
    }

    public List<Object[]> decodeDatas(List<byte[]> datas) {
        if (datas == null) {
            return Collections.emptyList();
        }
        return datas.stream()
                .map(bytes -> {
                    try {
                        return codec.decode(bytes);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public void delete(Object[] data) {
        if (redisConfig.dataStructure.equals(RedisOptions.LIST_DATA_STRUCTURE)) {
            redisClient.lrem(getKey(data), codec.encode(data));
        } else {
            redisClient.del(getKey(data));
        }
    }

    public void insert(Object[] data) {
        byte[] key = getKey(data);
        if (redisConfig.dataStructure.equals(RedisOptions.LIST_DATA_STRUCTURE)) {
            redisClient.lpush(key, codec.encode(data)).whenComplete((x, y) -> {
                redisClient.expire(key, redisConfig.ttl);
            });
            if (redisConfig.maxListSize != null && redisConfig.maxListSize > 0) {
                redisClient.ltrim(key, 0, redisConfig.maxListSize - 1);
            }
        } else {
            redisClient.set(key, codec.encode(data)).whenComplete((x, y) -> {
                redisClient.expire(key, redisConfig.ttl);
            });
        }
    }

    private byte[] getKey(Object[] data) {
        return getKeyBytes(data[redisConfig.primaryKeyIndex].toString());
    }

    private byte[] getKeyBytes(String rowKey) {
        String finalRowKey = redisConfig.database + ":" + redisConfig.tableName + ":" + rowKey;
        return finalRowKey.getBytes(StandardCharsets.UTF_8);
    }

    private String getOriginKey(byte[] key) {
        return new String(key, StandardCharsets.UTF_8)
                .substring(redisConfig.database.length() + redisConfig.tableName.length() + 2);
    }
}
