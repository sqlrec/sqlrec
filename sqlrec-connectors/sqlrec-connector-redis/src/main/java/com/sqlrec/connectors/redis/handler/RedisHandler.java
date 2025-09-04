package com.sqlrec.connectors.redis.handler;

import com.sqlrec.connectors.redis.client.AbstractRedisWrapper;
import com.sqlrec.connectors.redis.client.RedisClusterWrapper;
import com.sqlrec.connectors.redis.client.RedisWrapper;
import com.sqlrec.connectors.redis.codec.AbstractCodec;
import com.sqlrec.connectors.redis.codec.JsonCodec;
import com.sqlrec.common.utils.FieldSchema;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import io.lettuce.core.RedisFuture;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RedisHandler {
    AbstractRedisWrapper redisClient;

    private RedisConfig redisConfig;
    private List<FieldSchema> fieldSchemas;
    AbstractCodec codec;

    public RedisHandler(RedisConfig redisConfig, List<FieldSchema> fieldSchemas) {
        this.redisConfig = redisConfig;
        this.fieldSchemas = fieldSchemas;
    }

    public void open() {
        if (redisConfig.redisMode.equals(RedisOptions.CLUSTER_MODE)) {
            redisClient = new RedisClusterWrapper();
        } else {
            redisClient = new RedisWrapper();
        }
        redisClient.open(redisConfig.url);
        codec = new JsonCodec();
        codec.init(fieldSchemas);
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
            return future.toCompletableFuture().thenApply(list -> {
                if (list == null) {
                    return new ArrayList<>();
                }
                return list.stream()
                        .map(bytes -> codec.decode(bytes))
                        .collect(Collectors.toList());
            });
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
        } else {
            redisClient.set(key, codec.encode(data)).whenComplete((x, y) -> {
                redisClient.expire(key, redisConfig.ttl);
            });
        }
    }

    private byte[] getKey(Object[] data) {
        return getKeyBytes(data[0].toString());
    }

    private byte[] getKeyBytes(String rowKey) {
        return rowKey.getBytes(StandardCharsets.UTF_8);
    }


}
