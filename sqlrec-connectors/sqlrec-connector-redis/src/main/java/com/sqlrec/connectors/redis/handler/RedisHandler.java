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
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
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

    AbstractRedisWrapper redisClient;
    private RedisConfig redisConfig;
    private AbstractCodec codec;
    private String keyPrefix;

    public RedisHandler(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    /** Test-only: inject a mock redis client. */
    void setRedisClientForTest(AbstractRedisWrapper mockClient) {
        this.redisClient = mockClient;
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
                    .toCompletableFuture()
                    .whenComplete((data, ex) -> { if (ex != null) maybeInvalidateOnFailure(ex); })
                    .thenApply(data -> decodeList(data, rowKey));
        }
        return redisClient.get(getKeyBytes(rowKey))
                .toCompletableFuture()
                .whenComplete((bytes, ex) -> { if (ex != null) maybeInvalidateOnFailure(ex); })
                .thenApply(bytes -> {
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
                    .whenComplete((v, ex) -> { if (ex != null) maybeInvalidateOnFailure(ex); })
                    .thenApply(v -> {
                        Map<String, List<Object[]>> result = new HashMap<>();
                        futureMap.forEach((key, f) -> result.put(key, decodeList(f.join(), key)));
                        return result;
                    });
        }

        byte[][] keys = keySet.stream().map(this::getKeyBytes).toArray(byte[][]::new);
        return redisClient.mget(keys).toCompletableFuture()
                .whenComplete((list, ex) -> { if (ex != null) maybeInvalidateOnFailure(ex); })
                .thenApply(list -> {
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
                // Issue all commands first, then wait once: avoids serializing
                // each round trip (lpush -> ltrim -> expire).
                List<RedisFuture<?>> futures = new ArrayList<>(3);
                futures.add(redisClient.lpush(key, value));
                if (redisConfig.maxListSize != null && redisConfig.maxListSize > 0) {
                    futures.add(redisClient.ltrim(key, 0, redisConfig.maxListSize - 1));
                }
                futures.add(redisClient.expire(key, redisConfig.ttl));
                awaitAll(futures);
            } else {
                // SET key value EX ttl: one command instead of SET + EXPIRE.
                await(redisClient.setex(key, value, redisConfig.ttl));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert data to Redis", e);
        }
    }

    public void batchInsert(Collection<? extends Object[]> dataList) {
        try {
            // Compute keys and encode values before entering the pipeline section:
            // a malformed row (e.g. null primary key) fails the whole batch before
            // any command is issued, and JSON encoding does not hold the
            // autoFlush-disabled window of the shared connection.
            List<RedisFuture<?>> futures;
            if (isListMode()) {
                // Aggregate rows by key: byte[] has reference equality, so the map key
                // is the key string (UTF-8 encoded back to bytes when issuing commands).
                Map<String, List<byte[]>> keyToValues = new LinkedHashMap<>();
                for (Object[] data : dataList) {
                    byte[] key = getKey(data);
                    keyToValues.computeIfAbsent(new String(key, StandardCharsets.UTF_8), k -> new ArrayList<>())
                            .add(codec.encode(data));
                }
                List<Map.Entry<String, List<byte[]>>> entries = new ArrayList<>(keyToValues.entrySet());
                futures = redisClient.executePipelined(() -> {
                    List<RedisFuture<?>> fs = new ArrayList<>();
                    for (Map.Entry<String, List<byte[]>> entry : entries) {
                        byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
                        fs.add(redisClient.lpush(key, entry.getValue().toArray(new byte[0][])));
                        if (redisConfig.maxListSize != null && redisConfig.maxListSize > 0) {
                            fs.add(redisClient.ltrim(key, 0, redisConfig.maxListSize - 1));
                        }
                        fs.add(redisClient.expire(key, redisConfig.ttl));
                    }
                    return fs;
                });
            } else {
                List<byte[]> keys = new ArrayList<>(dataList.size());
                List<byte[]> values = new ArrayList<>(dataList.size());
                for (Object[] data : dataList) {
                    keys.add(getKey(data));
                    values.add(codec.encode(data));
                }
                // One SET ... EX per row, sent as a single pipeline. Unlike MSET +
                // EXPIRE this halves the command count and also works in cluster
                // mode (each SET is routed to the key's slot; MSET fails with
                // CROSSSLOT for keys spanning multiple slots).
                futures = redisClient.executePipelined(() -> {
                    List<RedisFuture<?>> fs = new ArrayList<>(keys.size());
                    for (int i = 0; i < keys.size(); i++) {
                        fs.add(redisClient.setex(keys.get(i), values.get(i), redisConfig.ttl));
                    }
                    return fs;
                });
            }
            awaitAll(futures);
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch insert data to Redis", e);
        }
    }

    public void batchDelete(Collection<? extends Object[]> dataList) {
        try {
            boolean listMode = isListMode();
            List<byte[]> keys = new ArrayList<>(dataList.size());
            List<byte[]> values = listMode ? new ArrayList<>(dataList.size()) : null;
            for (Object[] data : dataList) {
                keys.add(getKey(data));
                if (listMode) {
                    values.add(codec.encode(data));
                }
            }
            List<RedisFuture<?>> futures = redisClient.executePipelined(() -> {
                List<RedisFuture<?>> fs = new ArrayList<>(keys.size());
                for (int i = 0; i < keys.size(); i++) {
                    if (listMode) {
                        fs.add(redisClient.lrem(keys.get(i), values.get(i)));
                    } else {
                        fs.add(redisClient.del(keys.get(i)));
                    }
                }
                return fs;
            });
            awaitAll(futures);
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch delete data from Redis", e);
        }
    }

    private boolean isListMode() {
        return redisConfig.dataStructure.equals(RedisOptions.LIST_DATA_STRUCTURE);
    }

    private void await(RedisFuture<?> future) throws Exception {
        try {
            future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            maybeInvalidateOnFailure(e);
            throw e;
        }
    }

    private void awaitAll(List<RedisFuture<?>> futures) throws Exception {
        try {
            CompletableFuture.allOf(futures.stream()
                            .map(RedisFuture::toCompletableFuture)
                            .toArray(CompletableFuture[]::new))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            maybeInvalidateOnFailure(e);
            throw e;
        }
    }

    /**
     * Returns true if the throwable (or any cause in its chain) indicates a Redis
     * connection-level failure (as opposed to a semantic error like WRONGTYPE).
     * Such failures mean the shared connection should be discarded and re-opened.
     */
    private static boolean isConnectionFailure(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof RedisConnectionException
                    || cur instanceof RedisCommandTimeoutException) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    private void maybeInvalidateOnFailure(Throwable t) {
        if (t != null && isConnectionFailure(t)) {
            LOG.warn("Redis connection failure detected, invalidating shared connection for {}: {}",
                    redisConfig.url, t.getMessage());
            try {
                redisClient.invalidate();
            } catch (Exception ex) {
                LOG.warn("Failed to invalidate Redis connection for {}: {}", redisConfig.url, ex.getMessage());
            }
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
