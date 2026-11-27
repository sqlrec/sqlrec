package com.sqlrec.connectors.redis.handler;

import com.sqlrec.connectors.redis.client.BaseRedisWrapper;
import com.sqlrec.connectors.redis.client.RedisClusterWrapper;
import com.sqlrec.connectors.redis.client.RedisWrapper;
import com.sqlrec.connectors.redis.codec.AbstractCodec;
import com.sqlrec.connectors.redis.codec.JsonCodec;
import com.sqlrec.connectors.redis.codec.ProtobufCodec;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RedisHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RedisHandler.class);
    private static final int TIMEOUT_SECONDS = 30;

    BaseRedisWrapper<?, ?> redisClient;
    private RedisConfig redisConfig;
    private AbstractCodec codec;
    private String keyPrefix;

    public RedisHandler(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    /** Test-only: inject a mock redis client. */
    void setRedisClientForTest(BaseRedisWrapper<?, ?> mockClient) {
        this.redisClient = mockClient;
    }

    public void open() {
        redisClient = redisConfig.redisMode.equals(RedisOptions.CLUSTER_MODE)
                ? new RedisClusterWrapper() : new RedisWrapper();
        redisClient.open(redisConfig.url);
        if (RedisOptions.PROTOBUF_FORMAT.equals(redisConfig.format)) {
            codec = new ProtobufCodec(redisConfig.protobufMessageClassName);
        } else if (redisConfig.dataStructure.equals(RedisOptions.STRING_DATA_STRUCTURE)) {
            codec = new StringCodec();
        } else {
            codec = new JsonCodec();
        }
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
        byte[] key = getKeyBytes(rowKey);
        if (isListMode()) {
            return invalidateOnReadFailure(redisClient.lrange(key, 0, -1).toCompletableFuture())
                    .thenApply(data -> decodeList(data, rowKey));
        }
        return invalidateOnReadFailure(redisClient.get(key).toCompletableFuture())
                .thenApply(bytes -> bytes == null
                        ? Collections.emptyList() : Collections.singletonList(codec.decode(bytes, rowKey)));
    }

    public CompletableFuture<Map<String, List<Object[]>>> scan(Set<String> keySet) {
        if (keySet == null || keySet.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        if (isListMode()) {
            Map<String, CompletableFuture<List<byte[]>>> futureMap = keySet.stream()
                    .collect(Collectors.toMap(
                            key -> key,
                            key -> redisClient.lrange(getKeyBytes(key), 0, -1).toCompletableFuture()));
            return invalidateOnReadFailure(CompletableFuture.allOf(
                    futureMap.values().toArray(CompletableFuture<?>[]::new)))
                    .thenApply(v -> {
                        Map<String, List<Object[]>> result = new HashMap<>();
                        futureMap.forEach((key, f) -> result.put(key, decodeList(f.join(), key)));
                        return result;
                    });
        }

        byte[][] keys = keySet.stream().map(this::getKeyBytes).toArray(byte[][]::new);
        return invalidateOnReadFailure(redisClient.mget(keys).toCompletableFuture())
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

    private <T> CompletableFuture<T> invalidateOnReadFailure(CompletableFuture<T> read) {
        return read.whenComplete((result, failure) -> maybeInvalidateOnFailure(failure));
    }

    List<Object[]> decodeList(List<byte[]> datas, String primaryKey) {
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
        EncodedRow row = encodeRow(data);
        try {
            insertRows(List.of(row));
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert data to Redis", e);
        }
    }

    public void batchInsert(Collection<? extends Object[]> dataList) {
        try {
            // Compute keys and encode values before submitting any asynchronous command:
            // a malformed row (e.g. null primary key) fails the whole batch before
            // any command is issued.
            List<EncodedRow> rows = new ArrayList<>(dataList.size());
            for (Object[] data : dataList) {
                rows.add(encodeRow(data));
            }
            insertRows(rows);
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch insert data to Redis", e);
        }
    }

    private void insertRows(List<EncodedRow> rows) throws Exception {
        if (isListMode()) {
            Map<String, List<byte[]>> keyToValues = new LinkedHashMap<>();
            for (EncodedRow row : rows) {
                addListValue(keyToValues, row);
            }
            pushListRows(keyToValues);
            return;
        }
        // Each SET with expiry is routed to its own cluster slot.
        List<RedisFuture<?>> futures = new ArrayList<>(rows.size());
        for (EncodedRow row : rows) {
            futures.add(redisClient.setex(row.key, row.value, redisConfig.ttl));
        }
        if (futures.size() == 1) {
            await(futures.get(0));
        } else {
            awaitAll(futures);
        }
    }

    /** Apply same-key updates and move changed keys without overwriting existing destinations. */
    public int replaceSingleValueRows(List<Object[]> oldRows, List<Object[]> newRows) {
        if (oldRows.size() != newRows.size()) {
            throw new IllegalArgumentException("UPDATE row counts do not match");
        }
        List<byte[]> oldKeys = oldRows.stream().map(this::getKey).collect(Collectors.toList());
        List<EncodedRow> newValues = newRows.stream().map(this::encodeRow).collect(Collectors.toList());
        List<EncodedRow> updates = new ArrayList<>();
        int moved = 0;
        try {
            for (int i = 0; i < oldKeys.size(); i++) {
                byte[] oldKey = oldKeys.get(i);
                EncodedRow newRow = newValues.get(i);
                if (Arrays.equals(oldKey, newRow.key)) {
                    updates.add(newRow);
                } else if (moveSingleValue(oldKey, newRow)) {
                    moved++;
                }
            }
            insertRows(updates);
            return moved + updates.size();
        } catch (Exception e) {
            throw new RuntimeException("Failed to update Redis rows: " + e.getMessage(), e);
        }
    }

    private boolean moveSingleValue(byte[] oldKey, EncodedRow newRow) throws Exception {
        if (await(redisClient.get(oldKey)) == null) {
            return false;
        }
        if (await(redisClient.setIfAbsent(newRow.key, newRow.value, redisConfig.ttl)) == null) {
            throw new IllegalStateException("Redis UPDATE destination key already exists");
        }
        // Separate commands work across cluster slots. If deletion fails, both keys may remain.
        await(redisClient.del(oldKey));
        return true;
    }

    /** Replace one occurrence of each selected list row, then prepend its new value. */
    public int replaceListRows(List<Object[]> oldRows, List<Object[]> newRows) {
        if (!isListMode()) {
            throw new IllegalStateException("Row replacement requires Redis list mode");
        }
        if (oldRows.size() != newRows.size()) {
            throw new IllegalArgumentException("UPDATE row counts do not match");
        }
        List<RowReplacement> rows = new ArrayList<>(oldRows.size());
        // Encode every row before changing Redis so malformed input cannot remove old rows.
        for (int i = 0; i < oldRows.size(); i++) {
            rows.add(new RowReplacement(encodeRow(oldRows.get(i)), encodeRow(newRows.get(i))));
        }

        Map<String, List<byte[]>> replacements = new LinkedHashMap<>();
        int replaced = 0;
        try {
            // Remove all selected old rows before pushing new values. Otherwise a new
            // value could be mistaken for the next selected old row.
            for (RowReplacement row : rows) {
                Long removed = await(redisClient.lrem(row.oldRow.key, 1, row.oldRow.value));
                if (removed != null && removed > 0) {
                    addListValue(replacements, row.newRow);
                    replaced++;
                }
            }
            pushListRows(replacements);
            return replaced;
        } catch (Exception e) {
            throw new RuntimeException("Failed to replace Redis list rows", e);
        }
    }

    private static void addListValue(Map<String, List<byte[]>> keyToValues, EncodedRow row) {
        String key = new String(row.key, StandardCharsets.UTF_8);
        keyToValues.computeIfAbsent(key, ignored -> new ArrayList<>()).add(row.value);
    }

    private void pushListRows(Map<String, List<byte[]>> keyToValues) throws Exception {
        List<RedisFuture<?>> futures = new ArrayList<>(keyToValues.size() * 3);
        for (Map.Entry<String, List<byte[]>> entry : keyToValues.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            // LPUSH with multiple values has the same order as repeated LPUSH calls.
            futures.add(redisClient.lpush(key, entry.getValue().toArray(new byte[0][])));
            addListMaintenanceCommands(key, futures);
        }
        awaitAll(futures);
    }

    private void addListMaintenanceCommands(byte[] key, List<RedisFuture<?>> futures) {
        if (redisConfig.maxListSize != null && redisConfig.maxListSize > 0) {
            futures.add(redisClient.ltrim(key, 0, redisConfig.maxListSize - 1));
        }
        futures.add(redisClient.expire(key, redisConfig.ttl));
    }

    public void batchDelete(Collection<? extends Object[]> dataList) {
        try {
            boolean listMode = isListMode();
            List<EncodedRow> rows = new ArrayList<>(dataList.size());
            for (Object[] data : dataList) {
                byte[] key = getKey(data);
                rows.add(new EncodedRow(key, listMode ? codec.encode(data) : null));
            }
            List<RedisFuture<?>> futures = new ArrayList<>(rows.size());
            for (EncodedRow row : rows) {
                if (listMode) {
                    futures.add(redisClient.lrem(row.key, row.value));
                } else {
                    futures.add(redisClient.del(row.key));
                }
            }
            awaitAll(futures);
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch delete data from Redis", e);
        }
    }

    private EncodedRow encodeRow(Object[] data) {
        return new EncodedRow(getKey(data), codec.encode(data));
    }

    private static final class EncodedRow {
        private final byte[] key;
        private final byte[] value;

        private EncodedRow(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    private static final class RowReplacement {
        private final EncodedRow oldRow;
        private final EncodedRow newRow;

        private RowReplacement(EncodedRow oldRow, EncodedRow newRow) {
            this.oldRow = oldRow;
            this.newRow = newRow;
        }
    }

    public boolean isListMode() {
        return redisConfig.dataStructure.equals(RedisOptions.LIST_DATA_STRUCTURE);
    }

    private <T> T await(Future<T> future) throws Exception {
        try {
            return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            maybeInvalidateOnFailure(e);
            throw e;
        }
    }

    private void awaitAll(List<RedisFuture<?>> futures) throws Exception {
        await(CompletableFuture.allOf(futures.stream()
                .map(RedisFuture::toCompletableFuture)
                .toArray(CompletableFuture[]::new)));
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
            // Redis URLs may contain credentials; do not include them in logs.
            LOG.warn("Redis connection failure detected, invalidating shared connection: {}",
                    t.getMessage());
            try {
                redisClient.invalidate();
            } catch (Exception ex) {
                LOG.warn("Failed to invalidate Redis connection: {}", ex.getMessage());
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
