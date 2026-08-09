package com.sqlrec.connectors.redis.handler;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.redis.client.AbstractRedisWrapper;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Mock unit tests for RedisHandler.
 * Injects a mock AbstractRedisWrapper via setRedisClientForTest to verify
 * that handler scan / delete / close logic delegates correctly to the underlying wrapper.
 */
@ExtendWith(MockitoExtension.class)
class RedisHandlerUnitTest {

    @Mock
    AbstractRedisWrapper mockRedisClient;

    /** RedisFuture mock used by scan */
    @Mock
    RedisFuture<byte[]> mockGetFuture;

    /** RedisFuture mock used by delete */
    @Mock
    RedisFuture<Long> mockDelFuture;

    /** RedisFuture mock used by insert (set command) */
    @Mock
    RedisFuture<String> mockSetFuture;

    /** RedisFuture mock used by expire command */
    @Mock
    RedisFuture<Boolean> mockExpireFuture;

    /** RedisFuture mock used by scan multi-keys (mget command) */
    @Mock
    RedisFuture<List<KeyValue<byte[], byte[]>>> mockMgetFuture;

    private RedisHandler handler;

    @BeforeEach
    void setUp() {
        // Build a minimal RedisConfig (json mode, not list)
        RedisConfig config = new RedisConfig();
        config.url = "redis://localhost:6379";
        config.redisMode = RedisOptions.SINGLE_MODE;
        config.dataStructure = RedisOptions.JSON_DATA_STRUCTURE;
        config.database = "testdb";
        config.tableName = "testtable";
        config.primaryKeyIndex = 0;
        config.primaryKey = "id";
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "string"),
                new FieldSchema("name", "string")
        );
        config.ttl = 3600;
        config.maxListSize = 0;

        handler = new RedisHandler(config);
        // open() creates a real RedisWrapper (only stores url, no connection) and initializes codec/keyPrefix
        handler.open();
        // Replace the real wrapper with the mock
        handler.setRedisClientForTest(mockRedisClient);
    }

    @Test
    void testScanByKey() throws Exception {
        // Simulate Redis returning JSON-encoded data
        byte[] encodedData = "{\"id\":\"rowKey\",\"name\":\"test\"}".getBytes();
        when(mockRedisClient.get(any())).thenReturn(mockGetFuture);
        when(mockGetFuture.toCompletableFuture())
                .thenReturn(CompletableFuture.completedFuture(encodedData));

        // scan should return a non-null CompletableFuture
        CompletableFuture<List<Object[]>> result = handler.scan("rowKey");
        assertNotNull(result);

        // The result should contain 1 row
        List<Object[]> data = result.get();
        assertNotNull(data);
        assertEquals(1, data.size());
    }

    @Test
    void testScanByKeyNull() throws Exception {
        // Simulate Redis returning null (key does not exist)
        when(mockRedisClient.get(any())).thenReturn(mockGetFuture);
        when(mockGetFuture.toCompletableFuture())
                .thenReturn(CompletableFuture.completedFuture(null));

        // scan should return an empty list
        CompletableFuture<List<Object[]>> result = handler.scan("rowKey");
        List<Object[]> data = result.get();
        assertNotNull(data);
        assertTrue(data.isEmpty());
    }

    @Test
    void testDelete() throws Exception {
        // Mock del command return value
        when(mockRedisClient.del(any())).thenReturn(mockDelFuture);
        when(mockDelFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(1L);

        // primaryKeyIndex=0, so data[0] is used as the key
        handler.delete(new Object[]{"key_value", "data"});

        // Verify the underlying del command was called
        verify(mockRedisClient).del(any());
    }

    @Test
    void testClose() {
        // close should call redisClient.close()
        handler.close();

        // Verify the underlying wrapper's close was called
        verify(mockRedisClient).close();
    }

    @Test
    void testInsertKeyValue() throws Exception {
        when(mockRedisClient.set(any(), any())).thenReturn(mockSetFuture);
        when(mockRedisClient.expire(any(), anyLong())).thenReturn(mockExpireFuture);
        when(mockSetFuture.get(anyLong(), any(TimeUnit.class))).thenReturn("OK");
        when(mockExpireFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(true);

        handler.insert(new Object[]{"rowKey", "name_value"});

        verify(mockRedisClient).set(any(), any());
        verify(mockRedisClient).expire(any(), anyLong());
    }

    @Test
    void testInsertThrowsWrappedException() throws Exception {
        when(mockRedisClient.set(any(), any())).thenReturn(mockSetFuture);
        when(mockSetFuture.get(anyLong(), any(TimeUnit.class)))
                .thenThrow(new ExecutionException(new RuntimeException("connection lost")));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> handler.insert(new Object[]{"key", "val"}));
        assertTrue(ex.getMessage().contains("Failed to insert data to Redis"));
    }

    @Test
    void testDeleteThrowsWrappedException() throws Exception {
        when(mockRedisClient.del(any())).thenReturn(mockDelFuture);
        when(mockDelFuture.get(anyLong(), any(TimeUnit.class)))
                .thenThrow(new ExecutionException(new RuntimeException("delete failed")));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> handler.delete(new Object[]{"key", "val"}));
        assertTrue(ex.getMessage().contains("Failed to delete data from Redis"));
    }

    @Test
    void testGetKeyWithNullPrimaryKey() {
        Object[] data = new Object[]{null, "val"};
        assertThrows(IllegalArgumentException.class, () -> handler.insert(data));
    }

    @Test
    void testDecodeListWithMalformedData() {
        List<byte[]> list = Arrays.asList(
                "{\"id\":\"rowKey\",\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8),
                "not json".getBytes(StandardCharsets.UTF_8));

        List<Object[]> result = handler.decodeList(list, "rowKey");

        assertEquals(1, result.size());
    }

    @Test
    void testScanMultiKeys() throws Exception {
        byte[] key1Bytes = "testdb:testtable:key1".getBytes(StandardCharsets.UTF_8);
        byte[] key2Bytes = "testdb:testtable:key2".getBytes(StandardCharsets.UTF_8);
        List<KeyValue<byte[], byte[]>> kvList = Arrays.asList(
                KeyValue.just(key1Bytes, "{\"id\":\"key1\",\"name\":\"val1\"}".getBytes(StandardCharsets.UTF_8)),
                KeyValue.empty(key2Bytes));
        when(mockRedisClient.mget(any(byte[].class), any(byte[].class))).thenReturn(mockMgetFuture);
        when(mockMgetFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(kvList));

        CompletableFuture<Map<String, List<Object[]>>> result = handler.scan(Set.of("key1", "key2"));
        Map<String, List<Object[]>> data = result.get();

        assertEquals(1, data.size());
    }
}
