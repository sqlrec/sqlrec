package com.sqlrec.connectors.redis.handler;

import com.google.protobuf.Field;
import com.google.protobuf.StringValue;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.redis.client.AbstractRedisWrapper;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
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

    /** RedisFuture mock used by lpush command (list mode) */
    @Mock
    RedisFuture<Long> mockLpushFuture;

    /** RedisFuture mock used by ltrim command (list mode) */
    @Mock
    RedisFuture<String> mockLtrimFuture;

    /** RedisFuture mock used by lrem command (list mode delete) */
    @Mock
    RedisFuture<Long> mockLremFuture;

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
    void testScanByEmptyKeySetDoesNotCallRedis() throws Exception {
        assertTrue(handler.scan(Collections.emptySet()).get().isEmpty());
        assertTrue(handler.scan((Set<String>) null).get().isEmpty());
        verifyNoInteractions(mockRedisClient);
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
        when(mockRedisClient.setex(any(), any(), anyLong())).thenReturn(mockSetFuture);
        when(mockSetFuture.get(anyLong(), any(TimeUnit.class))).thenReturn("OK");

        handler.insert(new Object[]{"rowKey", "name_value"});

        // insert must use a single SET ... EX command instead of SET + EXPIRE
        verify(mockRedisClient).setex(any(), any(), anyLong());
        verify(mockRedisClient, never()).set(any(), any());
        verify(mockRedisClient, never()).expire(any(), anyLong());
    }

    @Test
    void testProtobufInsertAndScan() throws Exception {
        RedisConfig config = new RedisConfig();
        config.url = "redis://localhost:6379";
        config.redisMode = RedisOptions.SINGLE_MODE;
        config.dataStructure = RedisOptions.JSON_DATA_STRUCTURE;
        config.format = RedisOptions.PROTOBUF_FORMAT;
        config.protobufMessageClassName = "com.google.protobuf.StringValue";
        config.database = "testdb";
        config.tableName = "protobuf_table";
        config.primaryKeyIndex = 0;
        config.primaryKey = "value";
        config.fieldSchemas = Collections.singletonList(new FieldSchema("value", "VARCHAR"));
        config.ttl = 3600;
        config.maxListSize = 0;

        RedisHandler protobufHandler = new RedisHandler(config);
        protobufHandler.open();
        protobufHandler.setRedisClientForTest(mockRedisClient);

        when(mockRedisClient.setex(any(), any(), anyLong())).thenReturn(mockSetFuture);
        when(mockSetFuture.get(anyLong(), any(TimeUnit.class))).thenReturn("OK");

        protobufHandler.insert(new Object[]{"hello"});

        ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockRedisClient).setex(any(), valueCaptor.capture(), eq(3600L));
        assertEquals("hello", StringValue.parseFrom(valueCaptor.getValue()).getValue());

        when(mockRedisClient.get(any())).thenReturn(mockGetFuture);
        when(mockGetFuture.toCompletableFuture()).thenReturn(
                CompletableFuture.completedFuture(StringValue.of("world").toByteArray()));

        List<Object[]> rows = protobufHandler.scan("hello").get();
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{"world"}, rows.get(0));
    }

    @Test
    void testProtobufScanMultiKeys() throws Exception {
        RedisHandler protobufHandler = newProtobufHandler(false);
        byte[] key1Bytes = "testdb:protobuf_table:key1".getBytes(StandardCharsets.UTF_8);
        byte[] key2Bytes = "testdb:protobuf_table:key2".getBytes(StandardCharsets.UTF_8);
        List<KeyValue<byte[], byte[]>> values = Arrays.asList(
                KeyValue.just(key1Bytes, Field.newBuilder()
                        .setName("key1").setJsonName("value1").build().toByteArray()),
                KeyValue.empty(key2Bytes));
        when(mockRedisClient.mget(any(byte[].class), any(byte[].class))).thenReturn(mockMgetFuture);
        when(mockMgetFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(values));

        Map<String, List<Object[]>> rows = protobufHandler.scan(Set.of("key1", "key2")).get();

        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{"key1", "value1"}, rows.get("key1").get(0));
    }

    @Test
    void testProtobufBatchInsertEncodesEveryValue() throws Exception {
        RedisHandler protobufHandler = newProtobufHandler(false);
        when(mockRedisClient.setex(any(), any(), anyLong())).thenReturn(mockSetFuture);
        when(mockSetFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));

        protobufHandler.batchInsert(Arrays.asList(
                new Object[]{"key1", "value1"},
                new Object[]{"key2", "value2"}));

        ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockRedisClient, times(2)).setex(any(), valueCaptor.capture(), eq(60L));
        assertEquals("value1", Field.parseFrom(valueCaptor.getAllValues().get(0)).getJsonName());
        assertEquals("value2", Field.parseFrom(valueCaptor.getAllValues().get(1)).getJsonName());
    }

    @Test
    void testProtobufScanFailsForMalformedPayload() {
        RedisHandler protobufHandler = newProtobufHandler(false);
        when(mockRedisClient.get(any())).thenReturn(mockGetFuture);
        when(mockGetFuture.toCompletableFuture()).thenReturn(
                CompletableFuture.completedFuture(new byte[]{0x0A, 0x05, 0x01}));

        ExecutionException error = assertThrows(
                ExecutionException.class,
                () -> protobufHandler.scan("key").get());

        assertTrue(error.getCause() instanceof IllegalArgumentException);
    }

    @Test
    void testProtobufHandlerRejectsInvalidMessageClassOnOpen() {
        RedisConfig config = protobufConfig(false);
        config.protobufMessageClassName = "example.missing.Message";

        assertThrows(IllegalArgumentException.class, () -> new RedisHandler(config).open());
    }

    @Test
    void testInsertThrowsWrappedException() throws Exception {
        when(mockRedisClient.setex(any(), any(), anyLong())).thenReturn(mockSetFuture);
        when(mockSetFuture.get(anyLong(), any(TimeUnit.class)))
                .thenThrow(new ExecutionException(new RuntimeException("connection lost")));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> handler.insert(new Object[]{"key", "val"}));
        assertTrue(ex.getMessage().contains("Failed to insert data to Redis"));
    }

    @Test
    void testBatchInsertUsesAsyncSetex() throws Exception {
        when(mockRedisClient.setex(any(), any(), anyLong())).thenReturn(mockSetFuture);
        when(mockSetFuture.toCompletableFuture())
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture("OK"));

        handler.batchInsert(Arrays.asList(
                new Object[]{"key1", "v1"},
                new Object[]{"key2", "v2"}));

        // one SET ... EX per row, no EXPIRE
        verify(mockRedisClient, times(2)).setex(any(), any(), anyLong());
        verify(mockRedisClient, never()).expire(any(), anyLong());
    }

    @Test
    void testBatchInsertSubmitsAllCommandsBeforeWaiting() {
        RedisFuture<String> firstFuture = mock(RedisFuture.class);
        RedisFuture<String> secondFuture = mock(RedisFuture.class);
        when(mockRedisClient.setex(any(), any(), anyLong()))
                .thenReturn(firstFuture, secondFuture);
        when(firstFuture.toCompletableFuture())
                .thenReturn(CompletableFuture.completedFuture("OK"));
        when(secondFuture.toCompletableFuture())
                .thenReturn(CompletableFuture.completedFuture("OK"));

        handler.batchInsert(Arrays.asList(
                new Object[]{"key1", "v1"},
                new Object[]{"key2", "v2"}));

        InOrder order = inOrder(mockRedisClient, firstFuture, secondFuture);
        order.verify(mockRedisClient, times(2)).setex(any(), any(), anyLong());
        order.verify(firstFuture).toCompletableFuture();
        order.verify(secondFuture).toCompletableFuture();
    }

    @Test
    void testBatchDeleteSubmitsAllCommandsAndWaits() {
        RedisFuture<Long> firstFuture = mock(RedisFuture.class);
        RedisFuture<Long> secondFuture = mock(RedisFuture.class);
        when(mockRedisClient.del(any())).thenReturn(firstFuture, secondFuture);
        when(firstFuture.toCompletableFuture())
                .thenReturn(CompletableFuture.completedFuture(1L));
        when(secondFuture.toCompletableFuture())
                .thenReturn(CompletableFuture.completedFuture(1L));

        handler.batchDelete(Arrays.asList(
                new Object[]{"key1", "v1"},
                new Object[]{"key2", "v2"}));

        InOrder order = inOrder(mockRedisClient, firstFuture, secondFuture);
        order.verify(mockRedisClient, times(2)).del(any());
        order.verify(firstFuture).toCompletableFuture();
        order.verify(secondFuture).toCompletableFuture();
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

    // ------------------------------------------------------------------
    // Key construction format: {database}:{tableName}:{primaryKey} as UTF-8 bytes
    // ------------------------------------------------------------------

    @Test
    void testInsertKeyFormatAndTtl() throws Exception {
        when(mockRedisClient.setex(any(), any(), anyLong())).thenReturn(mockSetFuture);
        when(mockSetFuture.get(anyLong(), any(TimeUnit.class))).thenReturn("OK");

        handler.insert(new Object[]{"rowKey", "name_value"});

        ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<Long> ttlCaptor = ArgumentCaptor.forClass(Long.class);
        verify(mockRedisClient).setex(keyCaptor.capture(), any(), ttlCaptor.capture());
        assertEquals("testdb:testtable:rowKey",
                new String(keyCaptor.getValue(), StandardCharsets.UTF_8));
        assertEquals(3600L, ttlCaptor.getValue());
    }

    @Test
    void testDeleteKeyFormat() throws Exception {
        when(mockRedisClient.del(any())).thenReturn(mockDelFuture);
        when(mockDelFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(1L);

        handler.delete(new Object[]{"rowKey", "ignored"});

        ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockRedisClient).del(keyCaptor.capture());
        assertEquals("testdb:testtable:rowKey",
                new String(keyCaptor.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testBatchInsertKeyFormat() throws Exception {
        when(mockRedisClient.setex(any(), any(), anyLong())).thenReturn(mockSetFuture);
        when(mockSetFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));

        handler.batchInsert(Arrays.asList(
                new Object[]{"k1", "v1"},
                new Object[]{"k2", "v2"}));

        ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockRedisClient, times(2)).setex(keyCaptor.capture(), any(), anyLong());
        List<String> keys = keyCaptor.getAllValues().stream()
                .map(b -> new String(b, StandardCharsets.UTF_8))
                .collect(java.util.stream.Collectors.toList());
        assertEquals(Arrays.asList("testdb:testtable:k1", "testdb:testtable:k2"), keys);
    }

    // ------------------------------------------------------------------
    // List data structure mode
    // ------------------------------------------------------------------

    /** Build a handler configured for list mode (maxListSize=5, ttl=60). */
    private RedisHandler newListModeHandler() {
        RedisConfig config = new RedisConfig();
        config.url = "redis://localhost:6379";
        config.redisMode = RedisOptions.SINGLE_MODE;
        config.dataStructure = RedisOptions.LIST_DATA_STRUCTURE;
        config.database = "testdb";
        config.tableName = "testtable";
        config.primaryKeyIndex = 0;
        config.primaryKey = "id";
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "string"),
                new FieldSchema("name", "string"));
        config.ttl = 60;
        config.maxListSize = 5;
        RedisHandler listHandler = new RedisHandler(config);
        listHandler.open();
        listHandler.setRedisClientForTest(mockRedisClient);
        return listHandler;
    }

    private RedisConfig protobufConfig(boolean listMode) {
        RedisConfig config = new RedisConfig();
        config.url = "redis://localhost:6379";
        config.redisMode = RedisOptions.SINGLE_MODE;
        config.dataStructure = listMode
                ? RedisOptions.LIST_DATA_STRUCTURE : RedisOptions.JSON_DATA_STRUCTURE;
        config.format = RedisOptions.PROTOBUF_FORMAT;
        config.protobufMessageClassName = "com.google.protobuf.Field";
        config.database = "testdb";
        config.tableName = "protobuf_table";
        config.primaryKeyIndex = 0;
        config.primaryKey = "name";
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("json_name", "VARCHAR"));
        config.ttl = 60;
        config.maxListSize = 5;
        return config;
    }

    private RedisHandler newProtobufHandler(boolean listMode) {
        RedisHandler protobufHandler = new RedisHandler(protobufConfig(listMode));
        protobufHandler.open();
        protobufHandler.setRedisClientForTest(mockRedisClient);
        return protobufHandler;
    }

    @Test
    void testListModeInsertIssuesLpushLtrimExpire() throws Exception {
        RedisHandler listHandler = newListModeHandler();
        when(mockRedisClient.lpush(any(), any(byte[][].class))).thenReturn(mockLpushFuture);
        when(mockRedisClient.ltrim(any(), anyLong(), anyLong())).thenReturn(mockLtrimFuture);
        when(mockRedisClient.expire(any(), anyLong())).thenReturn(mockExpireFuture);
        when(mockLpushFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        when(mockLtrimFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(mockExpireFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(true));

        listHandler.insert(new Object[]{"rowKey", "v1"});

        byte[] expectedKey = "testdb:testtable:rowKey".getBytes(StandardCharsets.UTF_8);
        InOrder inOrder = inOrder(mockRedisClient);
        inOrder.verify(mockRedisClient).lpush(eq(expectedKey), any(byte[][].class));
        // ltrim keeps the newest maxListSize elements
        inOrder.verify(mockRedisClient).ltrim(eq(expectedKey), eq(0L), eq(4L));
        inOrder.verify(mockRedisClient).expire(eq(expectedKey), eq(60L));
        // list mode must not use SET
        verify(mockRedisClient, never()).setex(any(), any(), anyLong());
    }

    @Test
    void testListModeBatchInsertAggregatesSameKey() throws Exception {
        RedisHandler listHandler = newListModeHandler();
        when(mockRedisClient.lpush(any(), any(byte[][].class))).thenReturn(mockLpushFuture);
        when(mockRedisClient.ltrim(any(), anyLong(), anyLong())).thenReturn(mockLtrimFuture);
        when(mockRedisClient.expire(any(), anyLong())).thenReturn(mockExpireFuture);
        when(mockLpushFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(2L));
        when(mockLtrimFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(mockExpireFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(true));

        listHandler.batchInsert(Arrays.asList(
                new Object[]{"k1", "v1"},
                new Object[]{"k1", "v2"},
                new Object[]{"k2", "v3"}));

        // 3 rows but only 2 distinct keys -> one LPUSH per key, same key aggregated
        ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockRedisClient, times(2)).lpush(keyCaptor.capture(), any(byte[][].class));
        List<String> keys = keyCaptor.getAllValues().stream()
                .map(b -> new String(b, StandardCharsets.UTF_8))
                .collect(java.util.stream.Collectors.toList());
        assertEquals(Arrays.asList("testdb:testtable:k1", "testdb:testtable:k2"), keys);
        verify(mockRedisClient, times(2)).ltrim(any(), anyLong(), anyLong());
        verify(mockRedisClient, times(2)).expire(any(), anyLong());
    }

    @Test
    void testListModeDeleteUsesLrem() throws Exception {
        RedisHandler listHandler = newListModeHandler();
        when(mockRedisClient.lrem(any(), any())).thenReturn(mockLremFuture);
        when(mockLremFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(1L);

        listHandler.delete(new Object[]{"rowKey", "v1"});

        ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockRedisClient).lrem(keyCaptor.capture(), any());
        assertEquals("testdb:testtable:rowKey",
                new String(keyCaptor.getValue(), StandardCharsets.UTF_8));
        // list mode must not use DEL
        verify(mockRedisClient, never()).del(any());
    }

    @Test
    void testProtobufListInsertAndScan() throws Exception {
        RedisHandler protobufHandler = newProtobufHandler(true);
        when(mockRedisClient.lpush(any(), any(byte[][].class))).thenReturn(mockLpushFuture);
        when(mockRedisClient.ltrim(any(), anyLong(), anyLong())).thenReturn(mockLtrimFuture);
        when(mockRedisClient.expire(any(), anyLong())).thenReturn(mockExpireFuture);
        when(mockLpushFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        when(mockLtrimFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(mockExpireFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(true));

        protobufHandler.insert(new Object[]{"rowKey", "value1"});

        ArgumentCaptor<byte[][]> valuesCaptor = ArgumentCaptor.forClass(byte[][].class);
        verify(mockRedisClient).lpush(any(), valuesCaptor.capture());
        Field stored = Field.parseFrom(valuesCaptor.getValue()[0]);
        assertEquals("rowKey", stored.getName());
        assertEquals("value1", stored.getJsonName());

        RedisFuture<List<byte[]>> listFuture = mock(RedisFuture.class);
        when(mockRedisClient.lrange(any(), eq(0L), eq(-1L))).thenReturn(listFuture);
        when(listFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(Arrays.asList(
                Field.newBuilder().setName("rowKey").setJsonName("value1").build().toByteArray(),
                new byte[]{0x0A, 0x05, 0x01})));

        List<Object[]> rows = protobufHandler.scan("rowKey").get();
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{"rowKey", "value1"}, rows.get(0));
    }

    @Test
    void testProtobufListBatchInsertAndDelete() throws Exception {
        RedisHandler protobufHandler = newProtobufHandler(true);
        when(mockRedisClient.lpush(any(), any(byte[][].class))).thenReturn(mockLpushFuture);
        when(mockRedisClient.ltrim(any(), anyLong(), anyLong())).thenReturn(mockLtrimFuture);
        when(mockRedisClient.expire(any(), anyLong())).thenReturn(mockExpireFuture);
        when(mockLpushFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(2L));
        when(mockLtrimFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(mockExpireFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(true));

        List<Object[]> rows = Arrays.asList(
                new Object[]{"rowKey", "value1"},
                new Object[]{"rowKey", "value2"});
        protobufHandler.batchInsert(rows);

        ArgumentCaptor<byte[][]> batchValuesCaptor = ArgumentCaptor.forClass(byte[][].class);
        verify(mockRedisClient).lpush(any(), batchValuesCaptor.capture());
        assertEquals(2, batchValuesCaptor.getValue().length);
        assertEquals("value1", Field.parseFrom(batchValuesCaptor.getValue()[0]).getJsonName());
        assertEquals("value2", Field.parseFrom(batchValuesCaptor.getValue()[1]).getJsonName());

        when(mockRedisClient.lrem(any(), any())).thenReturn(mockLremFuture);
        when(mockLremFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        protobufHandler.batchDelete(rows);

        ArgumentCaptor<byte[]> deleteValueCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockRedisClient, times(2)).lrem(any(), deleteValueCaptor.capture());
        assertEquals("value1", Field.parseFrom(deleteValueCaptor.getAllValues().get(0)).getJsonName());
        assertEquals("value2", Field.parseFrom(deleteValueCaptor.getAllValues().get(1)).getJsonName());
        verify(mockRedisClient, never()).del(any());
    }

    @Test
    void testProtobufListDeleteUsesEncodedMessage() throws Exception {
        RedisHandler protobufHandler = newProtobufHandler(true);
        when(mockRedisClient.lrem(any(), any())).thenReturn(mockLremFuture);
        when(mockLremFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(1L);

        protobufHandler.delete(new Object[]{"rowKey", "value1"});

        ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockRedisClient).lrem(any(), valueCaptor.capture());
        Field deleted = Field.parseFrom(valueCaptor.getValue());
        assertEquals("rowKey", deleted.getName());
        assertEquals("value1", deleted.getJsonName());
    }
}
