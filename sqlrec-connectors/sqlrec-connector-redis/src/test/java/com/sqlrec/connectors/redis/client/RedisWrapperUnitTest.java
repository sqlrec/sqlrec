package com.sqlrec.connectors.redis.client;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Mock unit tests for RedisWrapper.
 * Injects mock Lettuce connection and client via setConnectionForTest to verify
 * that wrapper command methods delegate correctly to the underlying async commands.
 */
@ExtendWith(MockitoExtension.class)
class RedisWrapperUnitTest {

    private static final String URL = "redis://localhost:6379";

    @Mock
    StatefulRedisConnection<byte[], byte[]> mockConn;

    @Mock
    RedisAsyncCommands<byte[], byte[]> mockAsync;

    @Mock
    RedisClient mockRedisClient;

    /** Return value mock for testGet */
    @Mock
    RedisFuture<byte[]> mockGetFuture;

    /** Return value mock for testLrange */
    @Mock
    RedisFuture<List<byte[]>> mockLrangeFuture;

    /** Return value mock for testMget */
    @Mock
    RedisFuture<List<KeyValue<byte[], byte[]>>> mockMgetFuture;

    private RedisWrapper wrapper;

    @BeforeEach
    void setUp() {
        // Inject mock connection and client to avoid real Redis connections
        RedisWrapper.setConnectionForTest(URL, mockConn, mockRedisClient);
        wrapper = new RedisWrapper();
        // open() only stores the url; connection is lazy
        wrapper.open(URL);
    }

    @AfterEach
    void tearDown() {
        // Clean up mock references from the static map
        RedisWrapper.invalidate(URL);
    }

    @Test
    void testGet() throws Exception {
        // Mock async command chain
        when(mockConn.async()).thenReturn(mockAsync);
        when(mockAsync.get(any())).thenReturn(mockGetFuture);
        when(mockGetFuture.get()).thenReturn("value".getBytes());

        // Call wrapper.get; the returned RedisFuture.get() should be the expected value
        RedisFuture<byte[]> future = wrapper.get("key".getBytes());
        assertArrayEquals("value".getBytes(), future.get());
    }

    @Test
    void testSet() {
        when(mockConn.async()).thenReturn(mockAsync);

        wrapper.set("key".getBytes(), "value".getBytes());

        // Verify the underlying async set command was called
        verify(mockAsync).set(any(), any());
    }

    @Test
    void testDel() {
        when(mockConn.async()).thenReturn(mockAsync);

        wrapper.del("key".getBytes());

        // Verify the underlying async del command was called
        verify(mockAsync).del(any());
    }

    @Test
    void testLpush() {
        when(mockConn.async()).thenReturn(mockAsync);

        wrapper.lpush("key".getBytes(), "value".getBytes());

        // Verify the underlying async lpush command was called
        verify(mockAsync).lpush(any(), any());
    }

    @Test
    void testLrange() throws Exception {
        when(mockConn.async()).thenReturn(mockAsync);
        when(mockAsync.lrange(any(), eq(0L), eq(-1L))).thenReturn(mockLrangeFuture);
        when(mockLrangeFuture.get()).thenReturn(
                Arrays.asList("v1".getBytes(), "v2".getBytes()));

        // Call wrapper.lrange; the returned list should have size 2
        RedisFuture<List<byte[]>> future = wrapper.lrange("key".getBytes(), 0, -1);
        List<byte[]> result = future.get();
        assertEquals(2, result.size());
    }

    @Test
    void testInvalidate() {
        // Call invalidate; should close the connection and shut down the client
        RedisWrapper.invalidate(URL);

        // Verify the connection was closed and the client was shut down
        verify(mockConn).close();
        verify(mockRedisClient).shutdown();
    }

    @Test
    void testMget() throws Exception {
        when(mockConn.async()).thenReturn(mockAsync);
        when(mockAsync.mget(any())).thenReturn(mockMgetFuture);
        List<KeyValue<byte[], byte[]>> expected = Arrays.asList(
                KeyValue.just("k1".getBytes(), "v1".getBytes()));
        when(mockMgetFuture.get()).thenReturn(expected);

        RedisFuture<List<KeyValue<byte[], byte[]>>> future = wrapper.mget("k1".getBytes());
        assertEquals(expected, future.get());
    }

    @Test
    void testSetex() {
        when(mockConn.async()).thenReturn(mockAsync);

        wrapper.setex("key".getBytes(), "value".getBytes(), 60);

        // Verify the underlying async set command was called with SetArgs carrying the ttl
        verify(mockAsync).set(any(), any(), any(io.lettuce.core.SetArgs.class));
    }

    @Test
    void testExecutePipelined() {
        when(mockConn.async()).thenReturn(mockAsync);

        List<RedisFuture<?>> futures = wrapper.executePipelined(() ->
                Collections.singletonList(wrapper.set("k".getBytes(), "v".getBytes())));

        // Flushing must be suspended around the producer, commands flushed once, then resumed
        verify(mockConn).setAutoFlushCommands(false);
        verify(mockConn).flushCommands();
        verify(mockConn).setAutoFlushCommands(true);
        assertEquals(1, futures.size());
        verify(mockAsync).set(any(), any());
    }

    @Test
    void testExpire() {
        when(mockConn.async()).thenReturn(mockAsync);

        wrapper.expire("key".getBytes(), 60);

        verify(mockAsync).expire(any(), eq(60L));
    }

    @Test
    void testLrem() {
        when(mockConn.async()).thenReturn(mockAsync);

        wrapper.lrem("key".getBytes(), "value".getBytes());

        verify(mockAsync).lrem(any(), eq(0L), any());
    }

    @Test
    void testLtrim() {
        when(mockConn.async()).thenReturn(mockAsync);

        wrapper.ltrim("key".getBytes(), 0, 99);

        verify(mockAsync).ltrim(any(), eq(0L), eq(99L));
    }

    @Test
    void testDoubleInvalidate() {
        // First invalidate removes the entry and closes the connection/client
        RedisWrapper.invalidate(URL);
        // Second invalidate is a no-op (entry already removed from the map)
        RedisWrapper.invalidate(URL);

        // close/shutdown should only be called once (during the first invalidate)
        verify(mockConn, times(1)).close();
        verify(mockRedisClient, times(1)).shutdown();
    }
}
