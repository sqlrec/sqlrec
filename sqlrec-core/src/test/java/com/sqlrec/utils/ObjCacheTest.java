package com.sqlrec.utils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class ObjCacheTest {

    @Test
    public void testGetObjWithInitialNull() {
        AtomicInteger callCount = new AtomicInteger(0);
        ObjCache<String> cache = new ObjCache<>(1000, false, obj -> {
            callCount.incrementAndGet();
            return "test_value";
        });

        String result = cache.getObj();

        assertEquals("test_value", result);
        assertEquals(1, callCount.get());
    }

    @Test
    public void testGetObjWithCacheHit() {
        AtomicInteger callCount = new AtomicInteger(0);
        ObjCache<String> cache = new ObjCache<>(10000, false, obj -> {
            callCount.incrementAndGet();
            return "test_value";
        });

        cache.getObj();
        String result = cache.getObj();

        assertEquals("test_value", result);
        assertEquals(1, callCount.get());
    }

    @Test
    public void testGetObjWithMultipleRequestsInCachePeriod() {
        AtomicInteger callCount = new AtomicInteger(0);
        ObjCache<String> cache = new ObjCache<>(1000, false, obj -> {
            callCount.incrementAndGet();
            return "test_value_" + callCount.get();
        });

        String result1 = cache.getObj();
        String result2 = cache.getObj();
        String result3 = cache.getObj();
        String result4 = cache.getObj();
        String result5 = cache.getObj();

        assertEquals("test_value_1", result1);
        assertEquals("test_value_1", result2);
        assertEquals("test_value_1", result3);
        assertEquals("test_value_1", result4);
        assertEquals("test_value_1", result5);
        assertEquals(1, callCount.get());
    }

    @Test
    public void testGetObjWithCacheExpired() throws InterruptedException {
        AtomicInteger callCount = new AtomicInteger(0);
        ObjCache<String> cache = new ObjCache<>(10, false, obj -> {
            callCount.incrementAndGet();
            return "test_value_" + callCount.get();
        });

        String result1 = cache.getObj();
        Thread.sleep(50);
        String result2 = cache.getObj();

        assertEquals("test_value_1", result1);
        assertEquals("test_value_2", result2);
        assertEquals(2, callCount.get());
    }

    @Test
    public void testGetObjWithUpdateFunctionReturningNull() {
        ObjCache<String> cache = new ObjCache<>(1000, false, obj -> null);

        String result = cache.getObj();

        assertNull(result);
    }

    @Test
    public void testGetObjWithUpdateFunctionUsingPreviousValue() {
        AtomicInteger callCount = new AtomicInteger(0);
        ObjCache<String> cache = new ObjCache<>(10, false, obj -> {
            callCount.incrementAndGet();
            if (obj == null) {
                return "initial";
            }
            return obj + "_updated";
        });

        String result1 = cache.getObj();
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        String result2 = cache.getObj();

        assertEquals("initial", result1);
        assertEquals("initial_updated", result2);
    }

    @Test
    public void testGetObjWithAsyncUpdate() throws InterruptedException {
        AtomicInteger callCount = new AtomicInteger(0);
        ObjCache<String> cache = new ObjCache<>(10, true, obj -> {
            callCount.incrementAndGet();
            return "test_value_" + callCount.get();
        });

        String result1 = cache.getObj();
        Thread.sleep(50);
        String result2 = cache.getObj();

        assertEquals("test_value_1", result1);
        assertTrue(callCount.get() >= 1);
    }

    @Test
    public void testGetObjWithExceptionInUpdateFunction() {
        ObjCache<String> cache = new ObjCache<>(1000, false, obj -> {
            throw new RuntimeException("Test exception");
        });

        assertThrows(RuntimeException.class, cache::getObj);
    }

    @Test
    public void testGetObjWithVeryShortExpireTime() throws InterruptedException {
        AtomicInteger callCount = new AtomicInteger(0);
        ObjCache<String> cache = new ObjCache<>(1, false, obj -> {
            callCount.incrementAndGet();
            return "test_value_" + callCount.get();
        });

        String result1 = cache.getObj();
        Thread.sleep(10);
        String result2 = cache.getObj();

        assertEquals("test_value_1", result1);
        assertEquals("test_value_2", result2);
        assertEquals(2, callCount.get());
    }
}
