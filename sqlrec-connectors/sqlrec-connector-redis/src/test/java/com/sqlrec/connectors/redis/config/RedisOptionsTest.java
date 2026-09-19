package com.sqlrec.connectors.redis.config;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisOptionsTest {

    @Test
    void usesDocumentedDefaults() {
        RedisConfig config = RedisOptions.getRedisConfig(
                Collections.singletonMap("url", "redis://localhost:6379"));

        assertEquals(RedisOptions.SINGLE_MODE, config.redisMode);
        assertEquals(RedisOptions.JSON_DATA_STRUCTURE, config.dataStructure);
        assertEquals(0, config.maxListSize);
        assertEquals(30 * 24 * 3600, config.ttl);
        assertEquals(30, config.cacheTtl);
        assertEquals(100_000, config.maxCacheSize);
        assertEquals(1_000, config.batchSize);
        assertEquals(1L, config.flushInterval);
    }

    @Test
    void readsEverySupportedOption() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "redis://localhost:6379");
        options.put("redis-mode", "cluster");
        options.put("data-structure", "list");
        options.put("max-list-size", "25");
        options.put("ttl", "60");
        options.put("cache-ttl", "7");
        options.put("max-cache-size", "500");
        options.put("batch-size", "32");
        options.put("flush-interval", "9");

        RedisConfig config = RedisOptions.getRedisConfig(options);

        assertEquals("redis://localhost:6379", config.url);
        assertEquals("cluster", config.redisMode);
        assertEquals("list", config.dataStructure);
        assertEquals(25, config.maxListSize);
        assertEquals(60, config.ttl);
        assertEquals(7, config.cacheTtl);
        assertEquals(500, config.maxCacheSize);
        assertEquals(32, config.batchSize);
        assertEquals(9L, config.flushInterval);
    }
}
