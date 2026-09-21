package com.sqlrec.connectors.redis.config;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RedisOptionsTest {

    @Test
    void usesDocumentedDefaults() {
        RedisConfig config = RedisOptions.getRedisConfig(
                Collections.singletonMap("url", "redis://localhost:6379"));

        assertEquals(RedisOptions.SINGLE_MODE, config.redisMode);
        assertEquals(RedisOptions.JSON_DATA_STRUCTURE, config.dataStructure);
        assertEquals(RedisOptions.JSON_FORMAT, config.format);
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

    @Test
    void readsProtobufFormat() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "redis://localhost:6379");
        options.put("format", "protobuf");
        options.put("protobuf.message-class-name", "com.google.protobuf.StringValue");

        RedisConfig config = RedisOptions.getRedisConfig(options);

        assertEquals(RedisOptions.PROTOBUF_FORMAT, config.format);
        assertEquals("com.google.protobuf.StringValue", config.protobufMessageClassName);
    }

    @Test
    void requiresMessageClassForProtobuf() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "redis://localhost:6379");
        options.put("format", "protobuf");

        assertThrows(IllegalArgumentException.class,
                () -> RedisOptions.getRedisConfig(options));
    }

    @Test
    void rejectsBlankMessageClassForProtobuf() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "redis://localhost:6379");
        options.put("format", "protobuf");
        options.put("protobuf.message-class-name", "   ");

        assertThrows(IllegalArgumentException.class,
                () -> RedisOptions.getRedisConfig(options));
    }

    @Test
    void rejectsUnknownFormat() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "redis://localhost:6379");
        options.put("format", "avro");

        assertThrows(IllegalArgumentException.class,
                () -> RedisOptions.getRedisConfig(options));
    }

    @Test
    void acceptsProtobufWithListDataStructure() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "redis://localhost:6379");
        options.put("data-structure", "list");
        options.put("format", "protobuf");
        options.put("protobuf.message-class-name", "com.google.protobuf.StringValue");

        RedisConfig config = RedisOptions.getRedisConfig(options);

        assertEquals(RedisOptions.LIST_DATA_STRUCTURE, config.dataStructure);
        assertEquals(RedisOptions.PROTOBUF_FORMAT, config.format);
        assertEquals("com.google.protobuf.StringValue", config.protobufMessageClassName);
    }

    @Test
    void rejectsProtobufWithStringDataStructure() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "redis://localhost:6379");
        options.put("data-structure", "string");
        options.put("format", "protobuf");
        options.put("protobuf.message-class-name", "com.google.protobuf.StringValue");

        assertThrows(IllegalArgumentException.class,
                () -> RedisOptions.getRedisConfig(options));
    }
}
