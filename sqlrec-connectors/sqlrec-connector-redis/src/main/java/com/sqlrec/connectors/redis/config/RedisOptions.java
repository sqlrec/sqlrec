package com.sqlrec.connectors.redis.config;

import com.sqlrec.common.config.ConfigOption;

import java.util.Arrays;
import java.util.Map;

public class RedisOptions {
    public static final String CONNECTOR_IDENTIFIER = "redis";

    public static final String CLUSTER_MODE = "cluster";
    public static final String SINGLE_MODE = "single";

    public static final String STRING_DATA_STRUCTURE = "string";
    public static final String LIST_DATA_STRUCTURE = "list";

    public static final ConfigOption<String> URL = new ConfigOption<>(
            "url",
            null,
            "url for lettuce client like redis://password@127.0.0.1:6379/0",
            null,
            String.class
    );

    public static final ConfigOption<String> REDIS_MODE = new ConfigOption<>(
            "redis-mode",
            SINGLE_MODE,
            "redis-mode for connect to redis",
            Arrays.asList(CLUSTER_MODE, SINGLE_MODE),
            String.class
    );

    public static final ConfigOption<String> DATA_STRUCTURE = new ConfigOption<>(
            "data-structure",
            STRING_DATA_STRUCTURE,
            "data-structure for redis, can be string or list",
            Arrays.asList(STRING_DATA_STRUCTURE, LIST_DATA_STRUCTURE),
            String.class
    );

    public static final ConfigOption<Integer> TTL = new ConfigOption<>(
            "ttl",
            3600 * 24 * 30,
            "ttl for redis key(seconds), default is 30 day",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> CACHE_TTL = new ConfigOption<>(
            "cache-ttl",
            30,
            "ttl for local cache(seconds), 0 means no cache",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> MAX_CACHE_SIZE = new ConfigOption<>(
            "max-cache-size",
            100000,
            "max cache size for local cache",
            null,
            Integer.class
    );

    public static RedisConfig getRedisConfig(Map<String, String> options) {
        RedisConfig redisConfig = new RedisConfig();
        redisConfig.url = URL.getValue(options);
        redisConfig.redisMode = REDIS_MODE.getValue(options);
        redisConfig.dataStructure = DATA_STRUCTURE.getValue(options);
        redisConfig.ttl = TTL.getValue(options);
        redisConfig.cacheTtl = CACHE_TTL.getValue(options);
        redisConfig.maxCacheSize = MAX_CACHE_SIZE.getValue(options);

        return redisConfig;
    }
}
