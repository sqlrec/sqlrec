package com.sqlrec.connectors.redis.config;

import com.sqlrec.common.config.ConfigOption;

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
            "url for lettuce client like redis://password@127.0.0.1:6379/0"
    );

    public static final ConfigOption<String> REDIS_MODE = new ConfigOption<>(
            "redis-mode",
            SINGLE_MODE,
            "redis-mode for connect to redis"
    );

    public static final ConfigOption<String> DATA_STRUCTURE = new ConfigOption<>(
            "data-structure",
            STRING_DATA_STRUCTURE,
            "data-structure for redis, can be string or list"
    );

    public static final ConfigOption<Integer> TTL = new ConfigOption<>(
            "ttl",
            3600 * 24 * 30,
            "ttl for redis key(seconds), default is 30 day"
    );

    public static final ConfigOption<Integer> CACHE_TTL = new ConfigOption<>(
            "cache-ttl",
            0,
            "ttl for local cache(seconds), 0 means no cache"
    );

    public static RedisConfig getRedisConfig(Map<String, String> options) {
        RedisConfig redisConfig = new RedisConfig();
        // check url exists
        if (!options.containsKey(URL.getKey())) {
            throw new IllegalArgumentException("url is required");
        } else {
            redisConfig.url = options.get(URL.getKey());
        }

        // check redis mode
        if (!options.containsKey(REDIS_MODE.getKey())) {
            redisConfig.redisMode = SINGLE_MODE;
        } else {
            // check redis mode valid
            if (!SINGLE_MODE.equals(options.get(REDIS_MODE.getKey())) && !CLUSTER_MODE.equals(options.get(REDIS_MODE.getKey()))) {
                throw new IllegalArgumentException("redis mode must be single or cluster");
            }
            redisConfig.redisMode = options.get(REDIS_MODE.getKey());
        }

        // check data structure
        if (!options.containsKey(DATA_STRUCTURE.getKey())) {
            redisConfig.dataStructure = STRING_DATA_STRUCTURE;
        } else {
            // check data structure valid
            if (!STRING_DATA_STRUCTURE.equals(options.get(DATA_STRUCTURE.getKey())) && !LIST_DATA_STRUCTURE.equals(options.get(DATA_STRUCTURE.getKey()))) {
                throw new IllegalArgumentException("data structure must be string or list");
            }
            redisConfig.dataStructure = options.get(DATA_STRUCTURE.getKey());
        }

        // check ttl
        if (!options.containsKey(TTL.getKey())) {
            redisConfig.ttl = 3600 * 24 * 30;
        } else {
            redisConfig.ttl = Integer.parseInt(options.get(TTL.getKey()));
        }

        // check cache ttl
        if (!options.containsKey(CACHE_TTL.getKey())) {
            redisConfig.cacheTtl = 0;
        } else {
            redisConfig.cacheTtl = Integer.parseInt(options.get(CACHE_TTL.getKey()));
        }

        return redisConfig;
    }
}
