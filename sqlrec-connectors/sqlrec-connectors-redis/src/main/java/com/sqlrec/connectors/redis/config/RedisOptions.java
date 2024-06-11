package com.sqlrec.connectors.redis.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Map;

public class RedisOptions {
    public static final String CONNECTOR_IDENTIFIER = "redis";

    public static final String CLUSTER_MODE = "cluster";
    public static final String SINGLE_MODE = "single";

    public static final String STRING_DATA_STRUCTURE = "string";
    public static final String LIST_DATA_STRUCTURE = "list";

    public static final ConfigOption<String> URL = ConfigOptions.key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("url for lettuce client like redis://password@127.0.0.1:6379/0");

    public static final ConfigOption<String> REDIS_MODE = ConfigOptions.key("redis-mode")
            .stringType()
            .defaultValue(SINGLE_MODE)
            .withDescription("redis-mode for connect to redis");

    public static final ConfigOption<String> DATA_STRUCTURE = ConfigOptions.key("data-structure")
            .stringType()
            .defaultValue(STRING_DATA_STRUCTURE)
            .withDescription("data-structure for redis, can be string or list");

    public static final ConfigOption<Integer> TTL = ConfigOptions.key("ttl")
            .intType()
            .defaultValue(3600 * 24 * 30)
            .withDescription("ttl for redis key(seconds), default is 30 day");

    public static final ConfigOption<Integer> CACHE_TTL = ConfigOptions.key("cache-ttl")
            .intType()
            .defaultValue(0)
            .withDescription("ttl for local cache(seconds), 0 means no cache");

    public static RedisConfig getRedisConfig(Map<String, String> options) {
        RedisConfig redisConfig = new RedisConfig();
        // check url exists
        if (!options.containsKey(URL.key())) {
            throw new IllegalArgumentException("url is required");
        } else {
            redisConfig.url = options.get(URL.key());
        }

        // check redis mode
        if (!options.containsKey(REDIS_MODE.key())) {
            redisConfig.redisMode = SINGLE_MODE;
        } else {
            // check redis mode valid
            if (!SINGLE_MODE.equals(options.get(REDIS_MODE.key())) && !CLUSTER_MODE.equals(options.get(REDIS_MODE.key()))) {
                throw new IllegalArgumentException("redis mode must be single or cluster");
            }
            redisConfig.redisMode = options.get(REDIS_MODE.key());
        }

        // check data structure
        if (!options.containsKey(DATA_STRUCTURE.key())) {
            redisConfig.dataStructure = STRING_DATA_STRUCTURE;
        } else {
            // check data structure valid
            if (!STRING_DATA_STRUCTURE.equals(options.get(DATA_STRUCTURE.key())) && !LIST_DATA_STRUCTURE.equals(options.get(DATA_STRUCTURE.key()))) {
                throw new IllegalArgumentException("data structure must be string or list");
            }
            redisConfig.dataStructure = options.get(DATA_STRUCTURE.key());
        }

        // check ttl
        if (!options.containsKey(TTL.key())) {
            redisConfig.ttl = 3600 * 24 * 30;
        } else {
            redisConfig.ttl = Integer.parseInt(options.get(TTL.key()));
        }

        // check cache ttl
        if (!options.containsKey(CACHE_TTL.key())) {
            redisConfig.cacheTtl = 0;
        } else {
            redisConfig.cacheTtl = Integer.parseInt(options.get(CACHE_TTL.key()));
        }

        return redisConfig;
    }
}
