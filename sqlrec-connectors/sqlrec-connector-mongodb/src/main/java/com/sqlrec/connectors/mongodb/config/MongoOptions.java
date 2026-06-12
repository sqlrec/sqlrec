package com.sqlrec.connectors.mongodb.config;

import com.sqlrec.common.config.ConfigOption;

import java.util.Map;

public class MongoOptions {
    public static final String CONNECTOR_IDENTIFIER = "mongodb";

    // ==================== Basic Connection ====================

    public static final ConfigOption<String> URI = new ConfigOption<>(
            "uri",
            null,
            "MongoDB connection URI, e.g. mongodb://host:port",
            null,
            String.class
    );

    public static final ConfigOption<String> DATABASE = new ConfigOption<>(
            "database",
            null,
            "MongoDB database name",
            null,
            String.class
    );

    public static final ConfigOption<String> COLLECTION = new ConfigOption<>(
            "collection",
            null,
            "MongoDB collection name",
            null,
            String.class
    );

    // ==================== Cache ====================

    public static final ConfigOption<Integer> MAX_CACHE_SIZE = new ConfigOption<>(
            "max-cache-size",
            100000,
            "max cache size for local cache",
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

    public static MongoConfig getMongoConfig(Map<String, String> options) {
        MongoConfig config = new MongoConfig();
        config.uri = URI.getValue(options);
        config.database = DATABASE.getValue(options);
        config.collection = COLLECTION.getValue(options);

        config.maxCacheSize = MAX_CACHE_SIZE.getValue(options);
        config.cacheTtl = CACHE_TTL.getValue(options);

        return config;
    }
}
