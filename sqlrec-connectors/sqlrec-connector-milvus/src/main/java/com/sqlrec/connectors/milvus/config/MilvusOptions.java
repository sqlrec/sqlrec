package com.sqlrec.connectors.milvus.config;

import com.sqlrec.common.config.ConfigOption;

import java.util.Map;

public class MilvusOptions {
    public static final String CONNECTOR_IDENTIFIER = "milvus";

    public static final ConfigOption<String> URL = new ConfigOption<>(
            "url",
            null,
            "Milvus server URL",
            null,
            String.class
    );
    public static final ConfigOption<String> TOKEN = new ConfigOption<>(
            "token",
            null,
            "Milvus server token",
            null,
            String.class
    );
    public static final ConfigOption<String> DATABASE = new ConfigOption<>(
            "database",
            "default",
            "Milvus database name",
            null,
            String.class
    );
    public static final ConfigOption<String> COLLECTION = new ConfigOption<>(
            "collection",
            null,
            "Milvus collection name",
            null,
            String.class
    );

    public static final ConfigOption<Integer> BATCH_SIZE = new ConfigOption<>(
            "batch-size",
            1024,
            "Batch size for bulk insert operations",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> POOL_MAX_IDLE_PER_KEY = new ConfigOption<>(
            "pool.max-idle-per-key",
            10,
            "Maximum number of idle connections per key in the connection pool",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> POOL_MAX_TOTAL_PER_KEY = new ConfigOption<>(
            "pool.max-total-per-key",
            100,
            "Maximum number of total connections per key in the connection pool",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> POOL_MAX_TOTAL = new ConfigOption<>(
            "pool.max-total",
            100,
            "Maximum total number of connections in the connection pool",
            null,
            Integer.class
    );

    public static final ConfigOption<Long> POOL_MAX_BLOCK_WAIT_DURATION = new ConfigOption<>(
            "pool.max-block-wait-duration",
            5L,
            "Maximum block wait duration in seconds when getting connection from pool",
            null,
            Long.class
    );

    public static final ConfigOption<Long> POOL_MIN_EVICTABLE_IDLE_DURATION = new ConfigOption<>(
            "pool.min-evictable-idle-duration",
            10L,
            "Minimum evictable idle duration in seconds for connections in the pool",
            null,
            Long.class
    );

    public static final ConfigOption<Long> FLUSH_INTERVAL = new ConfigOption<>(
            "flush-interval",
            5L,
            "Flush interval in seconds for batch operations. Data will be flushed when buffer is full or this interval is reached",
            null,
            Long.class
    );

    public static MilvusConfig getMilvusConfig(Map<String, String> options) {
        MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.url = URL.getValue(options);
        milvusConfig.token = TOKEN.getValue(options);
        milvusConfig.database = DATABASE.getValue(options);
        milvusConfig.collection = COLLECTION.getValue(options);
        milvusConfig.batchSize = BATCH_SIZE.getValue(options);
        milvusConfig.poolMaxIdlePerKey = POOL_MAX_IDLE_PER_KEY.getValue(options);
        milvusConfig.poolMaxTotalPerKey = POOL_MAX_TOTAL_PER_KEY.getValue(options);
        milvusConfig.poolMaxTotal = POOL_MAX_TOTAL.getValue(options);
        milvusConfig.poolMaxBlockWaitDuration = POOL_MAX_BLOCK_WAIT_DURATION.getValue(options);
        milvusConfig.poolMinEvictableIdleDuration = POOL_MIN_EVICTABLE_IDLE_DURATION.getValue(options);
        milvusConfig.flushInterval = FLUSH_INTERVAL.getValue(options);

        return milvusConfig;
    }
}
