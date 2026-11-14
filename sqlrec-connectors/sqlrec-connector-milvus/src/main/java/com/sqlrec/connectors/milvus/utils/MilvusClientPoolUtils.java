package com.sqlrec.connectors.milvus.utils;

import com.sqlrec.connectors.milvus.config.MilvusConfig;
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Shared Milvus client pools and their lifecycle. */
public final class MilvusClientPoolUtils {
    private static final Logger logger = LoggerFactory.getLogger(MilvusClientPoolUtils.class);
    private static final Map<String, MilvusClientV2Pool> clientPools = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(MilvusClientPoolUtils::closeAll, "MilvusHandler-shutdown"));
    }

    private MilvusClientPoolUtils() {
    }

    /**
     * Close and discard the cached client pool for {@code key} so the next call builds a
     * fresh pool. Called after a transport-level failure (Milvus restart, network blip)
     * so subsequent calls recover instead of reusing broken pooled clients.
     */
    public static synchronized void invalidatePool(String key) {
        MilvusClientV2Pool pool = clientPools.remove(key);
        if (pool != null) {
            try {
                pool.close();
            } catch (Exception e) {
                // The cache key includes the authentication token; never log it.
                logger.warn("Failed to close Milvus client pool during invalidation: {}", e.getMessage());
            }
        }
    }

    /**
     * Close every cached client pool and clear the cache. Registered as a JVM shutdown
     * hook so pooled gRPC channels are released on exit.
     */
    public static synchronized void closeAll() {
        for (Map.Entry<String, MilvusClientV2Pool> entry : new ArrayList<>(clientPools.entrySet())) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                logger.warn("Failed to close Milvus client pool on shutdown: {}", e.getMessage());
            }
        }
        clientPools.clear();
        logger.info("Closed all Milvus client pools on shutdown");
    }

    public static void returnClient(MilvusClientV2 client, MilvusConfig milvusConfig) {
        String key = poolKey(milvusConfig);
        if (clientPools.containsKey(key)) {
            clientPools.get(key).returnClient(key, client);
        } else {
            logger.warn("Milvus client pool is not found; closing the returned client");
            client.close();
        }
    }

    public static MilvusClientV2 getClient(MilvusConfig milvusConfig) {
        String key = poolKey(milvusConfig);
        if (!clientPools.containsKey(key)) {
            openClientPool(key, milvusConfig);
        }
        MilvusClientV2Pool pool = clientPools.get(key);
        if (pool == null) {
            // Pool was invalidated concurrently between containsKey and get (e.g. by a
            // transport-failure retry); reopen so this call does not NPE.
            openClientPool(key, milvusConfig);
            pool = clientPools.get(key);
        }
        return pool.getClient(key);
    }

    public static String poolKey(MilvusConfig milvusConfig) {
        return milvusConfig.url + "|" + milvusConfig.token;
    }

    private static synchronized void openClientPool(String key, MilvusConfig milvusConfig) {
        if (clientPools.containsKey(key)) {
            return;
        }

        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(milvusConfig.url)
                .token(milvusConfig.token)
                .rpcDeadlineMs(milvusConfig.rpcDeadlineMs)
                .build();
        PoolConfig poolConfig = PoolConfig.builder()
                .maxIdlePerKey(milvusConfig.poolMaxIdlePerKey)
                .maxTotalPerKey(milvusConfig.poolMaxTotalPerKey)
                .maxTotal(milvusConfig.poolMaxTotal)
                .maxBlockWaitDuration(Duration.ofSeconds(milvusConfig.poolMaxBlockWaitDuration))
                .minEvictableIdleDuration(Duration.ofSeconds(milvusConfig.poolMinEvictableIdleDuration))
                .build();

        try {
            MilvusClientV2Pool pool = new MilvusClientV2Pool(poolConfig, connectConfig);
            clientPools.put(key, pool);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
