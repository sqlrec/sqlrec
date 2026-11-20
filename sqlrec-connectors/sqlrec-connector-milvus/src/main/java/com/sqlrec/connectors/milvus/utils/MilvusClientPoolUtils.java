package com.sqlrec.connectors.milvus.utils;

import com.sqlrec.connectors.milvus.config.MilvusConfig;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Shared Milvus client pools and their lifecycle. */
public final class MilvusClientPoolUtils {
    private static final Logger logger = LoggerFactory.getLogger(MilvusClientPoolUtils.class);
    private static final Map<String, MilvusClientV2Pool> clientPools = new HashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(MilvusClientPoolUtils::closeAll, "MilvusHandler-shutdown"));
    }

    private MilvusClientPoolUtils() {
    }

    public static <T> T withClient(MilvusConfig config, Function<MilvusClientV2, T> action) {
        String key = config.url + "|" + config.token;
        for (int attempt = 0; ; attempt++) {
            MilvusClientV2Pool pool = getOrCreatePool(key, config);
            MilvusClientV2 client = null;
            try {
                client = pool.getClient(key);
                if (client == null) {
                    throw new IllegalStateException("Failed to get Milvus client from pool after timeout");
                }
                return action.apply(client);
            } catch (RuntimeException e) {
                boolean poolRetiredDuringBorrow = client == null && !isCurrentPool(key, pool);
                if (attempt != 0 || (!isConnectionFailure(e) && !poolRetiredDuringBorrow)) {
                    throw e;
                }
                logger.warn("Retrying Milvus operation with a fresh or replacement pool: {}", e.getMessage());
                invalidatePool(key, pool);
            } finally {
                if (client != null) {
                    returnClient(key, pool, client);
                }
            }
        }
    }

    private static void invalidatePool(String key, MilvusClientV2Pool pool) {
        synchronized (MilvusClientPoolUtils.class) {
            if (!clientPools.remove(key, pool)) {
                return;
            }
        }
        closePool(pool);
    }

    private static void closePool(MilvusClientV2Pool pool) {
        try {
            pool.close();
        } catch (Exception e) {
            logger.warn("Failed to close Milvus client pool: {}", e.getMessage());
        }
    }

    /**
     * Close every cached client pool and clear the cache. Registered as a JVM shutdown
     * hook so pooled gRPC channels are released on exit.
     */
    public static void closeAll() {
        List<MilvusClientV2Pool> pools;
        synchronized (MilvusClientPoolUtils.class) {
            pools = new ArrayList<>(clientPools.values());
            clientPools.clear();
        }
        pools.forEach(MilvusClientPoolUtils::closePool);
        logger.info("Closed all Milvus client pools on shutdown");
    }

    private static void returnClient(String key, MilvusClientV2Pool pool, MilvusClientV2 client) {
        if (isCurrentPool(key, pool)) {
            try {
                pool.returnClient(key, client);
                return;
            } catch (RuntimeException e) {
                if (isCurrentPool(key, pool)) {
                    logger.warn("Failed to return Milvus client to pool: {}", e.getMessage());
                    return;
                }
            }
        }
        try {
            client.close();
        } catch (RuntimeException e) {
            logger.warn("Failed to close retired Milvus client: {}", e.getMessage());
        }
    }

    private static boolean isConnectionFailure(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof StatusRuntimeException) {
                Status.Code code = ((StatusRuntimeException) cause).getStatus().getCode();
                return code == Status.Code.UNAVAILABLE
                        || code == Status.Code.DEADLINE_EXCEEDED
                        || code == Status.Code.UNKNOWN;
            }
        }
        return false;
    }

    private static synchronized boolean isCurrentPool(String key, MilvusClientV2Pool pool) {
        return clientPools.get(key) == pool;
    }

    private static synchronized MilvusClientV2Pool getOrCreatePool(String key, MilvusConfig milvusConfig) {
        return clientPools.computeIfAbsent(key, ignored -> openClientPool(milvusConfig));
    }

    private static MilvusClientV2Pool openClientPool(MilvusConfig milvusConfig) {
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
            return new MilvusClientV2Pool(poolConfig, connectConfig);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to create Milvus client pool", e);
        }
    }
}
