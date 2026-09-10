package com.sqlrec.utils;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executor;

public final class CacheUtils {
    private static final Logger log = LoggerFactory.getLogger(CacheUtils.class);

    private CacheUtils() {
    }

    public static <K, V> LoadingCache<K, V> createRefreshCache(
            Duration refreshInterval,
            CacheLoader<K, V> loadFunction
    ) {
        return createRefreshCache(
                refreshInterval,
                ExecutorServiceUtils.getCacheRefreshExecutorService(),
                Ticker.systemTicker(),
                loadFunction
        );
    }

    public static <K, V> LoadingCache<K, V> createRefreshCache(
            Duration refreshInterval,
            Executor executor,
            Ticker ticker,
            CacheLoader<K, V> loadFunction
    ) {
        return Caffeine.newBuilder()
                .refreshAfterWrite(refreshInterval)
                .executor(executor)
                .ticker(ticker)
                .build(new CacheLoader<K, V>() {
                    @Override
                    public V load(K key) throws Exception {
                        return loadFunction.load(key);
                    }

                    @Override
                    public V reload(K key, V oldValue) {
                        try {
                            return loadFunction.load(key);
                        } catch (Exception e) {
                            log.warn("Failed to refresh cache entry: {}", key, e);
                            return oldValue;
                        }
                    }
                });
    }
}
