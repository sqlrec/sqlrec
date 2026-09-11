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
        return createRefreshCache(refreshInterval, (key, oldValue) -> loadFunction.load(key));
    }

    public static <K, V> LoadingCache<K, V> createRefreshCache(
            Duration refreshInterval,
            RefreshLoader<K, V> refreshLoader
    ) {
        return createRefreshCache(
                refreshInterval,
                ExecutorServiceUtils.getCacheRefreshExecutorService(),
                Ticker.systemTicker(),
                refreshLoader
        );
    }

    public static <K, V> LoadingCache<K, V> createRefreshCache(
            Duration refreshInterval,
            Executor executor,
            Ticker ticker,
            CacheLoader<K, V> loadFunction
    ) {
        return createRefreshCache(
                refreshInterval,
                executor,
                ticker,
                (key, oldValue) -> loadFunction.load(key)
        );
    }

    public static <K, V> LoadingCache<K, V> createRefreshCache(
            Duration refreshInterval,
            Executor executor,
            Ticker ticker,
            RefreshLoader<K, V> refreshLoader
    ) {
        return Caffeine.newBuilder()
                .refreshAfterWrite(refreshInterval)
                .executor(executor)
                .ticker(ticker)
                .build(new CacheLoader<K, V>() {
                    @Override
                    public V load(K key) throws Exception {
                        return refreshLoader.load(key, null);
                    }

                    @Override
                    public V reload(K key, V oldValue) {
                        try {
                            return refreshLoader.load(key, oldValue);
                        } catch (Exception e) {
                            log.warn("Failed to refresh cache entry: {}", key, e);
                            return oldValue;
                        }
                    }
                });
    }

    public static <V> SingleValueCache<V> createSingleValueRefreshCache(
            Duration refreshInterval,
            ValueLoader<V> valueLoader
    ) {
        return createSingleValueRefreshCache(
                refreshInterval,
                ExecutorServiceUtils.getCacheRefreshExecutorService(),
                Ticker.systemTicker(),
                valueLoader
        );
    }

    static <V> SingleValueCache<V> createSingleValueRefreshCache(
            Duration refreshInterval,
            Executor executor,
            Ticker ticker,
            ValueLoader<V> valueLoader
    ) {
        LoadingCache<SingleValueKey, V> cache = createRefreshCache(
                refreshInterval,
                executor,
                ticker,
                (key, oldValue) -> valueLoader.load(oldValue)
        );
        return new SingleValueCache<>() {
            @Override
            public V get() {
                return cache.get(SingleValueKey.INSTANCE);
            }

            @Override
            public void invalidate() {
                cache.invalidate(SingleValueKey.INSTANCE);
            }
        };
    }

    @FunctionalInterface
    public interface RefreshLoader<K, V> {
        V load(K key, V oldValue) throws Exception;
    }

    public interface SingleValueCache<V> {
        V get();

        void invalidate();
    }

    @FunctionalInterface
    public interface ValueLoader<V> {
        V load(V oldValue) throws Exception;
    }

    private enum SingleValueKey {
        INSTANCE
    }
}
