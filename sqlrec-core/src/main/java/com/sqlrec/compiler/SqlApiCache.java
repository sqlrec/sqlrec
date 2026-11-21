package com.sqlrec.compiler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.SqlApi;

import java.util.concurrent.TimeUnit;

/** Process-wide cache of SQL API metadata. */
public final class SqlApiCache {
    private static final Cache<String, SqlApi> CACHE = createCache(
            SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue(), TimeUnit.SECONDS, Ticker.systemTicker());

    private SqlApiCache() {
    }

    public static SqlApi get(String apiName) {
        return get(CACHE, apiName);
    }

    static SqlApi get(Cache<String, SqlApi> cache, String apiName) {
        String normalizedName = ResourceNames.normalize(apiName);
        SqlApi sqlApi = cache.get(normalizedName, key -> MetadataAccessFactory.getInstance().getSqlApi(key));
        if (sqlApi == null) {
            throw new IllegalArgumentException("API not found: " + normalizedName);
        }
        return sqlApi;
    }

    static Cache<String, SqlApi> createCache(long duration, TimeUnit timeUnit, Ticker ticker) {
        return Caffeine.newBuilder()
                .expireAfterWrite(duration, timeUnit)
                .ticker(ticker)
                .build();
    }

    public static void invalidate(String apiName) {
        CACHE.invalidate(ResourceNames.normalize(apiName));
    }

    public static void invalidateAll() {
        CACHE.invalidateAll();
    }
}
