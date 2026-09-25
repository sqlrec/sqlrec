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
    private static final Cache<String, SqlApi> CACHE = Caffeine.newBuilder()
            .expireAfterWrite(SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue(), TimeUnit.SECONDS)
            .build();

    private SqlApiCache() {
    }

    public static SqlApi get(String apiName) throws Exception {
        return get(CACHE, apiName);
    }

    static SqlApi get(Cache<String, SqlApi> cache, String apiName) throws Exception {
        String normalizedName = ResourceNames.normalize(apiName);
        SqlApi sqlApi = cache.getIfPresent(normalizedName);
        if (sqlApi == null) {
            sqlApi = MetadataAccessFactory.getInstance().getSqlApi(normalizedName);
            if (sqlApi != null) {
                cache.put(normalizedName, sqlApi);
            }
        }
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

    public static void invalidateAll() {
        CACHE.invalidateAll();
    }
}
