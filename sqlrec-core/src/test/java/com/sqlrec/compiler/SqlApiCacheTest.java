package com.sqlrec.compiler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Ticker;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.LocalHdfsAccess;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.entity.SqlApi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlApiCacheTest {
    private MetadataAccess savedMetadataAccess;
    private MetadataAccess metadataAccess;

    @BeforeEach
    void setUp() throws Exception {
        Field field = getMetadataAccessField();
        savedMetadataAccess = (MetadataAccess) field.get(null);
        metadataAccess = new MetadataAccess(
                new SqlFileSchemaAccess(new ArrayList<>(), new ArrayList<>()),
                new InMemoryStoreAccess(new ArrayList<>(), new ArrayList<>(), new ArrayList<>()),
                new LocalHdfsAccess()
        );
        field.set(null, metadataAccess);
    }

    @AfterEach
    void tearDown() throws Exception {
        getMetadataAccessField().set(null, savedMetadataAccess);
    }

    @Test
    void normalizesNamesCachesValuesAndReloadsAfterExpiration() throws Exception {
        ManualTicker ticker = new ManualTicker();
        Cache<String, SqlApi> cache = SqlApiCache.createCache(1, TimeUnit.SECONDS, ticker);
        SqlApi original = putApi("mixed_api", "function_v1");

        assertSame(original, SqlApiCache.get(cache, "MiXeD_ApI"));
        putApi("mixed_api", "function_v2");
        assertSame(original, SqlApiCache.get(cache, "mixed_api"));

        ticker.advance(Duration.ofSeconds(2));
        SqlApi refreshed = SqlApiCache.get(cache, "MIXED_API");
        assertEquals("function_v2", refreshed.getFunctionName());
    }

    @Test
    void invalidateForcesReloadAndMissingApiIsNotCached() throws Exception {
        ManualTicker ticker = new ManualTicker();
        Cache<String, SqlApi> cache = SqlApiCache.createCache(1, TimeUnit.DAYS, ticker);
        SqlApi original = putApi("api", "function_v1");
        assertSame(original, SqlApiCache.get(cache, "api"));

        putApi("api", "function_v2");
        cache.invalidateAll();
        assertEquals("function_v2", SqlApiCache.get(cache, "api").getFunctionName());

        IllegalArgumentException missing = assertThrows(
                IllegalArgumentException.class, () -> SqlApiCache.get(cache, "missing"));
        assertEquals("API not found: missing", missing.getMessage());
        putApi("missing", "available_now");
        assertEquals("available_now", SqlApiCache.get(cache, "missing").getFunctionName());
    }

    private SqlApi putApi(String name, String functionName) {
        SqlApi api = new SqlApi();
        api.setName(name);
        api.setFunctionName(functionName);
        api.setCreatedAt(System.currentTimeMillis());
        api.setUpdatedAt(System.currentTimeMillis());
        metadataAccess.upsertSqlApi(api);
        return api;
    }

    private static Field getMetadataAccessField() throws Exception {
        Field field = MetadataAccessFactory.class.getDeclaredField("instance");
        field.setAccessible(true);
        return field;
    }

    private static final class ManualTicker implements Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        void advance(Duration duration) {
            nanos.addAndGet(duration.toNanos());
        }
    }
}
