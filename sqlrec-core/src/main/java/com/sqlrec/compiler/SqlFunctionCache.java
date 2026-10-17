package com.sqlrec.compiler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.utils.CacheUtils;
import com.sqlrec.utils.ExecutorServiceUtils;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Creates and owns the process-wide SQL-function cache.
 */
public final class SqlFunctionCache {
    private static final Logger log = LoggerFactory.getLogger(SqlFunctionCache.class);
    private static final Cache<String, SqlFunctionBindable> CACHE = createCache();

    private SqlFunctionCache() {
    }

    static Cache<String, SqlFunctionBindable> getCache() {
        return CACHE;
    }

    public static void invalidateAll() {
        CACHE.invalidateAll();
    }

    private static Cache<String, SqlFunctionBindable> createCache() {
        return createCache(
                SqlRecConfigs.isFileSystemMetadata(),
                SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue(),
                TimeUnit.SECONDS,
                ExecutorServiceUtils.getCacheRefreshExecutorService(),
                Ticker.systemTicker()
        );
    }

    static Cache<String, SqlFunctionBindable> createCache(
            boolean fileSystemMeta,
            long refreshInterval,
            TimeUnit timeUnit,
            Executor executor,
            Ticker ticker
    ) {
        if (fileSystemMeta) {
            return Caffeine.newBuilder().build();
        }

        return CacheUtils.createRefreshCache(
                Duration.of(refreshInterval, timeUnit.toChronoUnit()),
                executor,
                ticker,
                SqlFunctionCache::refreshFunction
        );
    }

    private static SqlFunctionBindable refreshFunction(String functionName) throws Exception {
        long startTime = System.nanoTime();
        String status = "success";
        try {
            Cache<String, SqlFunctionBindable> temporaryCache = Caffeine.newBuilder().build();
            SqlFunctionBindable bindable =
                    new CompileManager(temporaryCache).getSqlFunction(functionName);
            log.info("SQL function cache refreshed: {}", functionName);
            return bindable;
        } catch (Exception | Error e) {
            status = "error";
            throw e;
        } finally {
            long duration = System.nanoTime() - startTime;
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_FUNCTION_UPDATE_DURATION, Tags.of("outcome", status))
                    .record(duration, TimeUnit.NANOSECONDS);
        }
    }

}
