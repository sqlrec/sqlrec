package com.sqlrec.compiler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.LocalHdfsAccess;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlFunctionCacheTest {
    private MetadataAccess savedMetadataAccess;
    private MetadataAccess metadataAccess;

    @BeforeEach
    void setUp() throws Exception {
        savedMetadataAccess = (MetadataAccess) getMetadataAccessField().get(null);
        metadataAccess = new MetadataAccess(
                new SqlFileSchemaAccess(new ArrayList<>(), new ArrayList<>()),
                new InMemoryStoreAccess(new ArrayList<>(), new ArrayList<>(), new ArrayList<>()),
                new LocalHdfsAccess()
        );
        getMetadataAccessField().set(null, metadataAccess);

        CalciteSchema globalSchema = CalciteSchema.createRootSchema(false);
        globalSchema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.emptyMap();
            }
        });
        CalciteSchemaFactory.setGlobalSchema(globalSchema);
        JavaFunctionUtils.setSkipHmsQuery(true);
    }

    @AfterEach
    void tearDown() throws Exception {
        CalciteSchemaFactory.setGlobalSchema(null);
        JavaFunctionUtils.setSkipHmsQuery(false);
        getMetadataAccessField().set(null, savedMetadataAccess);
    }

    @Test
    void refreshReturnsCurrentValueThenReplacesItAndRecordsMetrics() throws Exception {
        putFunction("refresh_me", 1);
        ManualTicker ticker = new ManualTicker();
        QueuedExecutor executor = new QueuedExecutor();
        Cache<String, SqlFunctionBindable> cache = newRefreshCache(ticker, executor);
        CompileManager manager = new CompileManager(cache);
        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(metrics);
        try {
            SqlFunctionBindable original = manager.getSqlFunction("refresh_me");
            executor.runAll();
            putFunction("refresh_me", 2);
            ticker.advance(Duration.ofMillis(1100));

            assertSame(original, manager.getSqlFunction("REFRESH_ME"));
            assertEquals(1, executor.size());

            executor.runAll();
            SqlFunctionBindable refreshed = manager.getSqlFunction("refresh_me");
            assertNotSame(original, refreshed);
            assertEquals(2, executeScalar(refreshed));

            Counter success = metrics.find(Consts.METRICS_FUNCTION_UPDATE_COUNT)
                    .tag("result", "success")
                    .counter();
            Timer duration = metrics.find(Consts.METRICS_FUNCTION_UPDATE_DURATION)
                    .tag("status", "success")
                    .timer();
            assertEquals(1.0, success.count());
            assertEquals(1, duration.count());
        } finally {
            MetricsUtils.getCompositeMeterRegistry().remove(metrics);
        }
    }

    @Test
    void refreshCompilesDependenciesInAnIsolatedTemporaryCache() throws Exception {
        putFunction("child", 1);
        putFunction("parent", List.of(
                "create sql function parent",
                "cache table output as call child()",
                "return output"
        ));
        ManualTicker ticker = new ManualTicker();
        QueuedExecutor executor = new QueuedExecutor();
        Cache<String, SqlFunctionBindable> cache = newRefreshCache(ticker, executor);
        CompileManager manager = new CompileManager(cache);

        SqlFunctionBindable originalParent = manager.getSqlFunction("parent");
        executor.runAll();
        assertEquals(1, executeScalar(originalParent));
        putFunction("child", 2);
        ticker.advance(Duration.ofMillis(1100));

        assertSame(originalParent, manager.getSqlFunction("parent"));
        executor.runAll();

        SqlFunctionBindable refreshedParent = manager.getSqlFunction("parent");
        assertNotSame(originalParent, refreshedParent);
        assertEquals(2, executeScalar(refreshedParent));
    }

    @Test
    void failedRefreshKeepsOldValueAndRecordsFailure() throws Exception {
        putFunction("deleted", 1);
        ManualTicker ticker = new ManualTicker();
        QueuedExecutor executor = new QueuedExecutor();
        Cache<String, SqlFunctionBindable> cache = newRefreshCache(ticker, executor);
        CompileManager manager = new CompileManager(cache);
        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(metrics);
        try {
            SqlFunctionBindable original = manager.getSqlFunction("deleted");
            executor.runAll();
            metadataAccess.deleteSqlFunction("deleted");
            ticker.advance(Duration.ofMillis(1100));

            assertSame(original, manager.getSqlFunction("deleted"));
            executor.runAll();
            assertSame(original, cache.getIfPresent("deleted"));

            Counter failed = metrics.find(Consts.METRICS_FUNCTION_UPDATE_COUNT)
                    .tag("result", "failed")
                    .counter();
            Timer duration = metrics.find(Consts.METRICS_FUNCTION_UPDATE_DURATION)
                    .tag("status", "error")
                    .timer();
            assertEquals(1.0, failed.count());
            assertEquals(1, duration.count());
        } finally {
            MetricsUtils.getCompositeMeterRegistry().remove(metrics);
        }
    }

    @Test
    void hardExpirationRemovesInactiveEntry() throws Exception {
        putFunction("expired", 1);
        ManualTicker ticker = new ManualTicker();
        Cache<String, SqlFunctionBindable> cache = SqlFunctionCache.createCache(
                false, 1, TimeUnit.SECONDS, Runnable::run, ticker);
        CompileManager manager = new CompileManager(cache);

        manager.getSqlFunction("expired");
        metadataAccess.deleteSqlFunction("expired");
        ticker.advance(Duration.ofSeconds(3));
        cache.cleanUp();

        assertNull(cache.getIfPresent("expired"));
        assertThrows(Exception.class, () -> manager.getSqlFunction("expired"));
    }

    @Test
    void fileSystemMetadataUsesNonExpiringNonLoadingCache() {
        ManualTicker ticker = new ManualTicker();
        Cache<String, SqlFunctionBindable> cache = SqlFunctionCache.createCache(
                true, 1, TimeUnit.NANOSECONDS, Runnable::run, ticker);
        SqlFunctionBindable bindable = new SqlFunctionBindable(
                new ArrayList<>(), new ArrayList<>(), new ArrayList<>());

        cache.put("function", bindable);
        ticker.advance(Duration.ofDays(365));
        cache.cleanUp();

        assertFalse(cache instanceof LoadingCache);
        assertSame(bindable, cache.getIfPresent("function"));
    }

    private Cache<String, SqlFunctionBindable> newRefreshCache(
            ManualTicker ticker,
            Executor executor
    ) {
        Cache<String, SqlFunctionBindable> cache = SqlFunctionCache.createCache(
                false, 1, TimeUnit.SECONDS, executor, ticker);
        assertInstanceOf(LoadingCache.class, cache);
        return cache;
    }

    private void putFunction(String name, int value) {
        putFunction(name, List.of(
                "create sql function " + name,
                "cache table output as select " + value + " as a",
                "return output"
        ));
    }

    private void putFunction(String name, List<String> sqlList) {
        SqlFunction function = new SqlFunction();
        function.setName(name);
        function.setSqlList(JsonUtils.toJson(sqlList));
        function.setCreatedAt(System.currentTimeMillis());
        function.setUpdatedAt(System.currentTimeMillis());
        metadataAccess.upsertSqlFunction(function);
    }

    private int executeScalar(SqlFunctionBindable bindable) {
        Enumerable<Object[]> result = bindable.bind(
                CalciteSchemaFactory.createCalciteSchema(),
                new ExecuteContextImpl()
        );
        return ((Number) result.toList().getFirst()[0]).intValue();
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

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        int size() {
            return tasks.size();
        }

        void runAll() {
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
            }
        }
    }
}
