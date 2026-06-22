package com.sqlrec.udf;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.udf.table.GetGrowthbookFeaturesFunction;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class GetGrowthbookFeaturesFunctionTest {
    private static final String API_HOST = "http://192.168.1.5:30284";
    private static final String CLIENT_KEY = "sdk-TXRZAkqm6avFjR";
    private static final String FEATURE_KEY = "test";

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, ?> getClientCache() throws Exception {
        Field field = GetGrowthbookFeaturesFunction.class.getDeclaredField("CLIENT_CACHE");
        field.setAccessible(true);
        return (ConcurrentHashMap<String, ?>) field.get(null);
    }

    private CacheTable createUserTable(List<Object[]> rows) {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("country", 1, SqlTypeName.VARCHAR));
        return new CacheTable("users", Linq4j.asEnumerable(rows), fields);
    }

    private ExecuteContext createContext() {
        ExecuteContextImpl ctx = new ExecuteContextImpl();
        ctx.setVariable(Consts.LOG_ID, null);
        return ctx;
    }

//    @Test
    public void testClientCacheReusesSameInstance() throws Exception {
        GetGrowthbookFeaturesFunction func = new GetGrowthbookFeaturesFunction();
        CacheTable usertable = createUserTable(Collections.singletonList(new Object[]{"user1", "US"}));

        func.evaluate(createContext(), API_HOST, CLIENT_KEY, usertable, FEATURE_KEY);
        func.evaluate(createContext(), API_HOST, CLIENT_KEY, usertable, FEATURE_KEY);

        ConcurrentHashMap<String, ?> cache = getClientCache();
        assertEquals(1, cache.size(), "Same apiHost+clientKey should reuse the same client instance");
        assertTrue(cache.containsKey(API_HOST + "|" + CLIENT_KEY));
    }

//    @Test
    public void testConcurrentAccessSameKey() throws Exception {
        int threadCount = 16;
        int iterationsPerThread = 10;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threadCount);

        GetGrowthbookFeaturesFunction func = new GetGrowthbookFeaturesFunction();

        for (int t = 0; t < threadCount; t++) {
            final int threadIndex = t;
            executor.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        ExecuteContext ctx = createContext();
                        CacheTable usertable = createUserTable(
                                Collections.singletonList(new Object[]{"user_t" + threadIndex + "_i" + i, "US"}));
                        CacheTable result = func.evaluate(ctx, API_HOST, CLIENT_KEY, usertable, FEATURE_KEY);
                        assertNotNull(result);
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS), "All threads should finish within timeout");
        executor.shutdown();

        assertEquals(threadCount * iterationsPerThread, successCount.get(), "All evaluations should succeed");
        assertEquals(0, errorCount.get(), "No errors should occur");

        ConcurrentHashMap<String, ?> cache = getClientCache();
        assertEquals(1, cache.size(), "Same key should create only one client instance");
    }

//    @Test
    public void testCacheRefreshByPolling() throws Exception {
        GetGrowthbookFeaturesFunction func = new GetGrowthbookFeaturesFunction();
        int durationSeconds = 120;
        int intervalMs = 1000;

        for (int i = 0; i < durationSeconds; i++) {
            ExecuteContext ctx = createContext();
            CacheTable usertable = createUserTable(Collections.singletonList(new Object[]{"user_poll_" + i, "US"}));
            CacheTable result = func.evaluate(ctx, API_HOST, CLIENT_KEY, usertable, FEATURE_KEY);

            String featureValue = ctx.getVariable(FEATURE_KEY);
            System.out.printf("[%ds] featureKey=%s, value=%s%n", i, FEATURE_KEY, featureValue);

            if (result != null) {
                result.scan(null).forEach(row -> System.out.printf("  tracking: experiment_id=%s, variation_id=%s, user_id=%s%n",
                        row[0], row[1], row[2]));
            }

            if (i < durationSeconds - 1) {
                Thread.sleep(intervalMs);
            }
        }
    }
}
