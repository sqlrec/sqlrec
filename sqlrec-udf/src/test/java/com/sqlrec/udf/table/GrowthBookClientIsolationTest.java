package com.sqlrec.udf.table;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sun.net.httpserver.HttpServer;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GrowthBookClientIsolationTest {
    private HttpServer server;
    private String host;
    private final ConcurrentHashMap<String, AtomicInteger> requests = new ConcurrentHashMap<>();
    private final GetGrowthbookFeaturesFunction function = new GetGrowthbookFeaturesFunction();

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/features/", exchange -> {
            String key = exchange.getRequestURI().getPath().substring("/api/features/".length());
            int count = requests.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
            boolean fail = key.equals("retry") && count == 1;
            String body = fail ? "{\"error\":\"temporarily unavailable\"}"
                    : "{\"features\":{\"flag\":{\"defaultValue\":\"fallback\",\"rules\":[{"
                    + "\"key\":\"" + key + "-experiment\",\"hashAttribute\":\"id\",\"hashVersion\":2,"
                    + "\"condition\":{\"country\":\"US\"},\"weights\":[0,1],"
                    + "\"variations\":[\"control\",\"" + key + "\"]}]},"
                    + "\"default\":{\"defaultValue\":\"plain\"}}}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(fail ? 500 : 200, bytes.length);
            try (var stream = exchange.getResponseBody()) {
                stream.write(bytes);
            }
        });
        server.start();
        host = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        GetGrowthbookFeaturesFunction.CLIENT_CACHE.invalidateAll();
        GetGrowthbookFeaturesFunction.CLIENT_CACHE.cleanUp();
        server.stop(0);
    }

    private CacheTable users(String id, String country) {
        return new CacheTable("users", Linq4j.asEnumerable(Collections.singletonList(new Object[]{id, country})),
                List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.VARCHAR),
                        DataTypeUtils.getRelDataTypeField("country", 1, SqlTypeName.VARCHAR)));
    }

    private void check(String key, String id, String country) {
        ExecuteContext context = mock(ExecuteContext.class);
        List<Object[]> rows = function.evaluate(context, host, key, users(id, country),
                "flag", "default", "missing").scan(null).toList();
        if ("US".equals(country)) {
            assertEquals(1, rows.size());
            assertArrayEquals(new Object[]{key + "-experiment", "1", id}, rows.get(0));
            verify(context).setVariable("flag", key);
        } else {
            assertTrue(rows.isEmpty());
        }
        verifyNoMoreInteractions(context);
    }

    @Test
    void sdkKeysHaveIndependentRepositoriesAndRepeatedRequestsReuseThem() {
        check("first", "user", "US");
        check("second", "user", "US");
        check("first", "user", "US");
        check("second", "user", "US");
        assertEquals(1, requests.get("first").get());
        assertEquals(1, requests.get("second").get());
    }

    @Test
    void concurrentRequestsKeepAttributesAndSdkKeysIsolated() throws Exception {
        var pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 32; i++) {
                String key = i % 2 == 0 ? "first" : "second";
                String country = i % 4 < 2 ? "US" : "CN";
                futures.add(pool.submit(() -> {
                    assertTrue(start.await(10, TimeUnit.SECONDS));
                    check(key, "same-user", country);
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(15, TimeUnit.SECONDS);
            }
            assertEquals(1, requests.get("first").get());
            assertEquals(1, requests.get("second").get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void failedInitializationDoesNotPoisonLaterClientsOrRetry() {
        assertThrows(RuntimeException.class, () -> check("retry", "user", "US"));
        check("other", "user", "US");
        check("retry", "user", "US");
        assertEquals(2, requests.get("retry").get());
    }

    @Test
    void rejectsInvalidRowCountsBeforeFetchingFeatures() {
        ExecuteContext context = mock(ExecuteContext.class);
        CacheTable empty = new CacheTable("users", Linq4j.emptyEnumerable(), users("u", "US").getDataFields());
        CacheTable multiple = new CacheTable("users", Linq4j.asEnumerable(List.of(
                new Object[]{"a", "US"}, new Object[]{"b", "US"})), empty.getDataFields());
        for (CacheTable input : List.of(empty, multiple)) {
            RuntimeException error = assertThrows(RuntimeException.class,
                    () -> function.evaluate(context, host, "unused", input, "flag"));
            assertEquals("usertable should contain 1 row", error.getMessage());
        }
        assertTrue(requests.isEmpty());
    }
}
