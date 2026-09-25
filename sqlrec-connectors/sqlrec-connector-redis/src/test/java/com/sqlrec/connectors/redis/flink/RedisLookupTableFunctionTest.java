package com.sqlrec.connectors.redis.flink;

import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.handler.RedisHandler;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisLookupTableFunctionTest {
    @Mock
    RedisHandler redisHandler;

    private RedisLookupTableFunction lookup;

    @BeforeEach
    void setUp() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("key", DataTypes.STRING()),
                Column.physical("tags", DataTypes.ARRAY(DataTypes.STRING())));
        lookup = new RedisLookupTableFunction(new RedisConfig(), schema);
        lookup.setRedisHandlerForTest(redisHandler);
    }

    @Test
    void evalConvertsEveryScannedRowAndCompletesFuture() throws Exception {
        when(redisHandler.scan("42")).thenReturn(CompletableFuture.completedFuture(Arrays.asList(
                new Object[]{"42", Collections.singletonList("red")},
                new Object[]{"42", Collections.singletonList("blue")})));
        CompletableFuture<Collection<GenericRowData>> result = new CompletableFuture<>();

        lookup.eval(result, 42);

        ArrayList<GenericRowData> rows = new ArrayList<>(result.get(1, TimeUnit.SECONDS));
        assertEquals(2, rows.size());
        assertEquals(StringData.fromString("42"), rows.get(0).getString(0));
        assertEquals(StringData.fromString("red"), rows.get(0).getArray(1).getString(0));
        assertEquals(StringData.fromString("blue"), rows.get(1).getArray(1).getString(0));
        verify(redisHandler).scan("42");
    }

    @Test
    void evalWithNullKeyReturnsEmptyWithoutScanning() throws Exception {
        CompletableFuture<Collection<GenericRowData>> result = new CompletableFuture<>();

        lookup.eval(result, null);

        assertTrue(result.get(1, TimeUnit.SECONDS).isEmpty());
        verifyNoInteractions(redisHandler);
    }

    @Test
    void evalPropagatesScanFailureToFuture() {
        IllegalStateException failure = new IllegalStateException("scan failed");
        when(redisHandler.scan("key")).thenReturn(CompletableFuture.failedFuture(failure));
        CompletableFuture<Collection<GenericRowData>> result = new CompletableFuture<>();

        lookup.eval(result, "key");

        ExecutionException error = assertThrows(ExecutionException.class,
                () -> result.get(1, TimeUnit.SECONDS));
        assertSame(failure, error.getCause());
    }

    @Test
    void evalPropagatesSynchronousScanFailureToFuture() {
        IllegalStateException failure = new IllegalStateException("scan failed before returning a future");
        when(redisHandler.scan("key")).thenThrow(failure);
        CompletableFuture<Collection<GenericRowData>> result = new CompletableFuture<>();

        lookup.eval(result, "key");

        ExecutionException error = assertThrows(ExecutionException.class,
                () -> result.get(1, TimeUnit.SECONDS));
        assertSame(failure, error.getCause());
    }

    @Test
    void evalPropagatesRowConversionFailureToFuture() {
        when(redisHandler.scan("key")).thenReturn(CompletableFuture.completedFuture(
                Collections.singletonList(new Object[]{"key"})));
        CompletableFuture<Collection<GenericRowData>> result = new CompletableFuture<>();

        lookup.eval(result, "key");

        ExecutionException error = assertThrows(ExecutionException.class,
                () -> result.get(1, TimeUnit.SECONDS));
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
        assertEquals("row size must match the Flink table schema", error.getCause().getMessage());
    }
}
