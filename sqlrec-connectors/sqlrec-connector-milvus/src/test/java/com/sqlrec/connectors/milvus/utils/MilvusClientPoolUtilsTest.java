package com.sqlrec.connectors.milvus.utils;

import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import io.grpc.Status;
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MilvusClientPoolUtilsTest {
    @AfterEach
    void closePools() {
        MilvusClientPoolUtils.closeAll();
    }

    @Test
    void sharesThePoolAcrossDatabasesAndReturnsBorrowedClients() {
        MilvusConfig config = config("token");
        config.rpcDeadlineMs = 12000L;
        config.poolMaxIdlePerKey = 2;
        config.poolMaxTotalPerKey = 3;
        config.poolMaxTotal = 4;
        config.poolMaxBlockWaitDuration = 6L;
        config.poolMinEvictableIdleDuration = 8L;
        MilvusClientV2 client = mock(MilvusClientV2.class);
        try (MockedConstruction<MilvusClientV2Pool> pools = mockConstruction(
                MilvusClientV2Pool.class, (pool, context) -> {
                    PoolConfig settings = (PoolConfig) context.arguments().get(0);
                    ConnectConfig connection = (ConnectConfig) context.arguments().get(1);
                    assertEquals(config.url, connection.getUri());
                    assertEquals(config.token, connection.getToken());
                    assertEquals(12000L, connection.getRpcDeadlineMs());
                    assertEquals(2, settings.getMaxIdlePerKey());
                    assertEquals(3, settings.getMaxTotalPerKey());
                    assertEquals(4, settings.getMaxTotal());
                    assertEquals(Duration.ofSeconds(6), settings.getMaxBlockWaitDuration());
                    assertEquals(Duration.ofSeconds(8), settings.getMinEvictableIdleDuration());
                    when(pool.getClient(anyString())).thenReturn(client);
                })) {
            MilvusClientPoolUtils.withClient(config, borrowed -> {
                assertSame(client, borrowed);
                return null;
            });
            MilvusConfig otherHandler = config("token");
            otherHandler.database = "other_database";
            MilvusClientPoolUtils.withClient(otherHandler, borrowed -> {
                assertSame(client, borrowed);
                return null;
            });
            assertEquals(1, pools.constructed().size());
            verify(pools.constructed().get(0), times(2)).returnClient(config.url + "|token", client);
        }
    }

    @Test
    @SilenceLoggers(MilvusClientPoolUtils.class)
    void connectionFailureReplacesOnlyThePoolThatFailed() {
        MilvusConfig first = config("first");
        MilvusConfig second = config("second");
        MilvusClientV2 failed = mock(MilvusClientV2.class);
        MilvusClientV2 other = mock(MilvusClientV2.class);
        MilvusClientV2 replacement = mock(MilvusClientV2.class);
        when(failed.clientIsReady()).thenThrow(Status.UNAVAILABLE.asRuntimeException());
        when(replacement.clientIsReady()).thenReturn(true);
        try (var pools = mockConstruction(MilvusClientV2Pool.class, (pool, context) -> {
            MilvusClientV2 client = context.getCount() == 1 ? failed : context.getCount() == 2 ? other : replacement;
            when(pool.getClient(anyString())).thenReturn(client);
        })) {
            MilvusClientPoolUtils.withClient(first, client -> null);
            MilvusClientPoolUtils.withClient(second, client -> null);
            boolean ready = MilvusClientPoolUtils.withClient(first, MilvusClientV2::clientIsReady);
            assertTrue(ready);

            assertEquals(3, pools.constructed().size());
            verify(pools.constructed().get(0)).close();
            verify(pools.constructed().get(1), never()).close();
            verify(pools.constructed().get(2), never()).close();
            verify(failed).close();
        }
    }

    @Test
    @SilenceLoggers(MilvusClientPoolUtils.class)
    void closeAllClearsEveryPoolEvenIfOneCloseFails() {
        MilvusConfig config = config("first");
        MilvusClientV2 client = mock(MilvusClientV2.class);
        try (var pools = mockConstruction(MilvusClientV2Pool.class,
                (pool, context) -> when(pool.getClient(anyString())).thenReturn(client))) {
            MilvusClientPoolUtils.withClient(config, borrowed -> null);
            MilvusClientPoolUtils.withClient(config("second"), borrowed -> null);
            doThrow(new RuntimeException("close failed")).when(pools.constructed().get(0)).close();
            assertDoesNotThrow(MilvusClientPoolUtils::closeAll);
            verify(pools.constructed().get(0)).close();
            verify(pools.constructed().get(1)).close();
            MilvusClientPoolUtils.closeAll();
            verify(pools.constructed().get(0), times(1)).close();
            MilvusClientPoolUtils.withClient(config, borrowed -> null);
            assertEquals(3, pools.constructed().size());
        }
    }

    @Test
    @SilenceLoggers(MilvusClientPoolUtils.class)
    void failureFromRetiredPoolDoesNotInvalidateItsReplacement() {
        MilvusConfig config = config("token");
        MilvusClientV2 oldClient = mock(MilvusClientV2.class);
        MilvusClientV2 newClient = mock(MilvusClientV2.class);
        try (var pools = mockConstruction(MilvusClientV2Pool.class, (pool, context) ->
                when(pool.getClient(anyString())).thenReturn(context.getCount() == 1 ? oldClient : newClient))) {
            MilvusClientV2 result = MilvusClientPoolUtils.withClient(config, client -> {
                if (client == oldClient) {
                    MilvusClientPoolUtils.closeAll();
                    assertSame(newClient, MilvusClientPoolUtils.withClient(config, replacement -> replacement));
                    throw Status.UNAVAILABLE.asRuntimeException();
                }
                return client;
            });

            assertSame(newClient, result);
            assertEquals(2, pools.constructed().size());
            MilvusClientV2Pool replacement = pools.constructed().get(1);
            verify(oldClient).close();
            verify(replacement, never()).close();
            verify(replacement, never()).returnClient(anyString(), same(oldClient));
            verify(replacement, times(2)).returnClient(anyString(), same(newClient));
        }
    }

    @Test
    @SilenceLoggers(MilvusClientPoolUtils.class)
    void retiredPoolDuringBorrowIsRetriedBeforeRunningTheAction() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        try (var pools = mockConstruction(MilvusClientV2Pool.class, (pool, context) -> {
            if (context.getCount() == 1) {
                when(pool.getClient(anyString())).thenAnswer(call -> {
                    MilvusClientPoolUtils.closeAll();
                    throw new IllegalStateException("Pool is closed");
                });
            } else {
                when(pool.getClient(anyString())).thenReturn(client);
            }
        })) {
            assertSame(client, MilvusClientPoolUtils.withClient(config("token"), borrowed -> borrowed));
            assertEquals(2, pools.constructed().size());
            verify(pools.constructed().get(0)).close();
            verify(pools.constructed().get(1)).returnClient(anyString(), same(client));
        }
    }

    private static MilvusConfig config(String token) {
        MilvusConfig config = new MilvusConfig();
        config.url = "http://localhost:19530";
        config.token = token;
        return config;
    }
}
