package com.sqlrec.connectors.milvus.utils;

import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.handler.MilvusHandler;
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
    void legacyAndUtilityEntrypointsShareThePoolAndReturnBorrowedClients() {
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
            assertSame(client, MilvusHandler.getClient(config));
            MilvusConfig otherHandler = config("token");
            otherHandler.database = "other_database";
            assertSame(client, MilvusClientPoolUtils.getClient(otherHandler));
            assertEquals(1, pools.constructed().size());
            MilvusHandler.returnClient(client, config);
            verify(pools.constructed().get(0)).returnClient(config.url + "|token", client);
        }
    }

    @Test
    void invalidationClosesOnlyTheSelectedPoolAndAllowsRecreation() {
        MilvusConfig first = config("first");
        MilvusConfig second = config("second");
        try (MockedConstruction<MilvusClientV2Pool> pools = mockConstruction(MilvusClientV2Pool.class)) {
            MilvusHandler.getClient(first);
            MilvusClientPoolUtils.getClient(second);
            MilvusHandler.invalidatePool(MilvusClientPoolUtils.poolKey(first));
            verify(pools.constructed().get(0)).close();
            verify(pools.constructed().get(1), never()).close();

            MilvusClientV2 borrowed = mock(MilvusClientV2.class);
            MilvusHandler.returnClient(borrowed, first);
            verify(borrowed).close();
            MilvusClientPoolUtils.getClient(second);
            assertEquals(2, pools.constructed().size());
            MilvusHandler.getClient(first);
            assertEquals(3, pools.constructed().size());
        }
    }

    @Test
    void closeAllClearsEveryPoolEvenIfOneCloseFails() {
        MilvusConfig config = config("first");
        try (MockedConstruction<MilvusClientV2Pool> pools = mockConstruction(MilvusClientV2Pool.class)) {
            MilvusClientPoolUtils.getClient(config);
            MilvusClientPoolUtils.getClient(config("second"));
            doThrow(new RuntimeException("close failed")).when(pools.constructed().get(0)).close();
            assertDoesNotThrow(MilvusHandler::closeAllClientPools);
            verify(pools.constructed().get(0)).close();
            verify(pools.constructed().get(1)).close();
            MilvusClientPoolUtils.closeAll();
            verify(pools.constructed().get(0), times(1)).close();
            MilvusClientPoolUtils.getClient(config);
            assertEquals(3, pools.constructed().size());
        }
    }

    private static MilvusConfig config(String token) {
        MilvusConfig config = new MilvusConfig();
        config.url = "http://localhost:19530";
        config.token = token;
        return config;
    }
}
