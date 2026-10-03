package com.sqlrec.connectors.redis.client;

import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class RedisClusterWrapperUnitTest {
    @Test
    void sharesClusterConnectionUntilInvalidationAndUsesAdvancedCommandRouting() {
        String url = "redis://cluster-test:6379";
        RedisClusterClient client = mock(RedisClusterClient.class);
        StatefulRedisClusterConnection<byte[], byte[]> connection = mock(StatefulRedisClusterConnection.class);
        RedisAdvancedClusterAsyncCommands<byte[], byte[]> commands = mock(RedisAdvancedClusterAsyncCommands.class);
        when(client.connect(any(ByteArrayCodec.class))).thenReturn(connection);
        when(connection.async()).thenReturn(commands);
        byte[] firstKey = "{one}:key".getBytes(StandardCharsets.UTF_8);
        byte[] secondKey = "{two}:key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        try (MockedStatic<RedisClusterClient> factory = mockStatic(RedisClusterClient.class)) {
            factory.when(() -> RedisClusterClient.create(url)).thenReturn(client);
            try {
                RedisClusterWrapper first = new RedisClusterWrapper();
                RedisClusterWrapper second = new RedisClusterWrapper();
                first.open(url);
                second.open(url);
                factory.verifyNoInteractions();
                first.get(firstKey);
                first.close();
                second.mget(firstKey, secondKey);
                second.setex(secondKey, value, 60);
                verify(client).connect(any(ByteArrayCodec.class));
                verify(connection, never()).close();
                verify(commands).mget(firstKey, secondKey);
                verify(commands).set(eq(secondKey), eq(value), any(SetArgs.class));

                first.invalidate();
                RedisClusterWrapper.invalidate(url);
                verify(connection).close();
                verify(client).shutdown();
                second.get(secondKey);
                factory.verify(() -> RedisClusterClient.create(url), times(2));
                verify(client, times(2)).connect(any(ByteArrayCodec.class));
            } finally {
                RedisClusterWrapper.invalidateAll();
            }
        }
    }
}
