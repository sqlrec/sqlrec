package com.sqlrec.connectors.redis.client;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

public class RedisClusterWrapper
        extends BaseRedisWrapper<RedisClusterClient, StatefulRedisClusterConnection<byte[], byte[]>> {
    private static final RedisConnectionRegistry<RedisClusterClient, StatefulRedisClusterConnection<byte[], byte[]>> CONNECTIONS =
            new RedisConnectionRegistry<>("RedisClusterWrapper", RedisClusterClient::create,
                    client -> client.connect(new ByteArrayCodec()));

    static {
        CONNECTIONS.registerShutdownHook();
    }

    public RedisClusterWrapper() {
        super(CONNECTIONS);
    }

    @Override
    protected RedisClusterAsyncCommands<byte[], byte[]> commands(
            StatefulRedisClusterConnection<byte[], byte[]> connection) {
        return connection.async();
    }

    /** Closes the shared resources for this URL; subsequent commands reconnect. */
    public static void invalidate(String url) {
        CONNECTIONS.invalidate(url);
    }

    public static void invalidateAll() {
        CONNECTIONS.invalidateAll();
    }
}
