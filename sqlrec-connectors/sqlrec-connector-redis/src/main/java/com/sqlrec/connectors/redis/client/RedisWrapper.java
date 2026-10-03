package com.sqlrec.connectors.redis.client;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

public class RedisWrapper extends BaseRedisWrapper<RedisClient, StatefulRedisConnection<byte[], byte[]>> {
    private static final RedisConnectionRegistry<RedisClient, StatefulRedisConnection<byte[], byte[]>> CONNECTIONS =
            new RedisConnectionRegistry<>("RedisWrapper",
                    url -> RedisClient.create(RedisURI.create(url)),
                    client -> client.connect(new ByteArrayCodec()));

    static {
        CONNECTIONS.registerShutdownHook();
    }

    public RedisWrapper() {
        super(CONNECTIONS);
    }

    @Override
    protected RedisClusterAsyncCommands<byte[], byte[]> commands(StatefulRedisConnection<byte[], byte[]> connection) {
        return connection.async();
    }

    /** Closes the shared resources for this URL; subsequent commands reconnect. */
    public static void invalidate(String url) {
        CONNECTIONS.invalidate(url);
    }

    public static void invalidateAll() {
        CONNECTIONS.invalidateAll();
    }

    /** Test-only injection. Call invalidate first when replacing an existing entry. */
    static void setConnectionForTest(String url,
            StatefulRedisConnection<byte[], byte[]> connection, RedisClient client) {
        CONNECTIONS.setConnectionForTest(url, connection, client);
    }
}
