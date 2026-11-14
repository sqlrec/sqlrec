package com.sqlrec.connectors.mongodb.utils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Shared MongoDB clients, keyed by connection URI. */
public final class MongoClientUtils {
    private static final Logger logger = LoggerFactory.getLogger(MongoClientUtils.class);
    private static final Map<String, MongoClient> mongoClients = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(MongoClientUtils::closeAll, "MongoHandler-shutdown"));
    }

    private MongoClientUtils() {
    }

    /**
     * Uses double-checked locking so expensive client creation does not hold a
     * ConcurrentHashMap bin lock and block lookups for other URIs.
     */
    public static MongoClient getOrCreate(String uri) {
        MongoClient client = mongoClients.get(uri);
        if (client != null) {
            return client;
        }
        synchronized (mongoClients) {
            client = mongoClients.get(uri);
            if (client == null) {
                client = MongoClients.create(uri);
                mongoClients.put(uri, client);
            }
            return client;
        }
    }

    /** Closes and discards a cached client so the next call creates a new one. */
    public static synchronized void invalidateClient(String uri) {
        MongoClient client = mongoClients.remove(uri);
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                // MongoDB URIs may contain credentials; do not include them in logs.
                logger.warn("Failed to close MongoClient during invalidation: {}", e.getMessage());
            }
        }
    }

    /** Closes all shared clients and clears the cache, including during JVM shutdown. */
    public static synchronized void closeAll() {
        for (Map.Entry<String, MongoClient> entry : new ArrayList<>(mongoClients.entrySet())) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                logger.warn("Failed to close MongoClient on shutdown: {}", e.getMessage());
            }
        }
        mongoClients.clear();
        logger.info("Closed all MongoDB clients on shutdown");
    }
}
