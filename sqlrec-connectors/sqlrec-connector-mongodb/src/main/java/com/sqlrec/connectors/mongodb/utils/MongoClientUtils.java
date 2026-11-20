package com.sqlrec.connectors.mongodb.utils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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

    public static MongoClient getOrCreate(String uri) {
        MongoClient client = mongoClients.get(uri);
        if (client != null) {
            return client;
        }
        synchronized (mongoClients) {
            return mongoClients.computeIfAbsent(uri, MongoClients::create);
        }
    }

    /** Closes all shared clients and clears the cache, including during JVM shutdown. */
    public static void closeAll() {
        List<MongoClient> clients;
        synchronized (mongoClients) {
            clients = new ArrayList<>(mongoClients.values());
            mongoClients.clear();
        }
        for (MongoClient client : clients) {
            try {
                client.close();
            } catch (Exception e) {
                logger.warn("Failed to close MongoClient on shutdown: {}", e.getMessage());
            }
        }
        logger.info("Closed all MongoDB clients on shutdown");
    }
}
