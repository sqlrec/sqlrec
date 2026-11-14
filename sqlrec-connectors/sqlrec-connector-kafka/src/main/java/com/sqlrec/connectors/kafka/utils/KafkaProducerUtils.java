package com.sqlrec.connectors.kafka.utils;

import com.sqlrec.connectors.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/** Shared Kafka producers, keyed by broker addresses and linger time. */
public final class KafkaProducerUtils {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerUtils.class);
    private static final Map<String, KafkaProducer<String, byte[]>> producers = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(
                new Thread(KafkaProducerUtils::closeAll, "KafkaCalciteTable-shutdown"));
    }

    private KafkaProducerUtils() {
    }

    public static KafkaProducer<String, byte[]> getOrCreate(KafkaConfig config) {
        String key = config.bootstrapServers + "|" + config.lingerMs;
        return producers.computeIfAbsent(key, ignored -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", config.bootstrapServers);
            props.put("key.serializer", StringSerializer.class.getName());
            props.put("value.serializer", ByteArraySerializer.class.getName());
            props.put("linger.ms", String.valueOf(config.lingerMs));
            return new KafkaProducer<>(props);
        });
    }

    /** Closes every shared producer and clears the cache, including during JVM shutdown. */
    public static synchronized void closeAll() {
        for (Map.Entry<String, KafkaProducer<String, byte[]>> entry : new ArrayList<>(producers.entrySet())) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                logger.warn("Failed to close Kafka producer for key {}: {}", entry.getKey(), e.getMessage(), e);
            }
        }
        producers.clear();
    }
}
