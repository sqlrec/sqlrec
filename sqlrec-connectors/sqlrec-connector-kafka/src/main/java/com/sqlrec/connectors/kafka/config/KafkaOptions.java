package com.sqlrec.connectors.kafka.config;

import com.sqlrec.common.config.ConfigOption;

import java.util.Map;

public class KafkaOptions {
    public static final String CONNECTOR_IDENTIFIER = "kafka";

    public static final ConfigOption<String> BOOTSTRAP_SERVERS = new ConfigOption<>(
            "properties.bootstrap.servers",
            null,
            "Kafka bootstrap servers");
    public static final ConfigOption<String> TOPIC = new ConfigOption<>(
            "topic",
            null,
            "Kafka topic");
    public static final ConfigOption<String> FORMAT = new ConfigOption<>(
            "format",
            "json",
            "Kafka message format");

    public static KafkaConfig getKafkaConfig(Map<String, String> options) {
        KafkaConfig kafkaConfig = new KafkaConfig();

        if (!options.containsKey(BOOTSTRAP_SERVERS.getKey())) {
            throw new RuntimeException("Kafka bootstrap servers is not set");
        }
        kafkaConfig.bootstrapServers = options.get(BOOTSTRAP_SERVERS.getKey());

        if (!options.containsKey(TOPIC.getKey())) {
            throw new RuntimeException("Kafka topic is not set");
        }
        kafkaConfig.topic = options.get(TOPIC.getKey());

        if (!options.containsKey(FORMAT.getKey())) {
            kafkaConfig.format = FORMAT.getDefaultValue();
        } else {
            kafkaConfig.format = options.get(FORMAT.getKey());
        }

        return kafkaConfig;
    }
}