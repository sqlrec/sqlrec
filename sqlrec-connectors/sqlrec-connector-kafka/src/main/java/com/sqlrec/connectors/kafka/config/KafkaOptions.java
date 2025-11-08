package com.sqlrec.connectors.kafka.config;

import com.sqlrec.common.config.ConfigOption;

import java.util.Map;

public class KafkaOptions {
    public static final String CONNECTOR_IDENTIFIER = "kafka";

    public static final ConfigOption<String> BOOTSTRAP_SERVERS = new ConfigOption<>(
            "properties.bootstrap.servers",
            null,
            "Kafka bootstrap servers",
            null,
            String.class
    );
    public static final ConfigOption<String> TOPIC = new ConfigOption<>(
            "topic",
            null,
            "Kafka topic",
            null,
            String.class
    );
    public static final ConfigOption<String> FORMAT = new ConfigOption<>(
            "format",
            "json",
            "Kafka message format",
            null,
            String.class
    );


    public static KafkaConfig getKafkaConfig(Map<String, String> options) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.bootstrapServers = BOOTSTRAP_SERVERS.getValue(options);
        kafkaConfig.topic = TOPIC.getValue(options);
        kafkaConfig.format = FORMAT.getValue(options);

        return kafkaConfig;
    }
}