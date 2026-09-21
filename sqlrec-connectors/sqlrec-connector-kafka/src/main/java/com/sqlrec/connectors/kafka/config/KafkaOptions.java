package com.sqlrec.connectors.kafka.config;

import com.sqlrec.common.config.ConfigOption;

import java.util.Arrays;
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
            Arrays.asList("json", "protobuf"),
            String.class
    );
    public static final ConfigOption<String> PROTOBUF_MESSAGE_CLASS_NAME = new ConfigOption<>(
            "protobuf.message-class-name",
            null,
            "Full name of the generated Protobuf message class",
            null,
            String.class
    );
    public static final ConfigOption<Integer> LINGER_MS = new ConfigOption<>(
            "properties.producer.linger.ms",
            5000,
            "Kafka producer linger ms",
            null,
            Integer.class
    );

    public static KafkaConfig getKafkaConfig(Map<String, String> options) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.bootstrapServers = BOOTSTRAP_SERVERS.getValue(options);
        kafkaConfig.topic = TOPIC.getValue(options);
        kafkaConfig.format = FORMAT.getValue(options);
        kafkaConfig.protobufMessageClassName = PROTOBUF_MESSAGE_CLASS_NAME.getValueOrNull(options);
        if ("protobuf".equals(kafkaConfig.format)
                && (kafkaConfig.protobufMessageClassName == null
                || kafkaConfig.protobufMessageClassName.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "protobuf.message-class-name is required when format is protobuf");
        }
        kafkaConfig.lingerMs = LINGER_MS.getValue(options);

        return kafkaConfig;
    }
}
