package com.sqlrec.connectors.kafka.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaOptionsTest {

    @Test
    void defaultsToJson() {
        KafkaConfig config = KafkaOptions.getKafkaConfig(requiredOptions());

        assertEquals("json", config.format);
    }

    @Test
    void readsProtobufMessageClassName() {
        Map<String, String> options = requiredOptions();
        options.put("format", "protobuf");
        options.put("protobuf.message-class-name", "com.google.protobuf.StringValue");

        KafkaConfig config = KafkaOptions.getKafkaConfig(options);

        assertEquals("protobuf", config.format);
        assertEquals("com.google.protobuf.StringValue", config.protobufMessageClassName);
    }

    @Test
    void requiresMessageClassForProtobuf() {
        Map<String, String> options = requiredOptions();
        options.put("format", "protobuf");

        assertThrows(IllegalArgumentException.class,
                () -> KafkaOptions.getKafkaConfig(options));
    }

    @Test
    void rejectsBlankMessageClassForProtobuf() {
        Map<String, String> options = requiredOptions();
        options.put("format", "protobuf");
        options.put("protobuf.message-class-name", "   ");

        assertThrows(IllegalArgumentException.class,
                () -> KafkaOptions.getKafkaConfig(options));
    }

    @Test
    void rejectsUnknownFormat() {
        Map<String, String> options = requiredOptions();
        options.put("format", "avro");

        assertThrows(IllegalArgumentException.class,
                () -> KafkaOptions.getKafkaConfig(options));
    }

    private static Map<String, String> requiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("properties.bootstrap.servers", "localhost:9092");
        options.put("topic", "test_topic");
        return options;
    }
}
