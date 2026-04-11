package com.sqlrec.connectors.kafka.config;

import com.sqlrec.common.schema.FieldSchema;

import java.util.List;

public class KafkaConfig {
    public String bootstrapServers;
    public String topic;
    public String format;
    public List<FieldSchema> fieldSchemas;
    public String keySerializer;
    public String valueSerializer;
    public int lingerMs;
}