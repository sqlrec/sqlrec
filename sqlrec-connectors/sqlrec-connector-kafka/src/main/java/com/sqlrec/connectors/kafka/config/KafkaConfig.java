package com.sqlrec.connectors.kafka.config;

import com.sqlrec.common.schema.FieldSchema;

import java.io.Serializable;
import java.util.List;

public class KafkaConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public String bootstrapServers;
    public String topic;
    public String format;
    public String protobufMessageClassName;
    public List<FieldSchema> fieldSchemas;
    public int lingerMs;
}
