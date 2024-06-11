package com.sqlrec.connectors.redis.codec;

import com.sqlrec.connectors.redis.config.FieldSchema;

import java.util.List;

public interface AbstractCodec {
    void init(List<FieldSchema> fieldSchemas);
    Object[] decode(byte[] bytes);
    byte[] encode(Object[] objects);
}
