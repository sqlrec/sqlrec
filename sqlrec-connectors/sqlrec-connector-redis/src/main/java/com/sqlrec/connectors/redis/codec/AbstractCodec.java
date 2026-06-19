package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;

import java.util.List;

public interface AbstractCodec {
    void init(List<FieldSchema> fieldSchemas, int primaryKeyIndex);
    Object[] decode(byte[] bytes, String primaryKey);
    byte[] encode(Object[] objects);
}
