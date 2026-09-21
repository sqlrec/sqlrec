package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.ProtobufRowCodec;

import java.util.List;

public class ProtobufCodec implements AbstractCodec {
    private final ProtobufRowCodec protobufRowCodec;
    private List<FieldSchema> fieldSchemas;

    public ProtobufCodec(String messageClassName) {
        this.protobufRowCodec = new ProtobufRowCodec(messageClassName);
    }

    @Override
    public void init(List<FieldSchema> fieldSchemas, int primaryKeyIndex) {
        this.fieldSchemas = fieldSchemas;
    }

    @Override
    public Object[] decode(byte[] bytes, String primaryKey) {
        return protobufRowCodec.decode(bytes, fieldSchemas);
    }

    @Override
    public byte[] encode(Object[] objects) {
        return protobufRowCodec.encode(objects, fieldSchemas);
    }
}
