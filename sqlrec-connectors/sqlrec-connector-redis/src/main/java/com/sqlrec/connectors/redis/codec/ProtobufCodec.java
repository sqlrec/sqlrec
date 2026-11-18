package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.ProtobufRowCodec;

import java.util.List;

public class ProtobufCodec implements AbstractCodec {
    private final String messageClassName;
    private ProtobufRowCodec protobufRowCodec;

    public ProtobufCodec(String messageClassName) {
        this.messageClassName = messageClassName;
    }

    @Override
    public void init(List<FieldSchema> fieldSchemas, int primaryKeyIndex) {
        this.protobufRowCodec = new ProtobufRowCodec(messageClassName, fieldSchemas);
    }

    @Override
    public Object[] decode(byte[] bytes, String primaryKey) {
        return protobufRowCodec.decode(bytes);
    }

    @Override
    public byte[] encode(Object[] objects) {
        return protobufRowCodec.encode(objects);
    }
}
