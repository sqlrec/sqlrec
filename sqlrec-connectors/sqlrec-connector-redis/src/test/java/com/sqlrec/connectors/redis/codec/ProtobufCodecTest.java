package com.sqlrec.connectors.redis.codec;

import com.google.protobuf.StringValue;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtobufCodecTest {

    @Test
    void encodesAndDecodesRedisValue() throws Exception {
        ProtobufCodec codec = new ProtobufCodec("com.google.protobuf.StringValue");
        codec.init(
                Collections.singletonList(new FieldSchema("value", "VARCHAR")),
                0);

        byte[] encoded = codec.encode(new Object[]{"hello"});
        assertEquals("hello", StringValue.parseFrom(encoded).getValue());
        assertArrayEquals(new Object[]{"hello"}, codec.decode(encoded, "ignored"));
    }
}
