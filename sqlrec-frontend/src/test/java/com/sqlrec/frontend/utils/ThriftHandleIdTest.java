package com.sqlrec.frontend.utils;

import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ThriftHandleIdTest {
    @Test
    void formatsEveryByteAsTwoLowercaseHexDigits() {
        byte[] guid = new byte[256];
        StringBuilder expected = new StringBuilder("guid=");
        String digits = "0123456789abcdef";
        for (int i = 0; i < guid.length; i++) {
            guid[i] = (byte) i;
            expected.append(digits.charAt(i >>> 4)).append(digits.charAt(i & 15));
        }
        THandleIdentifier handle = new THandleIdentifier(ByteBuffer.wrap(guid), ByteBuffer.wrap(new byte[0]));
        assertEquals(expected.toString(), ThriftUtils.safeHandleId(handle));
    }

    @Test
    void preservesNullAndEmptyGuidRepresentations() {
        assertEquals("null", ThriftUtils.safeHandleId(null));
        assertEquals("guid=null", ThriftUtils.safeHandleId(new THandleIdentifier()));
        assertEquals("guid=", ThriftUtils.safeHandleId(new THandleIdentifier(
                ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0]))));
    }
}
