package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.utils.FieldSchema;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class JsonCodecTest {

    @Test
    public void testCodec() throws IOException {

        JsonCodec jsonCodec = new JsonCodec();
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("name", "string"));
        fieldSchemas.add(new FieldSchema("age", "int"));
        jsonCodec.init(fieldSchemas);

        Object[] objects = new Object[3];
        objects[0] = "张三";
        objects[1] = 20L;
        objects[2] = 0.1D;

        byte[] bytes = jsonCodec.encode(objects);
        String str = new String(bytes, StandardCharsets.UTF_8);

        Object[] objects2 = jsonCodec.decode(bytes);
        assert objects[0].equals(objects2[0]);
        assert objects[1].equals(objects2[1]);
        assert objects[2].equals(objects2[2]);
    }
}