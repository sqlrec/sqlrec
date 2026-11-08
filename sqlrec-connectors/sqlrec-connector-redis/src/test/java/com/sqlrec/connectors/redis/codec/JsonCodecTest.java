package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class JsonCodecTest {
    @Test
    void convertsDecimalAndExponentNumericLiteralsToDeclaredIntegers() {
        JsonCodec codec = new JsonCodec();
        codec.init(List.of(new FieldSchema("id", "INT"), new FieldSchema("ids", "ARRAY<BIGINT>")), 0);
        assertArrayEquals(new Object[]{1, List.of(100L, 9007199254740993L)}, codec.decode(
                "{\"id\":1.0,\"ids\":[1e2,9007199254740993]}".getBytes(StandardCharsets.UTF_8), "1"));
    }

    @Test
    void resolvesSchemaOnceAndReplacesItOnReinitialization() {
        FieldSchema id = spy(new FieldSchema("id", "INT"));
        JsonCodec codec = new JsonCodec();
        codec.init(List.of(id), 0);
        byte[] json = "{\"id\":7}".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 5; i++) {
            assertArrayEquals(new Object[]{7}, codec.decode(json, "7"));
        }
        verify(id, times(1)).getType();
        codec.init(List.of(new FieldSchema("id", "STRING")), 0);
        assertArrayEquals(new Object[]{"7"}, codec.decode(json, "7"));
    }

    @Test
    void roundTripUsesDeclaredJavaTypes() {
        JsonCodec codec = new JsonCodec();
        codec.init(List.of(new FieldSchema("name", "string"), new FieldSchema("age", "int"),
                new FieldSchema("score", "double")), 0);
        Object[] values = {"张三", 20, 0.1d};
        assertArrayEquals(values, codec.decode(codec.encode(values), null));
    }

    @Test
    void convertsMismatchesAndKeepsOnlyInvalidCellsNull() {
        JsonCodec codec = new JsonCodec();
        codec.init(List.of(new FieldSchema("id", "INTEGER"), new FieldSchema("flag", "BOOLEAN"),
                new FieldSchema("scores", "ARRAY<DOUBLE>")), 0);
        Object[] decoded = codec.decode("{\"id\":\"42\",\"flag\":\"bad\",\"scores\":[\"0.25\",{},null]}"
                .getBytes(StandardCharsets.UTF_8), "42");
        assertArrayEquals(new Object[]{42, false, Arrays.asList(0.25d, null, null)}, decoded);
    }
}
