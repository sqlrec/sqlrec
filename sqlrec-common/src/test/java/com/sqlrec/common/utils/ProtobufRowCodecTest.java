package com.sqlrec.common.utils;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.SourceContext;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Syntax;
import com.google.protobuf.Type;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProtobufRowCodecTest {

    @Test
    void encodesGeneratedMessage() throws Exception {
        ProtobufRowCodec codec = valueCodec(StringValue.class, "VARCHAR");
        byte[] bytes = codec.encode(new Object[]{"hello"});

        assertEquals("hello", StringValue.parseFrom(bytes).getValue());
    }

    @Test
    void decodesGeneratedMessage() {
        ProtobufRowCodec codec = valueCodec(StringValue.class, "VARCHAR");
        Object[] row = codec.decode(StringValue.of("hello").toByteArray());

        assertEquals("hello", row[0]);
    }

    @Test
    void rejectsUnknownTableField() {
        ProtobufRowCodec codec = new ProtobufRowCodec(StringValue.class.getName(),
                List.of(new FieldSchema("missing", "VARCHAR")));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> codec.encode(new Object[]{"hello"}));

        assertEquals(
                "Table field 'missing' does not exist in Protobuf message google.protobuf.StringValue",
                error.getMessage());
    }

    @Test
    void rejectsNonMessageClass() {
        assertThrows(IllegalArgumentException.class,
                () -> valueCodec(String.class, "VARCHAR"));
    }

    @Test
    void roundTripsScalarTypes() throws Exception {
        assertScalarRoundTrip(Int32Value.class, 12L, 12);
        assertScalarRoundTrip(Int64Value.class, 13, 13L);
        assertScalarRoundTrip(FloatValue.class, 1.25D, 1.25F);
        assertScalarRoundTrip(DoubleValue.class, 2.5F, 2.5D);
        assertScalarRoundTrip(BoolValue.class, true, true);

        assertEquals(12, Int32Value.parseFrom(valueCodec(Int32Value.class, "INTEGER")
                .encode(new Object[]{12L})).getValue());
        assertEquals(13L, Int64Value.parseFrom(valueCodec(Int64Value.class, "BIGINT")
                .encode(new Object[]{13})).getValue());
        assertEquals(1.25F, FloatValue.parseFrom(valueCodec(FloatValue.class, "FLOAT")
                .encode(new Object[]{1.25D})).getValue());
        assertEquals(2.5D, DoubleValue.parseFrom(valueCodec(DoubleValue.class, "DOUBLE")
                .encode(new Object[]{2.5F})).getValue());
        assertTrue(BoolValue.parseFrom(valueCodec(BoolValue.class, "BOOLEAN")
                .encode(new Object[]{true})).getValue());
    }

    @Test
    void preservesUnsignedIntegerValuesAndRejectsNarrowSqlColumns() throws Exception {
        ProtobufRowCodec codec = valueCodec(UInt32Value.class, "BIGINT");
        byte[] encoded = codec.encode(new Object[]{4_000_000_000L});

        assertEquals(4_000_000_000L,
                Integer.toUnsignedLong(UInt32Value.parseFrom(encoded).getValue()));
        assertEquals(4_000_000_000L, codec.decode(encoded)[0]);
        assertThrows(IllegalArgumentException.class,
                () -> valueCodec(UInt32Value.class, "INTEGER").decode(encoded));

        ProtobufRowCodec uint64Codec = valueCodec(UInt64Value.class, "DECIMAL(20, 0)");
        BigInteger maxUnsigned64 = new BigInteger("18446744073709551615");
        byte[] uint64Encoded = uint64Codec.encode(new Object[]{maxUnsigned64});
        assertEquals(maxUnsigned64.toString(),
                Long.toUnsignedString(UInt64Value.parseFrom(uint64Encoded).getValue()));
        assertEquals(new BigDecimal(maxUnsigned64), uint64Codec.decode(uint64Encoded)[0]);
        assertThrows(IllegalArgumentException.class,
                () -> valueCodec(UInt64Value.class, "BIGINT").decode(uint64Encoded));
    }

    @Test
    void acceptsExactIntegersAcrossJavaNumericTypes() throws Exception {
        ProtobufRowCodec codec = valueCodec(Int32Value.class, "INTEGER");
        for (Number value : List.of((byte) 12, (short) 12, 12, 12L,
                BigInteger.valueOf(12), new BigDecimal("12.000"), 12.0F, 12.0D,
                new java.util.concurrent.atomic.AtomicLong(12))) {
            byte[] encoded = codec.encode(new Object[]{value});
            assertEquals(12, Int32Value.parseFrom(encoded).getValue(), value.getClass().getName());
        }
    }

    @Test
    void rejectsIntegerOverflowAndFractionalValues() {
        ProtobufRowCodec int32Codec = valueCodec(Int32Value.class, "BIGINT");
        assertThrows(IllegalArgumentException.class,
                () -> int32Codec.encode(new Object[]{2_147_483_648L}));
        for (Number value : List.of(1.5D, new BigDecimal("1.5"), Float.NaN, Double.POSITIVE_INFINITY)) {
            assertThrows(IllegalArgumentException.class,
                    () -> int32Codec.encode(new Object[]{value}));
        }

        ProtobufRowCodec uint32Codec = valueCodec(UInt32Value.class, "BIGINT");
        assertThrows(IllegalArgumentException.class,
                () -> uint32Codec.encode(new Object[]{-1}));
        assertThrows(IllegalArgumentException.class,
                () -> uint32Codec.encode(new Object[]{4_294_967_296L}));

        ProtobufRowCodec int64Codec = valueCodec(Int64Value.class, "DECIMAL");
        assertThrows(IllegalArgumentException.class,
                () -> int64Codec.encode(new Object[]{BigInteger.ONE.shiftLeft(63)}));

        ProtobufRowCodec enumCodec = new ProtobufRowCodec(Type.class.getName(),
                List.of(new FieldSchema("syntax", "BIGINT")));
        assertThrows(IllegalArgumentException.class,
                () -> enumCodec.encode(new Object[]{4_294_967_297L}));
    }

    @Test
    void supportsAllBytesInputTypesAndDecodesToByteArray() throws Exception {
        ProtobufRowCodec codec = valueCodec(BytesValue.class, "VARBINARY");
        byte[] expected = new byte[]{1, 2, 3};

        assertArrayEquals(expected, BytesValue.parseFrom(
                codec.encode(new Object[]{expected})).getValue().toByteArray());
        assertArrayEquals(expected, BytesValue.parseFrom(
                codec.encode(new Object[]{ByteBuffer.wrap(expected)})).getValue().toByteArray());
        assertArrayEquals(expected, BytesValue.parseFrom(
                codec.encode(new Object[]{ByteString.copyFrom(expected)})).getValue().toByteArray());
        assertArrayEquals(expected, (byte[]) codec.decode(
                BytesValue.of(ByteString.copyFrom(expected)).toByteArray())[0]);
    }

    @Test
    void encodesRemainingBufferBytesWithoutChangingItsPositionOrMark() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{9, 1, 2, 3, 8});
        buffer.position(1);
        buffer.limit(4);
        buffer.mark();
        ProtobufRowCodec codec = valueCodec(BytesValue.class, "VARBINARY");

        byte[] encoded = codec.encode(new Object[]{buffer});

        assertArrayEquals(new byte[]{1, 2, 3}, BytesValue.parseFrom(encoded).getValue().toByteArray());
        assertEquals(1, buffer.position());
        assertEquals(4, buffer.limit());
        assertDoesNotThrow(() -> buffer.reset());
        assertEquals(1, buffer.position());
    }

    @Test
    void roundTripsRepeatedNestedAndEnumFields() throws Exception {
        List<FieldSchema> schema = Arrays.asList(
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("fields", "ARRAY<ROW<name STRING>>"),
                new FieldSchema("source_context", "ROW<file_name STRING>"),
                new FieldSchema("syntax", "VARCHAR"));
        ProtobufRowCodec codec = new ProtobufRowCodec(Type.class.getName(), schema);
        Map<String, Object> nestedField = Collections.singletonMap("name", "id");
        Map<String, Object> sourceContext = Collections.singletonMap("file_name", "schema.proto");

        byte[] encoded = codec.encode(new Object[]{
                "example.Type",
                Collections.singletonList(nestedField),
                sourceContext,
                "SYNTAX_PROTO3"
        });

        Type message = Type.parseFrom(encoded);
        assertEquals("example.Type", message.getName());
        assertEquals("id", message.getFields(0).getName());
        assertEquals("schema.proto", message.getSourceContext().getFileName());
        assertEquals(Syntax.SYNTAX_PROTO3, message.getSyntax());

        Object[] decoded = codec.decode(encoded);
        assertEquals("example.Type", decoded[0]);
        assertEquals("id", ((Map<?, ?>) ((List<?>) decoded[1]).get(0)).get("name"));
        assertEquals("schema.proto", ((Map<?, ?>) decoded[2]).get("file_name"));
        assertEquals("SYNTAX_PROTO3", decoded[3]);
    }

    @Test
    void acceptsJavaArrayForRepeatedFieldAndNumericEnumValue() throws Exception {
        Object[] repeatedFields = {
                Collections.singletonMap("name", "first"),
                Collections.singletonMap("name", "second")
        };
        List<FieldSchema> schema = Arrays.asList(
                new FieldSchema("fields", "ARRAY<ROW<name STRING>>"),
                new FieldSchema("syntax", "INTEGER"));
        ProtobufRowCodec codec = new ProtobufRowCodec(Type.class.getName(), schema);

        byte[] encoded = codec.encode(new Object[]{repeatedFields, 1});
        Type message = Type.parseFrom(encoded);
        assertEquals(Arrays.asList("first", "second"), Arrays.asList(
                message.getFields(0).getName(), message.getFields(1).getName()));
        assertEquals(Syntax.SYNTAX_PROTO3, message.getSyntax());

        Object[] decoded = codec.decode(encoded);
        assertEquals(1, decoded[1]);
    }

    @Test
    void acceptsMatchingGeneratedMessageForNestedField() throws Exception {
        ProtobufRowCodec codec = new ProtobufRowCodec(Type.class.getName(),
                List.of(new FieldSchema("source_context", "ROW<file_name STRING>")));
        SourceContext sourceContext = SourceContext.newBuilder()
                .setFileName("schema.proto")
                .build();

        byte[] encoded = codec.encode(new Object[]{sourceContext});

        assertEquals("schema.proto", Type.parseFrom(encoded)
                .getSourceContext().getFileName());
    }

    @Test
    void roundTripsMapWithNestedMessageValues() throws Exception {
        ProtobufRowCodec codec = new ProtobufRowCodec(Struct.class.getName(),
                List.of(new FieldSchema("fields", "MAP<STRING, ROW<string_value STRING>>")));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("name", Collections.singletonMap("string_value", "alice"));

        byte[] encoded = codec.encode(new Object[]{value});

        assertEquals("alice", Struct.parseFrom(encoded)
                .getFieldsOrThrow("name").getStringValue());
        Map<?, ?> decodedMap = (Map<?, ?>) codec.decode(encoded)[0];
        assertEquals("alice", ((Map<?, ?>) decodedMap.get("name")).get("string_value"));
    }

    @Test
    void distinguishesAbsentMessageFromDefaultScalar() {
        ProtobufRowCodec codec = new ProtobufRowCodec(Type.class.getName(), List.of(
                new FieldSchema("source_context", "ROW<file_name STRING>"),
                new FieldSchema("name", "VARCHAR")));

        Object[] decoded = codec.decode(Type.getDefaultInstance().toByteArray());

        assertNull(decoded[0]);
        assertEquals("", decoded[1]);
    }

    @Test
    void rejectsInvalidConstructorAndInputArguments() {
        assertThrows(IllegalArgumentException.class, () -> new ProtobufRowCodec(null, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new ProtobufRowCodec("   ", List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new ProtobufRowCodec("example.missing.Message", List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new ProtobufRowCodec(StringValue.class.getName(), null));

        ProtobufRowCodec codec = valueCodec(StringValue.class, "VARCHAR");
        assertThrows(IllegalArgumentException.class, () -> codec.encode(null));
        assertThrows(IllegalArgumentException.class,
                () -> codec.encode(new Object[]{"one", "two"}));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(null));
    }

    @Test
    void rejectsMalformedPayloadAndUnknownDecodeField() {
        ProtobufRowCodec codec = valueCodec(StringValue.class, "VARCHAR");
        ProtobufRowCodec missingFieldCodec = new ProtobufRowCodec(StringValue.class.getName(),
                List.of(new FieldSchema("missing", "VARCHAR")));

        assertThrows(IllegalArgumentException.class,
                () -> codec.decode(new byte[]{0x0A, 0x05, 0x01}));
        assertThrows(IllegalArgumentException.class,
                () -> missingFieldCodec.decode(StringValue.of("hello").toByteArray()));
    }

    @Test
    void rejectsIncompatibleCollectionAndScalarValues() {
        ProtobufRowCodec fieldsCodec = new ProtobufRowCodec(Type.class.getName(),
                List.of(new FieldSchema("fields", "ARRAY<ROW<name STRING>>")));
        assertThrows(IllegalArgumentException.class,
                () -> fieldsCodec.encode(new Object[]{"not-a-list"}));
        assertThrows(IllegalArgumentException.class,
                () -> fieldsCodec.encode(new Object[]{Collections.singletonList(null)}));

        ProtobufRowCodec enumCodec = new ProtobufRowCodec(Type.class.getName(),
                List.of(new FieldSchema("syntax", "VARCHAR")));
        assertThrows(IllegalArgumentException.class,
                () -> enumCodec.encode(new Object[]{"UNKNOWN_SYNTAX"}));
        assertThrows(IllegalArgumentException.class,
                () -> enumCodec.encode(new Object[]{999}));

        ProtobufRowCodec nestedCodec = new ProtobufRowCodec(Type.class.getName(),
                List.of(new FieldSchema("source_context", "ROW<file_name STRING>")));
        assertThrows(IllegalArgumentException.class,
                () -> nestedCodec.encode(new Object[]{Collections.singletonMap("unknown", "value")}));
        assertThrows(IllegalArgumentException.class,
                () -> nestedCodec.encode(new Object[]{StringValue.of("wrong message type")}));

        ProtobufRowCodec structCodec = new ProtobufRowCodec(Struct.class.getName(),
                List.of(new FieldSchema("fields", "MAP<STRING, ROW<string_value STRING>>")));
        assertThrows(IllegalArgumentException.class,
                () -> structCodec.encode(new Object[]{"not-a-map"}));
        Map<String, Object> mapWithNullValue = new LinkedHashMap<>();
        mapWithNullValue.put("key", null);
        assertThrows(IllegalArgumentException.class,
                () -> structCodec.encode(new Object[]{mapWithNullValue}));

        ProtobufRowCodec intCodec = valueCodec(Int32Value.class, "INTEGER");
        assertThrows(IllegalArgumentException.class,
                () -> intCodec.encode(new Object[]{"not-a-number"}));

        ProtobufRowCodec bytesCodec = valueCodec(BytesValue.class, "VARBINARY");
        assertThrows(IllegalArgumentException.class,
                () -> bytesCodec.encode(new Object[]{"not-bytes"}));
    }

    private static ProtobufRowCodec valueCodec(Class<?> messageClass, String sqlType) {
        return new ProtobufRowCodec(messageClass.getName(), List.of(new FieldSchema("value", sqlType)));
    }

    private static void assertScalarRoundTrip(
            Class<?> messageClass,
            Object input,
            Object expected
    ) {
        ProtobufRowCodec codec = valueCodec(messageClass, "UNKNOWN");
        Object[] decoded = codec.decode(codec.encode(new Object[]{input}));
        assertEquals(expected, decoded[0]);
    }
}
