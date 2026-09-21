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
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

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
        ProtobufRowCodec codec =
                new ProtobufRowCodec("com.google.protobuf.StringValue");

        byte[] bytes = codec.encode(
                new Object[]{"hello"},
                Collections.singletonList(new FieldSchema("value", "VARCHAR")));

        assertEquals("hello", StringValue.parseFrom(bytes).getValue());
    }

    @Test
    void decodesGeneratedMessage() {
        ProtobufRowCodec codec =
                new ProtobufRowCodec("com.google.protobuf.StringValue");

        Object[] row = codec.decode(
                StringValue.of("hello").toByteArray(),
                Collections.singletonList(new FieldSchema("value", "VARCHAR")));

        assertEquals("hello", row[0]);
    }

    @Test
    void rejectsUnknownTableField() {
        ProtobufRowCodec codec =
                new ProtobufRowCodec("com.google.protobuf.StringValue");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> codec.encode(
                        new Object[]{"hello"},
                        Collections.singletonList(new FieldSchema("missing", "VARCHAR"))));

        assertEquals(
                "Table field 'missing' does not exist in Protobuf message google.protobuf.StringValue",
                error.getMessage());
    }

    @Test
    void rejectsNonMessageClass() {
        assertThrows(IllegalArgumentException.class,
                () -> new ProtobufRowCodec("java.lang.String"));
    }

    @Test
    void roundTripsScalarTypes() throws Exception {
        assertScalarRoundTrip("com.google.protobuf.Int32Value", 12L, 12);
        assertScalarRoundTrip("com.google.protobuf.Int64Value", 13, 13L);
        assertScalarRoundTrip("com.google.protobuf.FloatValue", 1.25D, 1.25F);
        assertScalarRoundTrip("com.google.protobuf.DoubleValue", 2.5F, 2.5D);
        assertScalarRoundTrip("com.google.protobuf.BoolValue", true, true);

        assertEquals(12, Int32Value.parseFrom(new ProtobufRowCodec(
                "com.google.protobuf.Int32Value").encode(
                new Object[]{12L}, valueSchema("INTEGER"))).getValue());
        assertEquals(13L, Int64Value.parseFrom(new ProtobufRowCodec(
                "com.google.protobuf.Int64Value").encode(
                new Object[]{13}, valueSchema("BIGINT"))).getValue());
        assertEquals(1.25F, FloatValue.parseFrom(new ProtobufRowCodec(
                "com.google.protobuf.FloatValue").encode(
                new Object[]{1.25D}, valueSchema("FLOAT"))).getValue());
        assertEquals(2.5D, DoubleValue.parseFrom(new ProtobufRowCodec(
                "com.google.protobuf.DoubleValue").encode(
                new Object[]{2.5F}, valueSchema("DOUBLE"))).getValue());
        assertTrue(BoolValue.parseFrom(new ProtobufRowCodec(
                "com.google.protobuf.BoolValue").encode(
                new Object[]{true}, valueSchema("BOOLEAN"))).getValue());
    }

    @Test
    void supportsAllBytesInputTypesAndDecodesToByteArray() throws Exception {
        ProtobufRowCodec codec = new ProtobufRowCodec("com.google.protobuf.BytesValue");
        List<FieldSchema> schema = valueSchema("VARBINARY");
        byte[] expected = new byte[]{1, 2, 3};

        assertArrayEquals(expected, BytesValue.parseFrom(
                codec.encode(new Object[]{expected}, schema)).getValue().toByteArray());
        assertArrayEquals(expected, BytesValue.parseFrom(
                codec.encode(new Object[]{ByteBuffer.wrap(expected)}, schema)).getValue().toByteArray());
        assertArrayEquals(expected, BytesValue.parseFrom(
                codec.encode(new Object[]{ByteString.copyFrom(expected)}, schema)).getValue().toByteArray());
        assertArrayEquals(expected, (byte[]) codec.decode(
                BytesValue.of(ByteString.copyFrom(expected)).toByteArray(), schema)[0]);
    }

    @Test
    void roundTripsRepeatedNestedAndEnumFields() throws Exception {
        ProtobufRowCodec codec = new ProtobufRowCodec("com.google.protobuf.Type");
        List<FieldSchema> schema = Arrays.asList(
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("fields", "ARRAY<ROW>"),
                new FieldSchema("source_context", "ROW"),
                new FieldSchema("syntax", "VARCHAR"));
        Map<String, Object> nestedField = Collections.singletonMap("name", "id");
        Map<String, Object> sourceContext = Collections.singletonMap("file_name", "schema.proto");

        byte[] encoded = codec.encode(new Object[]{
                "example.Type",
                Collections.singletonList(nestedField),
                sourceContext,
                "SYNTAX_PROTO3"
        }, schema);

        Type message = Type.parseFrom(encoded);
        assertEquals("example.Type", message.getName());
        assertEquals("id", message.getFields(0).getName());
        assertEquals("schema.proto", message.getSourceContext().getFileName());
        assertEquals(Syntax.SYNTAX_PROTO3, message.getSyntax());

        Object[] decoded = codec.decode(encoded, schema);
        assertEquals("example.Type", decoded[0]);
        assertEquals("id", ((Map<?, ?>) ((List<?>) decoded[1]).get(0)).get("name"));
        assertEquals("schema.proto", ((Map<?, ?>) decoded[2]).get("file_name"));
        assertEquals("SYNTAX_PROTO3", decoded[3]);
    }

    @Test
    void acceptsJavaArrayForRepeatedFieldAndNumericEnumValue() throws Exception {
        ProtobufRowCodec codec = new ProtobufRowCodec("com.google.protobuf.Type");
        Object[] repeatedFields = {
                Collections.singletonMap("name", "first"),
                Collections.singletonMap("name", "second")
        };
        List<FieldSchema> schema = Arrays.asList(
                new FieldSchema("fields", "ARRAY<ROW>"),
                new FieldSchema("syntax", "INTEGER"));

        byte[] encoded = codec.encode(new Object[]{repeatedFields, 1}, schema);
        Type message = Type.parseFrom(encoded);
        assertEquals(Arrays.asList("first", "second"), Arrays.asList(
                message.getFields(0).getName(), message.getFields(1).getName()));
        assertEquals(Syntax.SYNTAX_PROTO3, message.getSyntax());

        Object[] decoded = codec.decode(encoded, schema);
        assertEquals(1, decoded[1]);
    }

    @Test
    void acceptsMatchingGeneratedMessageForNestedField() throws Exception {
        ProtobufRowCodec codec = new ProtobufRowCodec("com.google.protobuf.Type");
        SourceContext sourceContext = SourceContext.newBuilder()
                .setFileName("schema.proto")
                .build();

        byte[] encoded = codec.encode(
                new Object[]{sourceContext},
                Collections.singletonList(new FieldSchema("source_context", "ROW")));

        assertEquals("schema.proto", Type.parseFrom(encoded)
                .getSourceContext().getFileName());
    }

    @Test
    void roundTripsMapWithNestedMessageValues() throws Exception {
        ProtobufRowCodec codec = new ProtobufRowCodec("com.google.protobuf.Struct");
        List<FieldSchema> schema = Collections.singletonList(new FieldSchema("fields", "MAP"));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("name", Collections.singletonMap("string_value", "alice"));

        byte[] encoded = codec.encode(new Object[]{value}, schema);

        assertEquals("alice", Struct.parseFrom(encoded)
                .getFieldsOrThrow("name").getStringValue());
        Map<?, ?> decodedMap = (Map<?, ?>) codec.decode(encoded, schema)[0];
        assertEquals("alice", ((Map<?, ?>) decodedMap.get("name")).get("string_value"));
    }

    @Test
    void distinguishesAbsentMessageFromDefaultScalar() {
        ProtobufRowCodec codec = new ProtobufRowCodec("com.google.protobuf.Type");
        List<FieldSchema> schema = Arrays.asList(
                new FieldSchema("source_context", "ROW"),
                new FieldSchema("name", "VARCHAR"));

        Object[] decoded = codec.decode(Type.getDefaultInstance().toByteArray(), schema);

        assertNull(decoded[0]);
        assertEquals("", decoded[1]);
    }

    @Test
    void rejectsInvalidConstructorAndInputArguments() {
        assertThrows(IllegalArgumentException.class, () -> new ProtobufRowCodec(null));
        assertThrows(IllegalArgumentException.class, () -> new ProtobufRowCodec("   "));
        assertThrows(IllegalArgumentException.class,
                () -> new ProtobufRowCodec("example.missing.Message"));

        ProtobufRowCodec codec = new ProtobufRowCodec("com.google.protobuf.StringValue");
        assertThrows(IllegalArgumentException.class, () -> codec.encode(null, valueSchema("VARCHAR")));
        assertThrows(IllegalArgumentException.class,
                () -> codec.encode(new Object[]{"one", "two"}, valueSchema("VARCHAR")));
        assertThrows(IllegalArgumentException.class, () -> codec.encode(new Object[]{"one"}, null));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(null, valueSchema("VARCHAR")));
        assertThrows(IllegalArgumentException.class,
                () -> codec.decode(StringValue.getDefaultInstance().toByteArray(), null));
    }

    @Test
    void rejectsMalformedPayloadAndUnknownDecodeField() {
        ProtobufRowCodec codec = new ProtobufRowCodec("com.google.protobuf.StringValue");

        assertThrows(IllegalArgumentException.class,
                () -> codec.decode(new byte[]{0x0A, 0x05, 0x01}, valueSchema("VARCHAR")));
        assertThrows(IllegalArgumentException.class,
                () -> codec.decode(StringValue.of("hello").toByteArray(),
                        Collections.singletonList(new FieldSchema("missing", "VARCHAR"))));
    }

    @Test
    void rejectsIncompatibleCollectionAndScalarValues() {
        ProtobufRowCodec typeCodec = new ProtobufRowCodec("com.google.protobuf.Type");
        assertThrows(IllegalArgumentException.class,
                () -> typeCodec.encode(new Object[]{"not-a-list"},
                        Collections.singletonList(new FieldSchema("fields", "ARRAY<ROW>"))));
        assertThrows(IllegalArgumentException.class,
                () -> typeCodec.encode(new Object[]{Collections.singletonList(null)},
                        Collections.singletonList(new FieldSchema("fields", "ARRAY<ROW>"))));
        assertThrows(IllegalArgumentException.class,
                () -> typeCodec.encode(new Object[]{"UNKNOWN_SYNTAX"},
                        Collections.singletonList(new FieldSchema("syntax", "VARCHAR"))));
        assertThrows(IllegalArgumentException.class,
                () -> typeCodec.encode(new Object[]{999},
                        Collections.singletonList(new FieldSchema("syntax", "INTEGER"))));
        assertThrows(IllegalArgumentException.class,
                () -> typeCodec.encode(new Object[]{Collections.singletonMap("unknown", "value")},
                        Collections.singletonList(new FieldSchema("source_context", "ROW"))));
        assertThrows(IllegalArgumentException.class,
                () -> typeCodec.encode(new Object[]{StringValue.of("wrong message type")},
                        Collections.singletonList(new FieldSchema("source_context", "ROW"))));

        ProtobufRowCodec structCodec = new ProtobufRowCodec("com.google.protobuf.Struct");
        assertThrows(IllegalArgumentException.class,
                () -> structCodec.encode(new Object[]{"not-a-map"},
                        Collections.singletonList(new FieldSchema("fields", "MAP"))));
        Map<String, Object> mapWithNullValue = new LinkedHashMap<>();
        mapWithNullValue.put("key", null);
        assertThrows(IllegalArgumentException.class,
                () -> structCodec.encode(new Object[]{mapWithNullValue},
                        Collections.singletonList(new FieldSchema("fields", "MAP"))));

        ProtobufRowCodec intCodec = new ProtobufRowCodec("com.google.protobuf.Int32Value");
        assertThrows(IllegalArgumentException.class,
                () -> intCodec.encode(new Object[]{"not-a-number"}, valueSchema("INTEGER")));

        ProtobufRowCodec bytesCodec = new ProtobufRowCodec("com.google.protobuf.BytesValue");
        assertThrows(IllegalArgumentException.class,
                () -> bytesCodec.encode(new Object[]{"not-bytes"}, valueSchema("VARBINARY")));
    }

    private static List<FieldSchema> valueSchema(String sqlType) {
        return Collections.singletonList(new FieldSchema("value", sqlType));
    }

    private static void assertScalarRoundTrip(
            String messageClassName,
            Object input,
            Object expected
    ) {
        ProtobufRowCodec codec = new ProtobufRowCodec(messageClassName);
        Object[] decoded = codec.decode(codec.encode(
                new Object[]{input}, valueSchema("UNKNOWN")), valueSchema("UNKNOWN"));
        assertEquals(expected, decoded[0]);
    }
}
