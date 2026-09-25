package com.sqlrec.connectors.redis.codec;

import com.google.protobuf.ByteString;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.redis.proto.RedisAllTypes;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Integration coverage for Protobuf code generation and the Redis Protobuf codec. */
class ProtobufGeneratedClassIntegrationTest {

    private static final String MESSAGE_CLASS_NAME =
            "com.sqlrec.connectors.redis.proto.RedisAllTypes";

    @Test
    void roundTripsGeneratedMessageWithAllSupportedFieldTypes() throws Exception {
        ProtobufCodec codec = codec(allFieldSchemas());
        Object[] input = allFieldValues();

        byte[] encoded = codec.encode(input);

        RedisAllTypes message = RedisAllTypes.parseFrom(encoded);
        assertGeneratedMessage(message);

        // Re-serialize through the generated Java class before exercising Redis decoding.
        Object[] decoded = codec.decode(message.toByteArray(), "ignored-primary-key");
        assertDecodedRow(decoded);
    }

    @Test
    void preservesOptionalAndOneofPresence() throws Exception {
        List<FieldSchema> schemas = Arrays.asList(
                new FieldSchema("optional_string", "VARCHAR"),
                new FieldSchema("choice_number", "BIGINT"));
        ProtobufCodec codec = codec(schemas);

        byte[] encoded = codec.encode(new Object[]{null, 99L});

        RedisAllTypes message = RedisAllTypes.parseFrom(encoded);
        assertFalse(message.hasOptionalString());
        assertEquals(RedisAllTypes.ChoiceCase.CHOICE_NUMBER, message.getChoiceCase());
        assertEquals(99L, message.getChoiceNumber());

        Object[] decoded = codec.decode(encoded, "ignored-primary-key");
        assertNull(decoded[0]);
        assertEquals(99L, decoded[1]);
    }

    @Test
    void acceptsAndReturnsNumericEnumRepresentation() throws Exception {
        List<FieldSchema> schemas = Arrays.asList(new FieldSchema("status", "INTEGER"));
        ProtobufCodec codec = codec(schemas);

        byte[] encoded = codec.encode(new Object[]{1});

        assertEquals(RedisAllTypes.Status.ACTIVE, RedisAllTypes.parseFrom(encoded).getStatus());
        assertEquals(1, codec.decode(encoded, "ignored-primary-key")[0]);
    }

    @Test
    @SuppressWarnings("unchecked")
    void mapEnumValuesFollowDeclaredSqlValueType() throws Exception {
        Map<String, String> values = Collections.singletonMap("primary", "ACTIVE");
        ProtobufCodec stringCodec = codec(Collections.singletonList(
                new FieldSchema("status_by_name", "MAP<VARCHAR, VARCHAR>")));
        byte[] encoded = stringCodec.encode(new Object[]{values});

        assertEquals(RedisAllTypes.Status.ACTIVE,
                RedisAllTypes.parseFrom(encoded).getStatusByNameOrThrow("primary"));
        Map<String, Object> decoded = (Map<String, Object>) stringCodec.decode(encoded, "key")[0];
        assertEquals("ACTIVE", decoded.get("primary"));

        ProtobufCodec numericCodec = codec(Collections.singletonList(
                new FieldSchema("status_by_name", "MAP<VARCHAR, INTEGER>")));
        assertEquals(1, ((Map<?, ?>) numericCodec.decode(encoded, "key")[0]).get("primary"));

        ProtobufCodec notNullCodec = codec(Collections.singletonList(
                new FieldSchema("status_by_name", "MAP<STRING NOT NULL, STRING NOT NULL> NOT NULL")));
        assertEquals("ACTIVE", ((Map<?, ?>) notNullCodec.decode(encoded, "key")[0]).get("primary"));

        ProtobufCodec notNullArrayCodec = codec(Collections.singletonList(
                new FieldSchema("repeated_status", "ARRAY<STRING NOT NULL> NOT NULL")));
        byte[] arrayBytes = RedisAllTypes.newBuilder()
                .addRepeatedStatus(RedisAllTypes.Status.ACTIVE).build().toByteArray();
        assertEquals(Collections.singletonList("ACTIVE"), notNullArrayCodec.decode(arrayBytes, "key")[0]);
    }

    private static ProtobufCodec codec(List<FieldSchema> schemas) {
        ProtobufCodec codec = new ProtobufCodec(MESSAGE_CLASS_NAME);
        codec.init(schemas, 0);
        return codec;
    }

    private static List<FieldSchema> allFieldSchemas() {
        return Arrays.asList(
                new FieldSchema("double_value", "DOUBLE"),
                new FieldSchema("float_value", "FLOAT"),
                new FieldSchema("int32_value", "INTEGER"),
                new FieldSchema("int64_value", "BIGINT"),
                new FieldSchema("uint32_value", "INTEGER"),
                new FieldSchema("uint64_value", "BIGINT"),
                new FieldSchema("sint32_value", "INTEGER"),
                new FieldSchema("sint64_value", "BIGINT"),
                new FieldSchema("fixed32_value", "INTEGER"),
                new FieldSchema("fixed64_value", "BIGINT"),
                new FieldSchema("sfixed32_value", "INTEGER"),
                new FieldSchema("sfixed64_value", "BIGINT"),
                new FieldSchema("bool_value", "BOOLEAN"),
                new FieldSchema("string_value", "VARCHAR"),
                new FieldSchema("bytes_value", "VARBINARY"),
                new FieldSchema("status", "VARCHAR"),
                new FieldSchema("nested", "ROW"),
                new FieldSchema("repeated_int32", "ARRAY<INTEGER>"),
                new FieldSchema("repeated_string", "ARRAY<VARCHAR>"),
                new FieldSchema("repeated_bytes", "ARRAY<VARBINARY>"),
                new FieldSchema("repeated_status", "ARRAY<VARCHAR>"),
                new FieldSchema("repeated_nested", "ARRAY<ROW>"),
                new FieldSchema("attributes", "MAP<VARCHAR, BIGINT>"),
                new FieldSchema("nested_by_name", "MAP<VARCHAR, ROW>"),
                new FieldSchema("optional_string", "VARCHAR"),
                new FieldSchema("choice_string", "VARCHAR"));
    }

    private static Object[] allFieldValues() {
        Map<String, Object> nested = nested(101L, "child", true);
        Map<String, Object> secondNested = nested(102L, "second", false);

        Map<String, Long> attributes = new LinkedHashMap<>();
        attributes.put("score", 9001L);
        attributes.put("rank", 7L);

        Map<String, Map<String, Object>> nestedByName = new LinkedHashMap<>();
        nestedByName.put("first", nested);
        nestedByName.put("second", secondNested);

        return new Object[]{
                3.25D,
                1.5F,
                -12,
                -13L,
                14,
                15L,
                -16,
                -17L,
                18,
                19L,
                -20,
                -21L,
                true,
                "中文-protobuf",
                new byte[]{0, 1, (byte) 0xFF},
                "ACTIVE",
                nested,
                Arrays.asList(1, 2, 3),
                Arrays.asList("one", "two"),
                Arrays.asList(new byte[]{4, 5}, new byte[]{6, 7}),
                Arrays.asList("ACTIVE", "DISABLED"),
                Arrays.asList(nested, secondNested),
                attributes,
                nestedByName,
                "optional-value",
                "selected-choice"
        };
    }

    private static Map<String, Object> nested(long id, String name, boolean enabled) {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("id", id);
        nested.put("name", name);
        nested.put("enabled", enabled);
        return nested;
    }

    private static void assertGeneratedMessage(RedisAllTypes message) {
        assertEquals(3.25D, message.getDoubleValue());
        assertEquals(1.5F, message.getFloatValue());
        assertEquals(-12, message.getInt32Value());
        assertEquals(-13L, message.getInt64Value());
        assertEquals(14, message.getUint32Value());
        assertEquals(15L, message.getUint64Value());
        assertEquals(-16, message.getSint32Value());
        assertEquals(-17L, message.getSint64Value());
        assertEquals(18, message.getFixed32Value());
        assertEquals(19L, message.getFixed64Value());
        assertEquals(-20, message.getSfixed32Value());
        assertEquals(-21L, message.getSfixed64Value());
        assertTrue(message.getBoolValue());
        assertEquals("中文-protobuf", message.getStringValue());
        assertEquals(ByteString.copyFrom(new byte[]{0, 1, (byte) 0xFF}), message.getBytesValue());
        assertEquals(RedisAllTypes.Status.ACTIVE, message.getStatus());
        assertEquals(101L, message.getNested().getId());
        assertEquals("child", message.getNested().getName());
        assertTrue(message.getNested().getEnabled());
        assertEquals(Arrays.asList(1, 2, 3), message.getRepeatedInt32List());
        assertEquals(Arrays.asList("one", "two"), message.getRepeatedStringList());
        assertEquals(Arrays.asList(
                ByteString.copyFrom(new byte[]{4, 5}),
                ByteString.copyFrom(new byte[]{6, 7})), message.getRepeatedBytesList());
        assertEquals(Arrays.asList(
                RedisAllTypes.Status.ACTIVE,
                RedisAllTypes.Status.DISABLED), message.getRepeatedStatusList());
        assertEquals(2, message.getRepeatedNestedCount());
        assertEquals(9001L, message.getAttributesOrThrow("score"));
        assertEquals("second", message.getNestedByNameOrThrow("second").getName());
        assertTrue(message.hasOptionalString());
        assertEquals("optional-value", message.getOptionalString());
        assertEquals(RedisAllTypes.ChoiceCase.CHOICE_STRING, message.getChoiceCase());
        assertEquals("selected-choice", message.getChoiceString());
    }

    @SuppressWarnings("unchecked")
    private static void assertDecodedRow(Object[] row) {
        assertEquals(3.25D, row[0]);
        assertEquals(1.5F, row[1]);
        assertEquals(-12, row[2]);
        assertEquals(-13L, row[3]);
        assertEquals(14, row[4]);
        assertEquals(15L, row[5]);
        assertEquals(-16, row[6]);
        assertEquals(-17L, row[7]);
        assertEquals(18, row[8]);
        assertEquals(19L, row[9]);
        assertEquals(-20, row[10]);
        assertEquals(-21L, row[11]);
        assertEquals(true, row[12]);
        assertEquals("中文-protobuf", row[13]);
        assertArrayEquals(new byte[]{0, 1, (byte) 0xFF}, (byte[]) row[14]);
        assertEquals("ACTIVE", row[15]);

        Map<String, Object> nested = (Map<String, Object>) row[16];
        assertEquals(101L, nested.get("id"));
        assertEquals("child", nested.get("name"));
        assertEquals(true, nested.get("enabled"));

        assertEquals(Arrays.asList(1, 2, 3), row[17]);
        assertEquals(Arrays.asList("one", "two"), row[18]);
        assertByteArrayListEquals(
                Arrays.asList(new byte[]{4, 5}, new byte[]{6, 7}),
                (List<byte[]>) row[19]);
        assertEquals(Arrays.asList("ACTIVE", "DISABLED"), row[20]);

        List<Map<String, Object>> repeatedNested = (List<Map<String, Object>>) row[21];
        assertEquals(2, repeatedNested.size());
        assertEquals("second", repeatedNested.get(1).get("name"));

        Map<String, Long> attributes = (Map<String, Long>) row[22];
        assertEquals(9001L, attributes.get("score"));
        assertEquals(7L, attributes.get("rank"));

        Map<String, Map<String, Object>> nestedByName =
                (Map<String, Map<String, Object>>) row[23];
        assertEquals(101L, nestedByName.get("first").get("id"));
        assertEquals("second", nestedByName.get("second").get("name"));
        assertEquals("optional-value", row[24]);
        assertEquals("selected-choice", row[25]);
    }

    private static void assertByteArrayListEquals(List<byte[]> expected, List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i), actual.get(i));
        }
    }
}
