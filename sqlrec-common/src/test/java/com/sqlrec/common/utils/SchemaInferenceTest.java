package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class SchemaInferenceTest {
    @Test
    void mixedNumericResponsesWidenAcrossRowsAndArrayElements() {
        List<RelDataTypeField> fields = SchemaInference.inferFields(List.of(
                Map.of("score", 1L, "scores", List.of(1L)),
                Map.of("score", 0.75d, "scores", List.of(1L, 0.5d))));
        assertEquals(SqlTypeName.DOUBLE, fields.stream().filter(f -> f.getName().equals("score"))
                .findFirst().orElseThrow().getType().getSqlTypeName());
        assertEquals(SqlTypeName.DOUBLE, fields.stream().filter(f -> f.getName().equals("scores"))
                .findFirst().orElseThrow().getType().getComponentType().getSqlTypeName());
    }

    @Test
    void jsonArrayInferenceUsesLaterRowsAndRecognizesNestedArrays() {
        List<FieldSchema> fields = SchemaInference.inferJsonFields(JsonRows.readObjects(
                "[{\"xs\":[],\"matrix\":[]},{\"xs\":[1,0.75],\"matrix\":[[1],[2.5]]}]"));
        assertEquals("ARRAY<DOUBLE>", fields.get(0).getType());
        assertEquals("ARRAY<ARRAY<DOUBLE>>", fields.get(1).getType());
    }

    @Test
    void jsonIntegersWithinLongRangeUseBigintIncludingNestedArrays() {
        List<FieldSchema> fields = SchemaInference.inferJsonFields(JsonRows.readObjects(
                "[{\"id\":null,\"ids\":[],\"matrix\":[]},"
                        + "{\"id\":9007199254740993,\"ids\":[-9223372036854775808,9223372036854775807],"
                        + "\"matrix\":[[1],[2]]}]"));
        assertEquals(Arrays.asList("BIGINT", "ARRAY<BIGINT>", "ARRAY<ARRAY<BIGINT>>"),
                fields.stream().map(FieldSchema::getType).collect(Collectors.toList()));
    }

    @Test
    void jsonNumbersWidenAcrossRowsAndArraysRegardlessOfOrder() {
        for (String[] pair : new String[][]{{"1", "0.75"}, {"0.75", "1"}}) {
            List<FieldSchema> fields = SchemaInference.inferJsonFields(JsonRows.readObjects(
                    "[{\"score\":" + pair[0] + ",\"scores\":[" + pair[0] + "],"
                            + "\"matrix\":[[" + pair[0] + "]]},"
                            + "{\"score\":" + pair[1] + ",\"scores\":[" + pair[1] + "],"
                            + "\"matrix\":[[" + pair[1] + "]]}]"));
            assertEquals(Arrays.asList("DOUBLE", "ARRAY<DOUBLE>", "ARRAY<ARRAY<DOUBLE>>"),
                    fields.stream().map(FieldSchema::getType).collect(Collectors.toList()));
        }
    }

    @Test
    void jsonFractionalExponentAndOutOfRangeNumbersUseDouble() {
        for (String number : List.of("1.0", "1e3", "-2E-3", "9223372036854775808", "-9223372036854775809")) {
            List<FieldSchema> fields = SchemaInference.inferJsonFields(JsonRows.readObjects(
                    "[{\"value\":0,\"values\":[0]},{\"value\":" + number + ",\"values\":[" + number + "]}]"));
            assertEquals("DOUBLE", fields.get(0).getType(), number);
            assertEquals("ARRAY<DOUBLE>", fields.get(1).getType(), number);
        }
    }

    @Test
    void javaResponsesUseFirstRowKeysAndPreserveIntegralAndArrayPolicies() {
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("id", null);
        first.put("empty", Collections.emptyList());
        first.put("nested", Arrays.asList(null, Collections.singletonMap("id", 1), 2));
        first.put("values", Arrays.asList(null, 1, 2));
        Map<String, Object> second = new LinkedHashMap<>(first);
        second.put("id", 9007199254740993L);
        second.put("late", true);
        List<RelDataTypeField> fields = SchemaInference.inferFields(Arrays.asList(first, second));
        assertEquals(Arrays.asList("id", "empty", "nested", "values"),
                fields.stream().map(RelDataTypeField::getName).collect(Collectors.toList()));
        assertEquals(Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.ARRAY),
                fields.stream().map(field -> field.getType().getSqlTypeName()).collect(Collectors.toList()));
        assertEquals(SqlTypeName.BIGINT, fields.get(3).getType().getComponentType().getSqlTypeName());
    }

    @Test
    void jsonTablesUseAllKeysAndFirstPrimitiveArrayElements() {
        List<FieldSchema> fields = SchemaInference.inferJsonFields(JsonRows.readObjects(
                "[{\"id\":1,\"empty\":[],\"nested\":[{},[1],null,2],\"flag\":null},{\"flag\":true,\"late\":false}]"));
        assertEquals(Arrays.asList("id", "empty", "nested", "flag", "late"),
                fields.stream().map(FieldSchema::getName).collect(Collectors.toList()));
        assertEquals(Arrays.asList("BIGINT", "ARRAY<VARCHAR>", "ARRAY<BIGINT>", "BOOLEAN", "BOOLEAN"),
                fields.stream().map(FieldSchema::getType).collect(Collectors.toList()));
    }

    @Test
    void emptyAndAllNullInputsRetainTheirExistingFallbacks() {
        assertTrue(SchemaInference.inferFields(null).isEmpty());
        assertTrue(SchemaInference.inferFields(Collections.emptyList()).isEmpty());
        assertTrue(SchemaInference.inferJsonFields(Collections.emptyList()).isEmpty());
        assertEquals(SqlTypeName.VARCHAR, SchemaInference.inferFields(
                Collections.singletonList(Collections.singletonMap("value", null)))
                .get(0).getType().getSqlTypeName());
    }
}
