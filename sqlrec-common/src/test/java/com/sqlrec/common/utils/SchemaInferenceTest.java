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
    void jsonTablesUseAllKeysDoubleNumbersAndFirstPrimitiveArrayElements() {
        List<FieldSchema> fields = SchemaInference.inferJsonFields(JsonRows.readObjects(
                "[{\"id\":1,\"empty\":[],\"nested\":[{},[1],null,2],\"flag\":null},{\"flag\":true,\"late\":false}]"));
        assertEquals(Arrays.asList("id", "empty", "nested", "flag", "late"),
                fields.stream().map(FieldSchema::getName).collect(Collectors.toList()));
        assertEquals(Arrays.asList("DOUBLE", "ARRAY<VARCHAR>", "ARRAY<DOUBLE>", "BOOLEAN", "BOOLEAN"),
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
