package com.sqlrec.common.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RowJsonEncoderTest {
    private static final Gson gson = JsonUtils.getGson();

    @Test
    public void testToJsonArray() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Zhang San", 20, true});
        data.add(new Object[]{2, "Li Si", 21, false});
        data.add(new Object[]{3, "Wang Wu", 22, true});

        List<FieldSchema> inputFields = new ArrayList<>();
        inputFields.add(new FieldSchema("id", "int"));
        inputFields.add(new FieldSchema("name", "string"));
        inputFields.add(new FieldSchema("age", "int"));
        inputFields.add(new FieldSchema("active", "boolean"));

        List<RelDataTypeField> dataFields = createTestDataFields();

        String json = RowJsonEncoder.toJsonArray(data, inputFields, dataFields);
        assertNotNull(json);

        Type listType = new TypeToken<List<Map<String, Object>>>(){}.getType();
        List<Map<String, Object>> result = gson.fromJson(json, listType);
        
        assertEquals(3, result.size());
        
        Map<String, Object> firstRow = result.get(0);
        assertEquals(1L, firstRow.get("id"));
        assertEquals("Zhang San", firstRow.get("name"));
        assertEquals(20L, firstRow.get("age"));
        assertEquals(true, firstRow.get("active"));
        
        Map<String, Object> secondRow = result.get(1);
        assertEquals(2L, secondRow.get("id"));
        assertEquals("Li Si", secondRow.get("name"));
        assertEquals(21L, secondRow.get("age"));
        assertEquals(false, secondRow.get("active"));
    }

    @Test
    public void testToJsonArrayWithNull() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, null, 20, null});

        List<FieldSchema> inputFields = new ArrayList<>();
        inputFields.add(new FieldSchema("id", "int"));
        inputFields.add(new FieldSchema("name", "string"));
        inputFields.add(new FieldSchema("age", "int"));
        inputFields.add(new FieldSchema("active", "boolean"));

        List<RelDataTypeField> dataFields = createTestDataFields();

        String json = RowJsonEncoder.toJsonArray(data, inputFields, dataFields);
        assertNotNull(json);

        Type listType = new TypeToken<List<Map<String, Object>>>(){}.getType();
        List<Map<String, Object>> result = gson.fromJson(json, listType);
        
        assertEquals(1, result.size());
        Map<String, Object> row = result.get(0);
        assertEquals(1L, row.get("id"));
        assertTrue(row.containsKey("name"));
        assertNull(row.get("name"));
        assertEquals(20L, row.get("age"));
        assertTrue(row.containsKey("active"));
        assertNull(row.get("active"));
    }

    @Test
    void preservesAnEntireNullFeatureInMapBatches() {
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("id", 1);
        row.put("feature", null);
        assertEquals("[{\"id\":1,\"feature\":null},{\"id\":1,\"feature\":null}]",
                RowJsonEncoder.toJsonArray(List.of(row, row)));
    }

    @Test
    void nonPredictionRowFormatsKeepOmittingNullAttributes() {
        assertEquals("{\"id\":1}", RowJsonEncoder.toJson(new Object[]{1, null},
                List.of(new FieldSchema("id", "INT"), new FieldSchema("country", "STRING"))));
        assertEquals("{\"id\":1}", RowJsonEncoder.toJsonByFields(new Object[]{1, null},
                List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER),
                        DataTypeUtils.getRelDataTypeField("country", 1, SqlTypeName.VARCHAR))));
    }

    private List<RelDataTypeField> createTestDataFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("name", 1, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("age", 2, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("active", 3, SqlTypeName.BOOLEAN));
        return fields;
    }

    @Test
    public void testToJsonArrayWithList() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Zhang San", Arrays.asList(1, 2, 3)});
        data.add(new Object[]{2, "Li Si", Arrays.asList(4, 5)});

        List<FieldSchema> inputFields = new ArrayList<>();
        inputFields.add(new FieldSchema("id", "int"));
        inputFields.add(new FieldSchema("name", "string"));
        inputFields.add(new FieldSchema("tags", "array<int>"));

        List<RelDataTypeField> dataFields = createTestDataFieldsWithArray();

        String json = RowJsonEncoder.toJsonArray(data, inputFields, dataFields);
        assertNotNull(json);

        Type listType = new TypeToken<List<Map<String, Object>>>(){}.getType();
        List<Map<String, Object>> result = gson.fromJson(json, listType);
        
        assertEquals(2, result.size());
        
        Map<String, Object> firstRow = result.get(0);
        assertEquals(1L, firstRow.get("id"));
        assertEquals("Zhang San", firstRow.get("name"));
        assertTrue(firstRow.get("tags") instanceof List);
        assertEquals(3, ((List<?>)firstRow.get("tags")).size());
        
        Map<String, Object> secondRow = result.get(1);
        assertEquals(2L, secondRow.get("id"));
        assertEquals("Li Si", secondRow.get("name"));
        assertTrue(secondRow.get("tags") instanceof List);
        assertEquals(2, ((List<?>)secondRow.get("tags")).size());
    }

    private List<RelDataTypeField> createTestDataFieldsWithArray() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("name", 1, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("tags", 2, SqlTypeName.ARRAY));
        return fields;
    }

}
