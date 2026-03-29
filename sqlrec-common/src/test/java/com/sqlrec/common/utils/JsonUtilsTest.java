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

public class JsonUtilsTest {
    private static final Gson gson = new Gson();

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

        String json = JsonUtils.toJsonArray(data, inputFields, dataFields);
        assertNotNull(json);

        Type listType = new TypeToken<List<Map<String, Object>>>(){}.getType();
        List<Map<String, Object>> result = gson.fromJson(json, listType);
        
        assertEquals(3, result.size());
        
        Map<String, Object> firstRow = result.get(0);
        assertEquals(1.0, firstRow.get("id"));
        assertEquals("Zhang San", firstRow.get("name"));
        assertEquals(20.0, firstRow.get("age"));
        assertEquals(true, firstRow.get("active"));
        
        Map<String, Object> secondRow = result.get(1);
        assertEquals(2.0, secondRow.get("id"));
        assertEquals("Li Si", secondRow.get("name"));
        assertEquals(21.0, secondRow.get("age"));
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

        String json = JsonUtils.toJsonArray(data, inputFields, dataFields);
        assertNotNull(json);

        Type listType = new TypeToken<List<Map<String, Object>>>(){}.getType();
        List<Map<String, Object>> result = gson.fromJson(json, listType);
        
        assertEquals(1, result.size());
        Map<String, Object> row = result.get(0);
        assertEquals(1.0, row.get("id"));
        assertFalse(row.containsKey("name"));
        assertEquals(20.0, row.get("age"));
        assertFalse(row.containsKey("active"));
    }

    @Test
    public void testToColumnarJson() {
        List<Object[]> queryData = new ArrayList<>();
        queryData.add(new Object[]{"user123", "Beijing", true});

        List<Object[]> valueData = new ArrayList<>();
        valueData.add(new Object[]{1, "Product A", 99.9});
        valueData.add(new Object[]{2, "Product B", 199.9});
        valueData.add(new Object[]{3, "Product C", 299.9});

        List<FieldSchema> queryFields = new ArrayList<>();
        queryFields.add(new FieldSchema("user_id", "string"));
        queryFields.add(new FieldSchema("user_city", "string"));
        queryFields.add(new FieldSchema("is_vip", "boolean"));

        List<FieldSchema> valueFields = new ArrayList<>();
        valueFields.add(new FieldSchema("item_id", "int"));
        valueFields.add(new FieldSchema("item_name", "string"));
        valueFields.add(new FieldSchema("price", "double"));

        List<RelDataTypeField> queryDataFields = createQueryDataFields();
        List<RelDataTypeField> valueDataFields = createValueDataFields();

        String json = JsonUtils.toColumnarJson(queryData, valueData, queryFields, valueFields, 
                                                queryDataFields, valueDataFields);
        assertNotNull(json);

        Type mapType = new TypeToken<Map<String, List<Object>>>(){}.getType();
        Map<String, List<Object>> result = gson.fromJson(json, mapType);
        
        assertTrue(result.containsKey("user_id"));
        assertTrue(result.containsKey("user_city"));
        assertTrue(result.containsKey("is_vip"));
        assertTrue(result.containsKey("item_id"));
        assertTrue(result.containsKey("item_name"));
        assertTrue(result.containsKey("price"));
        
        assertEquals(1, result.get("user_id").size());
        assertEquals("user123", result.get("user_id").get(0));
        assertEquals("Beijing", result.get("user_city").get(0));
        assertEquals(true, result.get("is_vip").get(0));
        
        assertEquals(3, result.get("item_id").size());
        assertEquals(1.0, result.get("item_id").get(0));
        assertEquals(2.0, result.get("item_id").get(1));
        assertEquals(3.0, result.get("item_id").get(2));
        
        assertEquals("Product A", result.get("item_name").get(0));
        assertEquals("Product B", result.get("item_name").get(1));
        assertEquals("Product C", result.get("item_name").get(2));
        
        assertEquals(99.9, (Double)result.get("price").get(0), 0.001);
        assertEquals(199.9, (Double)result.get("price").get(1), 0.001);
        assertEquals(299.9, (Double)result.get("price").get(2), 0.001);
    }

    @Test
    public void testToColumnarJsonWithNull() {
        List<Object[]> queryData = new ArrayList<>();
        queryData.add(new Object[]{"user123", null, true});

        List<Object[]> valueData = new ArrayList<>();
        valueData.add(new Object[]{1, null, 99.9});
        valueData.add(new Object[]{2, "Product B", null});

        List<FieldSchema> queryFields = new ArrayList<>();
        queryFields.add(new FieldSchema("user_id", "string"));
        queryFields.add(new FieldSchema("user_city", "string"));
        queryFields.add(new FieldSchema("is_vip", "boolean"));

        List<FieldSchema> valueFields = new ArrayList<>();
        valueFields.add(new FieldSchema("item_id", "int"));
        valueFields.add(new FieldSchema("item_name", "string"));
        valueFields.add(new FieldSchema("price", "double"));

        List<RelDataTypeField> queryDataFields = createQueryDataFields();
        List<RelDataTypeField> valueDataFields = createValueDataFields();

        String json = JsonUtils.toColumnarJson(queryData, valueData, queryFields, valueFields, 
                                                queryDataFields, valueDataFields);
        assertNotNull(json);

        Type mapType = new TypeToken<Map<String, List<Object>>>(){}.getType();
        Map<String, List<Object>> result = gson.fromJson(json, mapType);
        
        assertEquals(1, result.get("user_city").size());
        assertNull(result.get("user_city").get(0));
        
        assertNull(result.get("item_name").get(0));
        assertEquals("Product B", result.get("item_name").get(1));
        assertEquals(99.9, (Double)result.get("price").get(0), 0.001);
        assertNull(result.get("price").get(1));
    }

    private List<RelDataTypeField> createTestDataFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("name", 1, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("age", 2, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("active", 3, SqlTypeName.BOOLEAN));
        return fields;
    }

    private List<RelDataTypeField> createQueryDataFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("user_id", 0, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("user_city", 1, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("is_vip", 2, SqlTypeName.BOOLEAN));
        return fields;
    }

    private List<RelDataTypeField> createValueDataFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("item_id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("item_name", 1, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("price", 2, SqlTypeName.DOUBLE));
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

        String json = JsonUtils.toJsonArray(data, inputFields, dataFields);
        assertNotNull(json);

        Type listType = new TypeToken<List<Map<String, Object>>>(){}.getType();
        List<Map<String, Object>> result = gson.fromJson(json, listType);
        
        assertEquals(2, result.size());
        
        Map<String, Object> firstRow = result.get(0);
        assertEquals(1.0, firstRow.get("id"));
        assertEquals("Zhang San", firstRow.get("name"));
        assertTrue(firstRow.get("tags") instanceof List);
        assertEquals(3, ((List<?>)firstRow.get("tags")).size());
        
        Map<String, Object> secondRow = result.get(1);
        assertEquals(2.0, secondRow.get("id"));
        assertEquals("Li Si", secondRow.get("name"));
        assertTrue(secondRow.get("tags") instanceof List);
        assertEquals(2, ((List<?>)secondRow.get("tags")).size());
    }

    @Test
    public void testToColumnarJsonWithList() {
        List<Object[]> queryData = new ArrayList<>();
        queryData.add(new Object[]{"user123", Arrays.asList("tag1", "tag2")});

        List<Object[]> valueData = new ArrayList<>();
        valueData.add(new Object[]{1, Arrays.asList(1.1, 2.2)});
        valueData.add(new Object[]{2, Arrays.asList(3.3, 4.4, 5.5)});

        List<FieldSchema> queryFields = new ArrayList<>();
        queryFields.add(new FieldSchema("user_id", "string"));
        queryFields.add(new FieldSchema("user_tags", "array<string>"));

        List<FieldSchema> valueFields = new ArrayList<>();
        valueFields.add(new FieldSchema("item_id", "int"));
        valueFields.add(new FieldSchema("scores", "array<double>"));

        List<RelDataTypeField> queryDataFields = createQueryDataFieldsWithArray();
        List<RelDataTypeField> valueDataFields = createValueDataFieldsWithArray();

        String json = JsonUtils.toColumnarJson(queryData, valueData, queryFields, valueFields, 
                                                queryDataFields, valueDataFields);
        assertNotNull(json);

        Type mapType = new TypeToken<Map<String, List<Object>>>(){}.getType();
        Map<String, List<Object>> result = gson.fromJson(json, mapType);
        
        assertTrue(result.containsKey("user_id"));
        assertTrue(result.containsKey("user_tags"));
        assertTrue(result.containsKey("item_id"));
        assertTrue(result.containsKey("scores"));
        
        assertEquals(1, result.get("user_id").size());
        assertEquals("user123", result.get("user_id").get(0));
        assertTrue(result.get("user_tags").get(0) instanceof List);
        assertEquals(2, ((List<?>)result.get("user_tags").get(0)).size());
        
        assertEquals(2, result.get("item_id").size());
        assertEquals(1.0, result.get("item_id").get(0));
        assertEquals(2.0, result.get("item_id").get(1));
        
        assertTrue(result.get("scores").get(0) instanceof List);
        assertEquals(2, ((List<?>)result.get("scores").get(0)).size());
        assertTrue(result.get("scores").get(1) instanceof List);
        assertEquals(3, ((List<?>)result.get("scores").get(1)).size());
    }

    private List<RelDataTypeField> createTestDataFieldsWithArray() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("name", 1, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("tags", 2, SqlTypeName.ARRAY));
        return fields;
    }

    private List<RelDataTypeField> createQueryDataFieldsWithArray() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("user_id", 0, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("user_tags", 1, SqlTypeName.ARRAY));
        return fields;
    }

    private List<RelDataTypeField> createValueDataFieldsWithArray() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("item_id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("scores", 1, SqlTypeName.ARRAY));
        return fields;
    }
}
