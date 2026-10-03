package com.sqlrec.common.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Infers schemas from named values, keeping Java-response and JSON-table policies explicit. */
public final class SchemaInference {
    private SchemaInference() {
    }

    /** Uses the first row's columns and the first non-null value in each column. */
    public static List<RelDataTypeField> inferFields(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return new ArrayList<>();
        }
        List<FieldSchema> schema = fields(rows.get(0).keySet(), name -> inferColumnTypeName(rows, name));
        return new ArrayList<>(DataTypeUtils.getRelDataType(
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT), schema).getFieldList());
    }

    /** Uses all JSON object keys in encounter order; JSON numbers are always DOUBLE. */
    public static List<FieldSchema> inferJsonFields(List<JsonObject> objects) {
        Collection<String> names = new LinkedHashSet<>();
        for (JsonObject object : objects) {
            names.addAll(object.keySet());
        }
        return fields(names, name -> {
            for (JsonObject object : objects) {
                JsonElement value = object.get(name);
                if (value != null && !value.isJsonNull()) {
                    return inferJsonType(value);
                }
            }
            return "VARCHAR";
        });
    }

    private static List<FieldSchema> fields(
            Collection<String> names, Function<String, String> typeReader) {
        List<FieldSchema> fields = new ArrayList<>(names.size());
        for (String name : names) {
            fields.add(new FieldSchema(name, typeReader.apply(name)));
        }
        return fields;
    }

    private static String inferColumnTypeName(List<Map<String, Object>> rows, String name) {
        for (Map<String, Object> row : rows) {
            Object value = row.get(name);
            if (value != null) {
                return inferTypeName(value);
            }
        }
        return "VARCHAR";
    }

    private static String inferTypeName(Object value) {
        if (value instanceof Long || value instanceof Integer) {
            return "BIGINT";
        }
        if (value instanceof Number) {
            return "DOUBLE";
        }
        if (value instanceof Boolean) {
            return "BOOLEAN";
        }
        if (value instanceof List) {
            String elementType = inferListElementType((List<?>) value);
            return elementType == null ? "VARCHAR" : "ARRAY<" + elementType + ">";
        }
        return "VARCHAR";
    }

    private static String inferListElementType(List<?> values) {
        for (Object value : values) {
            if (value != null) {
                // Java responses with empty or nested lists remain JSON text columns.
                return value instanceof Map || value instanceof List ? null : inferTypeName(value);
            }
        }
        return null;
    }

    private static String inferJsonType(JsonElement value) {
        if (value.isJsonArray()) {
            for (JsonElement element : value.getAsJsonArray()) {
                if (element.isJsonPrimitive()) {
                    return "ARRAY<" + inferJsonType(element) + ">";
                }
            }
            return "ARRAY<VARCHAR>";
        }
        if (value.isJsonPrimitive()) {
            JsonPrimitive primitive = value.getAsJsonPrimitive();
            if (primitive.isNumber()) {
                return "DOUBLE";
            }
            return primitive.isBoolean() ? "BOOLEAN" : "VARCHAR";
        }
        return "VARCHAR";
    }
}
