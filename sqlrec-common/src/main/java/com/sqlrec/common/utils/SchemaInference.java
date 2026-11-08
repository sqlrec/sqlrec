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

    /** Uses the first row's columns, widening mixed integral/floating values across rows. */
    public static List<RelDataTypeField> inferFields(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return new ArrayList<>();
        }
        List<FieldSchema> schema = fields(rows.get(0).keySet(), name -> {
            List<Object> values = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) values.add(row.get(name));
            return inferJavaType(values);
        });
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
            List<JsonElement> values = new ArrayList<>();
            for (JsonObject object : objects) {
                values.add(object.get(name));
            }
            return inferJsonValues(values);
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

    private static String inferJavaType(List<?> values) {
        for (Object value : values) {
            if (value == null) continue;
            if (value instanceof List) {
                List<Object> elements = new ArrayList<>();
                for (Object candidate : values) {
                    if (candidate instanceof List) elements.addAll((List<?>) candidate);
                }
                // Empty and nested Java lists retain their JSON-text fallback.
                for (Object element : elements) {
                    if (element == null) continue;
                    return element instanceof Map || element instanceof List ? "VARCHAR"
                            : "ARRAY<" + inferJavaType(elements) + ">";
                }
                return "VARCHAR";
            }
            if (value instanceof Number) {
                for (Object candidate : values) {
                    if (candidate instanceof Number && !(candidate instanceof Byte || candidate instanceof Short
                            || candidate instanceof Integer || candidate instanceof Long)) return "DOUBLE";
                }
                return "BIGINT";
            }
            return value instanceof Boolean ? "BOOLEAN" : "VARCHAR";
        }
        return "VARCHAR";
    }

    private static String inferJsonValues(List<JsonElement> values) {
        for (JsonElement value : values) {
            if (value == null || value.isJsonNull()) continue;
            if (!value.isJsonArray()) return inferJsonType(value);
            List<JsonElement> elements = new ArrayList<>();
            for (JsonElement candidate : values) {
                if (candidate != null && candidate.isJsonArray()) {
                    candidate.getAsJsonArray().forEach(elements::add);
                }
            }
            for (JsonElement element : elements) {
                if (element.isJsonPrimitive()) return "ARRAY<" + inferJsonType(element) + ">";
            }
            List<JsonElement> nested = new ArrayList<>();
            for (JsonElement element : elements) if (element.isJsonArray()) nested.add(element);
            return "ARRAY<" + (nested.isEmpty() ? "VARCHAR" : inferJsonValues(nested)) + ">";
        }
        return "VARCHAR";
    }

    private static String inferJsonType(JsonElement value) {
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
