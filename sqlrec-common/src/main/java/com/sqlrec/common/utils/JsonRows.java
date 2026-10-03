package com.sqlrec.common.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sqlrec.common.schema.FieldSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Reads JSON objects as ordered rows, with conversion rules selected at the input boundary. */
public final class JsonRows {
    public enum Decoding {
        /** Preserve Gson's Java values (including LONG_OR_DOUBLE numbers) without coercion. */
        RAW,
        /** Parse scalar text using declared types, keeping invalid cells null and complex values as JSON text. */
        COERCE_SCALARS,
        /** Match inferred JSON types; incompatible numeric/boolean values become null. */
        MATCH_SCHEMA
    }

    private JsonRows() {
    }

    /** Accepts one object or an array; non-object array entries are skipped. */
    public static List<JsonObject> readObjects(String json) {
        JsonElement root = JsonParser.parseString(json);
        List<JsonObject> objects = new ArrayList<>();
        if (root.isJsonObject()) {
            objects.add(root.getAsJsonObject());
        } else if (root.isJsonArray()) {
            for (JsonElement value : root.getAsJsonArray()) {
                if (value.isJsonObject()) {
                    objects.add(value.getAsJsonObject());
                }
            }
        } else {
            throw new IllegalArgumentException("json string must be a json object or json array");
        }
        return objects;
    }

    public static Object[] decode(String json, List<FieldSchema> fields, Decoding decoding) {
        if (decoding == Decoding.RAW) {
            // Decode the complete object to retain Gson's duplicate-key validation.
            Map<?, ?> values = JsonUtils.fromJson(json, Map.class);
            return RowTransformUtils.projectRow(fields, field -> values.get(field.getName()));
        }
        return decode(JsonParser.parseString(json).getAsJsonObject(), fields, decoding);
    }

    public static Object[] decode(JsonObject object, List<FieldSchema> fields, Decoding decoding) {
        return RowTransformUtils.projectRow(fields,
                field -> decodeValue(object.get(field.getName()), field.getType(), decoding));
    }

    public static List<Object[]> decodeRows(
            List<JsonObject> objects, List<FieldSchema> fields, Decoding decoding) {
        List<Object[]> rows = new ArrayList<>(objects.size());
        for (JsonObject object : objects) {
            rows.add(decode(object, fields, decoding));
        }
        return rows;
    }

    private static Object decodeValue(JsonElement value, String type, Decoding decoding) {
        if (value == null || value.isJsonNull()) {
            return null;
        }
        switch (decoding) {
            case RAW:
                return JsonUtils.getGson().fromJson(value, Object.class);
            case COERCE_SCALARS:
                try {
                    return value.isJsonPrimitive()
                            ? ScalarConversions.convert(value.getAsString(), type) : value.toString();
                } catch (RuntimeException failure) {
                    return null;
                }
            case MATCH_SCHEMA:
                return matchSchema(value, type);
            default:
                throw new IllegalArgumentException("Unsupported JSON decoding: " + decoding);
        }
    }

    private static Object matchSchema(JsonElement value, String type) {
        if (type.startsWith("ARRAY<")) {
            if (!value.isJsonArray()) {
                return null;
            }
            String elementType = type.substring("ARRAY<".length(), type.length() - 1);
            List<Object> elements = new ArrayList<>(value.getAsJsonArray().size());
            for (JsonElement element : value.getAsJsonArray()) {
                elements.add(decodeValue(element, elementType, Decoding.MATCH_SCHEMA));
            }
            return elements;
        }
        if ("BOOLEAN".equals(type) || "DOUBLE".equals(type)) {
            if (!value.isJsonPrimitive()) {
                return null;
            }
            boolean matches = "BOOLEAN".equals(type)
                    ? value.getAsJsonPrimitive().isBoolean() : value.getAsJsonPrimitive().isNumber();
            return matches ? ScalarConversions.convert(value.getAsString(), type) : null;
        }
        return value.isJsonPrimitive() ? value.getAsString() : value.toString();
    }
}
