package com.sqlrec.common.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Decodes named JSON values using their declared schema; unconvertible cells become null. */
public final class JsonRows {
    private static final Type OBJECT_TYPE = new TypeToken<Map<String, JsonElement>>() {}.getType();

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

    public static Object[] decode(String json, List<FieldSchema> fields) {
        return decode(json, resolveSchema(fields));
    }

    /** Reuses a schema resolved once by a connector during initialization. */
    public static Object[] decode(String json, RelDataType schema) {
        // JsonElement values retain numeric text and make even duplicate null-valued keys detectable.
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            if (reader.peek() != JsonToken.BEGIN_OBJECT) throw new JsonSyntaxException("expected JSON object");
            Map<String, JsonElement> object = JsonUtils.getGson().fromJson(reader, OBJECT_TYPE);
            if (reader.peek() != JsonToken.END_DOCUMENT) throw new JsonSyntaxException("unexpected content after JSON object");
            return decode(object, schema);
        } catch (IOException invalidJson) {
            throw new JsonSyntaxException(invalidJson);
        }
    }

    public static Object[] decode(JsonObject object, List<FieldSchema> fields) {
        return decode(object.asMap(), resolveSchema(fields));
    }

    public static List<Object[]> decodeRows(List<JsonObject> objects, List<FieldSchema> fields) {
        RelDataType schema = resolveSchema(fields);
        List<Object[]> rows = new ArrayList<>(objects.size());
        for (JsonObject object : objects) {
            rows.add(decode(object.asMap(), schema));
        }
        return rows;
    }

    private static RelDataType resolveSchema(List<FieldSchema> fields) {
        return DataTypeUtils.getRelDataType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT), fields);
    }

    /** Accepts parsed JSON fields or native values from an API response without a JSON round trip. */
    public static Object[] decode(Map<String, ?> object, RelDataType schema) {
        return RowTransformUtils.projectRow(schema.getFieldList(),
                field -> decodeValue(object.get(field.getName()), field.getType()));
    }

    private static Object decodeValue(Object value, RelDataType target) {
        if (target.getSqlTypeName() == SqlTypeName.ANY) {
            return value instanceof JsonElement ? JsonUtils.getGson().fromJson((JsonElement) value, Object.class) : value;
        }
        Object original = value;
        value = unwrap(value);
        if (value == null) return null;
        try {
            switch (target.getSqlTypeName()) {
                case ARRAY:
                    return decodeArray(value, target);
                case MAP:
                    return decodeMap(value, target);
                case ROW:
                    return decodeRow(value, target);
                case VARCHAR:
                case CHAR:
                    return value instanceof Map || value instanceof List || value.getClass().isArray()
                            ? (original instanceof JsonElement ? original : JsonUtils.getGson().toJsonTree(value)).toString()
                            : value.toString();
                default:
                    return value instanceof Map || value instanceof List || value.getClass().isArray()
                            ? null : ScalarConversions.convert(value, target.getSqlTypeName());
            }
        } catch (RuntimeException invalidValue) {
            return null;
        }
    }

    private static List<Object> decodeArray(Object value, RelDataType target) {
        value = parseContainer(value);
        if (!(value instanceof List)) return null;
        List<Object> elements = new ArrayList<>(((List<?>) value).size());
        for (Object element : (List<?>) value) {
            elements.add(decodeValue(element, target.getComponentType()));
        }
        return elements;
    }

    private static Map<Object, Object> decodeMap(Object value, RelDataType target) {
        value = parseContainer(value);
        if (!(value instanceof Map)) return null;
        Map<Object, Object> entries = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            Object key = decodeValue(entry.getKey(), target.getKeyType());
            if (key == null || entries.containsKey(key)) return null;
            entries.put(key, decodeValue(entry.getValue(), target.getValueType()));
        }
        return entries;
    }

    private static Map<String, Object> decodeRow(Object value, RelDataType target) {
        value = parseContainer(value);
        if (!(value instanceof Map)) return null;
        Map<String, Object> row = new LinkedHashMap<>();
        for (var field : target.getFieldList()) {
            row.put(field.getName(), decodeValue(((Map<?, ?>) value).get(field.getName()), field.getType()));
        }
        return row;
    }

    private static Object unwrap(Object value) {
        if (!(value instanceof JsonElement)) return value;
        JsonElement json = (JsonElement) value;
        if (json.isJsonNull()) return null;
        if (json.isJsonPrimitive()) {
            JsonPrimitive primitive = json.getAsJsonPrimitive();
            // Gson's Number retains the literal text, including large integers and decimal scale.
            if (primitive.isNumber()) return primitive.getAsNumber();
            if (primitive.isBoolean()) return primitive.getAsBoolean();
            return primitive.getAsString();
        }
        return json.isJsonArray() ? json.getAsJsonArray().asList() : json.getAsJsonObject().asMap();
    }

    private static Object parseContainer(Object value) {
        if (value.getClass().isArray()) return JsonUtils.getGson().toJsonTree(value).getAsJsonArray().asList();
        return value instanceof String ? unwrap(JsonParser.parseString((String) value)) : value;
    }
}
