package com.sqlrec.common.utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Reads, infers the schema of, and encodes named rows used in JSON. */
public final class JsonRows {
    private static final Type OBJECT_TYPE = new TypeToken<Map<String, JsonElement>>() {}.getType();
    private static final Gson GSON = JsonUtils.getGson();
    private static final Gson REQUEST_GSON = GSON.newBuilder().serializeNulls().create();

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
            Map<String, JsonElement> object = GSON.fromJson(reader, OBJECT_TYPE);
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

    /** Accepts parsed JSON fields or native values from an API response without a JSON round trip. */
    public static Object[] decode(Map<String, ?> object, RelDataType schema) {
        return RowTransformUtils.projectRow(schema.getFieldList(),
                field -> decodeValue(object.get(field.getName()), field.getType()));
    }

    private static RelDataType resolveSchema(List<FieldSchema> fields) {
        return DataTypeUtils.getRelDataType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT), fields);
    }

    /** Decodes a cell using its declared type, yielding null if conversion fails. */
    private static Object decodeValue(Object value, RelDataType target) {
        if (target.getSqlTypeName() == SqlTypeName.ANY) {
            return value instanceof JsonElement ? GSON.fromJson((JsonElement) value, Object.class) : value;
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
                case VARCHAR:
                case CHAR:
                    return value instanceof Map || value instanceof List || value.getClass().isArray()
                            ? (original instanceof JsonElement ? original : GSON.toJsonTree(value)).toString()
                            : value.toString();
                default:
                    return value instanceof Map || value instanceof List || value.getClass().isArray()
                            ? null : RowTransformUtils.convertScalar(value, target.getSqlTypeName());
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
        if (value.getClass().isArray()) return GSON.toJsonTree(value).getAsJsonArray().asList();
        return value instanceof String ? unwrap(JsonParser.parseString((String) value)) : value;
    }

    /** Uses column names in encounter order and gives all-null columns the NULL type. */
    public static List<RelDataTypeField> inferFields(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return new ArrayList<>();
        }
        List<FieldSchema> schema = inferFields(rows, Map::keySet, name -> {
            List<Object> values = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                values.add(row.get(name));
            }
            return inferJavaType(values);
        });
        return new ArrayList<>(resolveSchema(schema).getFieldList());
    }

    /** Uses JSON keys in encounter order and widens mixed numeric literals to DOUBLE. */
    public static List<FieldSchema> inferJsonFields(List<JsonObject> objects) {
        return inferFields(objects, JsonObject::keySet, name -> {
            List<JsonElement> values = new ArrayList<>(objects.size());
            for (JsonObject object : objects) {
                values.add(object.get(name));
            }
            return inferJsonType(values);
        });
    }

    private static <T> List<FieldSchema> inferFields(
            List<T> rows, Function<T, Set<String>> getNames, Function<String, String> inferType) {
        Set<String> names = new LinkedHashSet<>();
        for (T row : rows) {
            names.addAll(getNames.apply(row));
        }
        List<FieldSchema> fields = new ArrayList<>(names.size());
        for (String name : names) {
            fields.add(new FieldSchema(name, inferType.apply(name)));
        }
        return fields;
    }

    private static String inferJavaType(List<?> values) {
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            if (value instanceof List) {
                List<Object> elements = new ArrayList<>();
                for (Object candidate : values) {
                    if (candidate instanceof List) {
                        elements.addAll((List<?>) candidate);
                    }
                }
                // Empty and nested Java lists retain their JSON-text fallback.
                for (Object element : elements) {
                    if (element != null) {
                        return element instanceof Map || element instanceof List ? "VARCHAR"
                                : "ARRAY<" + inferJavaType(elements) + ">";
                    }
                }
                return "VARCHAR";
            }
            if (value instanceof Number) {
                for (Object candidate : values) {
                    if (candidate instanceof Number && !(candidate instanceof Byte || candidate instanceof Short
                            || candidate instanceof Integer || candidate instanceof Long)) {
                        return "DOUBLE";
                    }
                }
                return "BIGINT";
            }
            return value instanceof Boolean ? "BOOLEAN" : "VARCHAR";
        }
        return "NULL";
    }

    private static String inferJsonType(List<JsonElement> values) {
        for (JsonElement value : values) {
            if (value == null || value.isJsonNull()) {
                continue;
            }
            if (!value.isJsonArray()) {
                return inferJsonValueType(value, values);
            }
            List<JsonElement> elements = new ArrayList<>();
            for (JsonElement candidate : values) {
                if (candidate != null && candidate.isJsonArray()) {
                    candidate.getAsJsonArray().forEach(elements::add);
                }
            }
            List<JsonElement> nested = new ArrayList<>();
            for (JsonElement element : elements) {
                if (element.isJsonPrimitive()) {
                    return "ARRAY<" + inferJsonValueType(element, elements) + ">";
                }
                if (element.isJsonArray()) {
                    nested.add(element);
                }
            }
            return "ARRAY<" + (nested.isEmpty() ? "VARCHAR" : inferJsonType(nested)) + ">";
        }
        return "VARCHAR";
    }

    private static String inferJsonValueType(JsonElement value, List<JsonElement> values) {
        if (!value.isJsonPrimitive()) {
            return "VARCHAR";
        }
        JsonPrimitive primitive = value.getAsJsonPrimitive();
        if (!primitive.isNumber()) {
            return primitive.isBoolean() ? "BOOLEAN" : "VARCHAR";
        }
        for (JsonElement candidate : values) {
            if (candidate == null || !candidate.isJsonPrimitive() || !candidate.getAsJsonPrimitive().isNumber()) {
                continue;
            }
            try {
                Long.parseLong(candidate.getAsString());
            } catch (NumberFormatException notBigint) {
                // Fractions, exponents and integers outside BIGINT use DOUBLE.
                return "DOUBLE";
            }
        }
        return "BIGINT";
    }

    /** Encodes one row, omitting null-valued fields. */
    public static String encodeRow(Object[] row, List<FieldSchema> fields) {
        return encodeRow(row, fields, FieldSchema::getName);
    }

    /** Encodes one Calcite row, omitting null-valued fields. */
    public static String encodeRowByFields(Object[] row, List<RelDataTypeField> fields) {
        return encodeRow(row, fields, RelDataTypeField::getName);
    }

    private static <F> String encodeRow(Object[] row, List<F> fields, Function<F, String> nameReader) {
        JsonObject object = new JsonObject();
        for (int i = 0; i < fields.size(); i++) {
            object.add(nameReader.apply(fields.get(i)), GSON.toJsonTree(row[i]));
        }
        return GSON.toJson(object);
    }

    /** Encodes named rows for a request, preserving null-valued fields. */
    public static String encodeRows(List<Map<String, Object>> rows) {
        JsonArray array = new JsonArray();
        for (Map<String, Object> row : rows) {
            JsonObject object = new JsonObject();
            row.forEach((name, value) -> object.add(name, REQUEST_GSON.toJsonTree(value)));
            array.add(object);
        }
        return REQUEST_GSON.toJson(array);
    }

    /** Encodes a batch in the requested field order, preserving explicit nulls and skipping absent fields. */
    public static String encodeRows(
            List<Object[]> rows, List<FieldSchema> inputFields, List<RelDataTypeField> dataFields) {
        JsonArray array = new JsonArray();
        if (rows.isEmpty()) {
            return REQUEST_GSON.toJson(array);
        }

        int[] fieldIndexes = new int[inputFields.size()];
        for (int i = 0; i < inputFields.size(); i++) {
            fieldIndexes[i] = DataTypeUtils.findFieldIndex(dataFields, inputFields.get(i).getName());
        }

        for (Object[] row : rows) {
            JsonObject object = new JsonObject();
            for (int i = 0; i < inputFields.size(); i++) {
                int fieldIndex = fieldIndexes[i];
                if (fieldIndex >= 0 && fieldIndex < row.length) {
                    object.add(inputFields.get(i).getName(), REQUEST_GSON.toJsonTree(row[fieldIndex]));
                }
            }
            array.add(object);
        }
        return REQUEST_GSON.toJson(array);
    }
}
