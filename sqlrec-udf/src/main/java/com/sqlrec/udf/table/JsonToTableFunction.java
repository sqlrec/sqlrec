package com.sqlrec.udf.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonToTableFunction {

    public CacheTable evaluate(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            throw new IllegalArgumentException("json string is empty");
        }

        JsonElement root = JsonParser.parseString(jsonString);
        List<JsonObject> objects = collectObjects(root);

        if (objects.isEmpty()) {
            throw new IllegalArgumentException("no json objects found in input");
        }

        List<String> keys = collectKeys(objects);
        Map<String, String> columnTypes = inferColumnTypes(objects, keys);
        List<RelDataTypeField> dataFields = createDataFields(keys, columnTypes);
        List<Object[]> rows = createRows(objects, keys, columnTypes);
        return new CacheTable("output", Linq4j.asEnumerable(rows), dataFields);
    }

    private static List<JsonObject> collectObjects(JsonElement root) {
        List<JsonObject> objects = new ArrayList<>();
        if (root.isJsonArray()) {
            for (JsonElement element : root.getAsJsonArray()) {
                if (element.isJsonObject()) {
                    objects.add(element.getAsJsonObject());
                }
            }
            return objects;
        }
        if (root.isJsonObject()) {
            objects.add(root.getAsJsonObject());
            return objects;
        }
        throw new IllegalArgumentException("json string must be a json object or json array");
    }

    private static List<String> collectKeys(List<JsonObject> objects) {
        Set<String> allKeys = new LinkedHashSet<>();
        for (JsonObject object : objects) {
            allKeys.addAll(object.keySet());
        }
        return new ArrayList<>(allKeys);
    }

    private static Map<String, String> inferColumnTypes(
            List<JsonObject> objects,
            List<String> keys
    ) {
        Map<String, String> columnTypes = new LinkedHashMap<>();
        for (String key : keys) {
            columnTypes.put(key, inferColumnType(objects, key));
        }
        return columnTypes;
    }

    private static List<RelDataTypeField> createDataFields(
            List<String> keys,
            Map<String, String> columnTypes
    ) {
        List<RelDataTypeField> dataFields = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            dataFields.add(DataTypeUtils.getRelDataTypeField(key, i, columnTypes.get(key)));
        }
        return dataFields;
    }

    private static List<Object[]> createRows(
            List<JsonObject> objects,
            List<String> keys,
            Map<String, String> columnTypes
    ) {
        List<Object[]> rows = new ArrayList<>();
        for (JsonObject object : objects) {
            Object[] row = new Object[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                row[i] = convertValue(object.get(key), columnTypes.get(key));
            }
            rows.add(row);
        }
        return rows;
    }

    private static String inferColumnType(List<JsonObject> objects, String key) {
        for (JsonObject obj : objects) {
            JsonElement element = obj.get(key);
            if (element != null && !element.isJsonNull()) {
                if (element.isJsonArray()) {
                    String elementType = inferArrayElementType(element.getAsJsonArray());
                    return "ARRAY<" + elementType + ">";
                }
                if (element.isJsonObject()) {
                    return "VARCHAR";
                }
                if (element.isJsonPrimitive()) {
                    JsonPrimitive prim = element.getAsJsonPrimitive();
                    if (prim.isBoolean()) {
                        return "BOOLEAN";
                    }
                    if (prim.isNumber()) {
                        return "DOUBLE";
                    }
                    return "VARCHAR";
                }
            }
        }
        return "VARCHAR";
    }

    private static String inferArrayElementType(JsonArray array) {
        for (JsonElement item : array) {
            if (item != null && !item.isJsonNull() && item.isJsonPrimitive()) {
                JsonPrimitive prim = item.getAsJsonPrimitive();
                if (prim.isNumber()) {
                    return "DOUBLE";
                }
                if (prim.isBoolean()) {
                    return "BOOLEAN";
                }
                return "VARCHAR";
            }
        }
        return "VARCHAR";
    }

    private static Object convertValue(JsonElement element, String type) {
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (type.startsWith("ARRAY<")) {
            if (!element.isJsonArray()) {
                return null;
            }
            String elementType = type.substring("ARRAY<".length(), type.length() - 1);
            JsonArray array = element.getAsJsonArray();
            List<Object> result = new ArrayList<>(array.size());
            for (int i = 0; i < array.size(); i++) {
                JsonElement item = array.get(i);
                if (item == null || item.isJsonNull()) {
                    result.add(null);
                } else if ("DOUBLE".equals(elementType)) {
                    if (item.isJsonPrimitive() && item.getAsJsonPrimitive().isNumber()) {
                        result.add(item.getAsDouble());
                    } else {
                        result.add(null);
                    }
                } else if ("BOOLEAN".equals(elementType)) {
                    if (item.isJsonPrimitive() && item.getAsJsonPrimitive().isBoolean()) {
                        result.add(item.getAsBoolean());
                    } else {
                        result.add(null);
                    }
                } else {
                    // VARCHAR
                    if (item.isJsonPrimitive()) {
                        result.add(item.getAsString());
                    } else {
                        result.add(item.toString());
                    }
                }
            }
            return result;
        }
        if ("BOOLEAN".equals(type)) {
            if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isBoolean()) {
                return element.getAsBoolean();
            }
            return null;
        }
        if ("DOUBLE".equals(type)) {
            if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isNumber()) {
                return element.getAsDouble();
            }
            return null;
        }
        // VARCHAR - store as string
        if (element.isJsonPrimitive()) {
            return element.getAsString();
        }
        // nested object or other - store as json string
        return element.toString();
    }
}
