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

        // Collect all json objects to process
        List<JsonObject> objects = new ArrayList<>();
        if (root.isJsonArray()) {
            JsonArray array = root.getAsJsonArray();
            for (JsonElement element : array) {
                if (element.isJsonObject()) {
                    objects.add(element.getAsJsonObject());
                }
            }
        } else if (root.isJsonObject()) {
            objects.add(root.getAsJsonObject());
        } else {
            throw new IllegalArgumentException("json string must be a json object or json array");
        }

        if (objects.isEmpty()) {
            throw new IllegalArgumentException("no json objects found in input");
        }

        // Collect all keys preserving insertion order
        Set<String> allKeys = new LinkedHashSet<>();
        for (JsonObject obj : objects) {
            for (String key : obj.keySet()) {
                allKeys.add(key);
            }
        }

        List<String> keyList = new ArrayList<>(allKeys);

        // Infer column types from the first non-null value for each key
        Map<String, String> columnTypes = new LinkedHashMap<>();
        for (String key : keyList) {
            columnTypes.put(key, inferColumnType(objects, key));
        }

        // Build schema fields
        List<RelDataTypeField> dataFields = new ArrayList<>();
        for (int i = 0; i < keyList.size(); i++) {
            String key = keyList.get(i);
            dataFields.add(DataTypeUtils.getRelDataTypeField(key, i, columnTypes.get(key)));
        }

        // Build row data
        List<Object[]> rows = new ArrayList<>();
        for (JsonObject obj : objects) {
            Object[] row = new Object[keyList.size()];
            for (int i = 0; i < keyList.size(); i++) {
                String key = keyList.get(i);
                row[i] = convertValue(obj.get(key), columnTypes.get(key));
            }
            rows.add(row);
        }

        return new CacheTable("output", Linq4j.asEnumerable(rows), dataFields);
    }

    private String inferColumnType(List<JsonObject> objects, String key) {
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

    private String inferArrayElementType(JsonArray array) {
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

    private Object convertValue(JsonElement element, String type) {
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
