package com.sqlrec.common.utils;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.Map;

public class JsonUtils {
    private static final Gson gson = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .create();

    public static String toJson(Object object) {
        return gson.toJson(object);
    }

    public static String toJson(Object[] objects, List<FieldSchema> fieldSchemas) {
        JsonObject jsonObject = new JsonObject();
        for (int i = 0; i < fieldSchemas.size(); i++) {
            FieldSchema fieldSchema = fieldSchemas.get(i);
            jsonObject.add(fieldSchema.name, gson.toJsonTree(objects[i]));
        }
        return gson.toJson(jsonObject);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) throws JsonSyntaxException {
        return gson.fromJson(json, classOfT);
    }

    public static Object[] fromJson(String json, List<FieldSchema> fieldSchemas) {
        Map<String, Object> dataMap = gson.fromJson(json, Map.class);
        Object[] objects = new Object[fieldSchemas.size()];
        for (int i = 0; i < fieldSchemas.size(); i++) {
            objects[i] = dataMap.get(fieldSchemas.get(i).name);
        }
        return objects;
    }

    public static List<String> parseStringList(String json) {
        return gson.fromJson(json, new TypeToken<List<String>>() {
        }.getType());
    }

    public static String toJsonArray(List<Object[]> data, List<FieldSchema> inputFields, List<RelDataTypeField> dataFields) {
        JsonArray jsonArray = new JsonArray();
        
        for (Object[] row : data) {
            JsonObject jsonObject = new JsonObject();
            for (int i = 0; i < inputFields.size(); i++) {
                FieldSchema field = inputFields.get(i);
                int fieldIndex = findFieldIndex(dataFields, field.name);
                if (fieldIndex >= 0 && fieldIndex < row.length) {
                    Object value = row[fieldIndex];
                    if (value != null) {
                        jsonObject.addProperty(field.name, value.toString());
                    }
                }
            }
            jsonArray.add(jsonObject);
        }
        
        return gson.toJson(jsonArray);
    }

    private static int findFieldIndex(List<RelDataTypeField> dataFields, String fieldName) {
        for (int i = 0; i < dataFields.size(); i++) {
            if (dataFields.get(i).getName().equalsIgnoreCase(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    public static Map<String, Object> parseJsonToMap(String json) {
        return gson.fromJson(json, Map.class);
    }
}
