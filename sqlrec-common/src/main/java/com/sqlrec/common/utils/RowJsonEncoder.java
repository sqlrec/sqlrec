package com.sqlrec.common.utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** JSON serialization of named rows, field projections and grouped columns. */
public final class RowJsonEncoder {
    private static final Gson gson = JsonUtils.getGson();

    private RowJsonEncoder() {
    }

    public static String toJson(Object[] row, List<FieldSchema> fields) {
        return encodeRow(row, fields, FieldSchema::getName);
    }

    public static String toJsonByFields(Object[] row, List<RelDataTypeField> fields) {
        return encodeRow(row, fields, RelDataTypeField::getName);
    }

    private static <F> String encodeRow(Object[] row, List<F> fields, Function<F, String> nameReader) {
        JsonObject object = new JsonObject();
        for (int i = 0; i < fields.size(); i++) {
            object.add(nameReader.apply(fields.get(i)), gson.toJsonTree(row[i]));
        }
        return gson.toJson(object);
    }

    public static String toJsonArray(List<Map<String, Object>> rows) {
        JsonArray jsonArray = new JsonArray();
        for (Map<String, Object> row : rows) {
            JsonObject jsonObject = new JsonObject();
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (entry.getValue() != null) {
                    jsonObject.add(entry.getKey(), gson.toJsonTree(entry.getValue()));
                }
            }
            jsonArray.add(jsonObject);
        }
        return gson.toJson(jsonArray);
    }

    public static String toJsonArray(List<Object[]> data, List<FieldSchema> inputFields, List<RelDataTypeField> dataFields) {
        JsonArray jsonArray = new JsonArray();

        for (Object[] row : data) {
            JsonObject jsonObject = new JsonObject();
            for (int i = 0; i < inputFields.size(); i++) {
                FieldSchema field = inputFields.get(i);
                int fieldIndex = DataTypeUtils.findFieldIndex(dataFields, field.getName());
                if (fieldIndex >= 0 && fieldIndex < row.length) {
                    Object value = row[fieldIndex];
                    if (value != null) {
                        jsonObject.add(field.getName(), gson.toJsonTree(value));
                    }
                }
            }
            jsonArray.add(jsonObject);
        }

        return gson.toJson(jsonArray);
    }

    public static String toColumnarJson(List<Object[]> queryData, List<Object[]> valueData,
                                        List<FieldSchema> queryFields, List<FieldSchema> valueFields,
                                        List<RelDataTypeField> queryDataFields, List<RelDataTypeField> valueDataFields) {
        JsonObject jsonObject = new JsonObject();

        for (int i = 0; i < queryFields.size(); i++) {
            FieldSchema field = queryFields.get(i);
            int fieldIndex = DataTypeUtils.findFieldIndex(queryDataFields, field.getName());
            if (fieldIndex >= 0 && queryData.size() > 0) {
                Object value = queryData.get(0)[fieldIndex];
                JsonArray jsonArray = new JsonArray();
                if (value != null) {
                    jsonArray.add(gson.toJsonTree(value));
                } else {
                    jsonArray.add(JsonNull.INSTANCE);
                }
                jsonObject.add(field.getName(), jsonArray);
            }
        }

        for (int i = 0; i < valueFields.size(); i++) {
            FieldSchema field = valueFields.get(i);
            int fieldIndex = DataTypeUtils.findFieldIndex(valueDataFields, field.getName());
            if (fieldIndex >= 0) {
                JsonArray jsonArray = new JsonArray();
                for (Object[] row : valueData) {
                    Object value = row[fieldIndex];
                    if (value != null) {
                        jsonArray.add(gson.toJsonTree(value));
                    } else {
                        jsonArray.add(JsonNull.INSTANCE);
                    }
                }
                jsonObject.add(field.getName(), jsonArray);
            }
        }

        return gson.toJson(jsonObject);
    }
}
