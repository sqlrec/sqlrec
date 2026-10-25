package com.sqlrec.common.utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** JSON serialization of named rows and field projections. */
public final class RowJsonEncoder {
    private static final Gson gson = JsonUtils.getGson();
    private static final Gson requestGson = gson.newBuilder().serializeNulls().create();

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
                jsonObject.add(entry.getKey(), requestGson.toJsonTree(entry.getValue()));
            }
            jsonArray.add(jsonObject);
        }
        return requestGson.toJson(jsonArray);
    }

    public static String toJsonArray(List<Object[]> data, List<FieldSchema> inputFields, List<RelDataTypeField> dataFields) {
        JsonArray jsonArray = new JsonArray();
        if (data.isEmpty()) {
            return requestGson.toJson(jsonArray);
        }

        int[] fieldIndexes = new int[inputFields.size()];
        for (int i = 0; i < inputFields.size(); i++) {
            fieldIndexes[i] = DataTypeUtils.findFieldIndex(dataFields, inputFields.get(i).getName());
        }

        for (Object[] row : data) {
            JsonObject jsonObject = new JsonObject();
            for (int i = 0; i < inputFields.size(); i++) {
                FieldSchema field = inputFields.get(i);
                int fieldIndex = fieldIndexes[i];
                if (fieldIndex >= 0 && fieldIndex < row.length) {
                    Object value = row[fieldIndex];
                    jsonObject.add(field.getName(), requestGson.toJsonTree(value));
                }
            }
            jsonArray.add(jsonObject);
        }

        return requestGson.toJson(jsonArray);
    }
}
