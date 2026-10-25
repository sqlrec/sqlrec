package com.sqlrec.udf.inference;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.RowJsonEncoder;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Model input selection and request layouts shared by prediction functions. */
public final class PredictionRequestEncoder {
    private static final Gson gson = JsonUtils.getGson();

    private PredictionRequestEncoder() {
    }

    public static String encodeRows(List<Map<String, Object>> rows) {
        return RowJsonEncoder.toJsonArray(rows);
    }

    public static String encodeRows(
            List<Object[]> rows, List<FieldSchema> modelFields, List<RelDataTypeField> dataFields) {
        return RowJsonEncoder.toJsonArray(rows, modelFields, dataFields);
    }

    /** Encodes the first user row and all item rows as separate feature column arrays. */
    public static String encodeUserItems(
            List<Object[]> userRows, List<Object[]> itemRows, List<FieldSchema> modelFields,
            List<RelDataTypeField> userDataFields, List<RelDataTypeField> itemDataFields) {
        JsonObject request = new JsonObject();
        List<FieldSchema> itemFields = new ArrayList<>();
        for (FieldSchema field : modelFields) {
            int index = DataTypeUtils.findFieldIndex(userDataFields, field.getName());
            if (index < 0) {
                itemFields.add(field);
            } else if (!userRows.isEmpty()) {
                JsonArray values = new JsonArray();
                values.add(gson.toJsonTree(userRows.get(0)[index]));
                request.add(field.getName(), values);
            }
        }
        for (FieldSchema field : itemFields) {
            int index = DataTypeUtils.findFieldIndex(itemDataFields, field.getName());
            if (index >= 0) {
                JsonArray values = new JsonArray();
                for (Object[] row : itemRows) {
                    values.add(gson.toJsonTree(row[index]));
                }
                request.add(field.getName(), values);
            }
        }
        return gson.toJson(request);
    }
}
