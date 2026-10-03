package com.sqlrec.udf.inference;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.RowJsonEncoder;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Model input selection and request layouts shared by prediction functions. */
public final class PredictionRequestEncoder {
    private PredictionRequestEncoder() {
    }

    public static String encodeRows(List<Map<String, Object>> rows) {
        return RowJsonEncoder.toJsonArray(rows);
    }

    public static String encodeRows(
            List<Object[]> rows, List<FieldSchema> modelFields, List<RelDataTypeField> dataFields) {
        return RowJsonEncoder.toJsonArray(rows, modelFields, dataFields);
    }

    public static String encodeUserItems(
            List<Object[]> userRows, List<Object[]> itemRows, List<FieldSchema> modelFields,
            List<RelDataTypeField> userDataFields, List<RelDataTypeField> itemDataFields) {
        List<FieldSchema> userFields = new ArrayList<>();
        List<FieldSchema> itemFields = new ArrayList<>();
        for (FieldSchema field : modelFields) {
            if (DataTypeUtils.findFieldIndex(userDataFields, field.getName()) >= 0) {
                userFields.add(field);
            } else {
                itemFields.add(field);
            }
        }
        return RowJsonEncoder.toColumnarJson(userRows, itemRows, userFields, itemFields,
                userDataFields, itemDataFields);
    }
}
