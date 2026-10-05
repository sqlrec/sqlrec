package com.sqlrec.udf.inference;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PredictionRequestEncoderTest {
    @Test
    void rowRequestsProjectModelFieldsPreserveNullAndOmitMissingValues() {
        List<RelDataTypeField> dataFields = Arrays.asList(
                field("score", 0), field("ID", 1), field("ignored", 2));
        List<FieldSchema> modelFields = Arrays.asList(
                new FieldSchema("id", "STRING"), new FieldSchema("score", "FLOAT"),
                new FieldSchema("missing", "STRING"));
        List<Object[]> rows = Arrays.asList(new Object[]{null, "item", "unused"}, new Object[]{0.5});

        assertEquals("[{\"id\":\"item\",\"score\":null},{\"score\":0.5}]",
                PredictionRequestEncoder.encodeRows(rows, modelFields, dataFields));

        Map<String, Object> batchRow = new LinkedHashMap<>();
        batchRow.put("id", "item");
        batchRow.put("score", null);
        assertEquals("[{\"id\":\"item\",\"score\":null}]",
                PredictionRequestEncoder.encodeRows(Collections.singletonList(batchRow)));
    }

    @Test
    void userItemRequestsPreferUserFieldsAndPreserveNullsInColumnArrays() {
        List<RelDataTypeField> userFields = Arrays.asList(field("shared", 0), field("user", 1));
        List<RelDataTypeField> itemFields = Arrays.asList(field("shared", 0), field("item", 1));
        List<FieldSchema> modelFields = Arrays.asList(
                new FieldSchema("item", "STRING"), new FieldSchema("shared", "STRING"),
                new FieldSchema("user", "STRING"), new FieldSchema("missing", "STRING"));

        assertEquals("{\"shared\":[\"user-shared\"],\"user\":[null],\"item\":[\"first\",null]}",
                PredictionRequestEncoder.encodeUserItems(
                        Collections.singletonList(new Object[]{"user-shared", null}),
                        Arrays.asList(new Object[]{"item-shared", "first"}, new Object[]{"unused", null}),
                        modelFields, userFields, itemFields));
    }

    private static RelDataTypeField field(String name, int index) {
        return DataTypeUtils.getRelDataTypeField(name, index, SqlTypeName.VARCHAR);
    }
}
