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

    @Test
    void userItemRequestsUseOnlyTheFirstUserAndPreserveNestedFeatures() {
        List<FieldSchema> modelFields = Arrays.asList(
                new FieldSchema("user_tags", "ARRAY<STRING>"),
                new FieldSchema("item_id", "INT"), new FieldSchema("scores", "ARRAY<DOUBLE>"));

        assertEquals("{\"user_tags\":[[\"tag1\",\"tag2\"]],\"item_id\":[1,2],"
                        + "\"scores\":[[1.1,2.2],[3.3,4.4,5.5]]}",
                PredictionRequestEncoder.encodeUserItems(
                        Arrays.asList(new Object[]{Arrays.asList("tag1", "tag2")},
                                new Object[]{Collections.singletonList("ignored")}),
                        Arrays.asList(new Object[]{1, Arrays.asList(1.1, 2.2)},
                                new Object[]{2, Arrays.asList(3.3, 4.4, 5.5)}),
                        modelFields, Collections.singletonList(field("user_tags", 0)),
                        Arrays.asList(field("item_id", 0), field("scores", 1))));
    }

    @Test
    void userItemRequestsHandleEmptyUserAndItemBatches() {
        List<FieldSchema> modelFields = Arrays.asList(
                new FieldSchema("user", "STRING"), new FieldSchema("item", "STRING"));
        List<RelDataTypeField> userFields = Collections.singletonList(field("user", 0));
        List<RelDataTypeField> itemFields = Collections.singletonList(field("item", 0));

        assertEquals("{\"item\":[]}", PredictionRequestEncoder.encodeUserItems(
                Collections.emptyList(), Collections.emptyList(), modelFields, userFields, itemFields));
        assertEquals("{\"user\":[null],\"item\":[]}", PredictionRequestEncoder.encodeUserItems(
                Collections.singletonList(new Object[]{null}), Collections.emptyList(),
                modelFields, userFields, itemFields));
    }

    private static RelDataTypeField field(String name, int index) {
        return DataTypeUtils.getRelDataTypeField(name, index, SqlTypeName.VARCHAR);
    }
}
