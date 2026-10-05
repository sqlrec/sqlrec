package com.sqlrec.udf.table;

import com.google.gson.JsonParser;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.RowJsonEncoder;
import growthbook.sdk.java.evaluators.ConditionEvaluator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GrowthBookNullAttributesTest {
    @Test
    void predictionNullEncodingDoesNotChangeGrowthBookExistsTargeting() {
        String attributes = RowJsonEncoder.toJsonByFields(new Object[]{null},
                List.of(DataTypeUtils.getRelDataTypeField("country", 0, SqlTypeName.VARCHAR)));
        var evaluator = new ConditionEvaluator();
        var condition = JsonParser.parseString("{\"country\":{\"$exists\":true}}").getAsJsonObject();
        assertFalse(evaluator.evaluateCondition(JsonParser.parseString(attributes).getAsJsonObject(), condition, null));
        assertTrue(evaluator.evaluateCondition(JsonParser.parseString("{\"country\":\"CN\"}").getAsJsonObject(), condition, null));
    }
}
