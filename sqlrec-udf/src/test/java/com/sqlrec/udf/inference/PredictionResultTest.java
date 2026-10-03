package com.sqlrec.udf.inference;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PredictionResultTest {
    @Test
    void distinguishesShortListsFromExplicitNullPredictionsWhenOverwritingInputs() {
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", Arrays.asList(99L));
        columns.put("label", "recommended");
        columns.put("score", Arrays.asList(0.5, null));
        columns.put("cleared", null);
        PredictionResult predictions = new PredictionResult(columns);

        Map<String, Object> secondRow = new LinkedHashMap<>(Map.of("id", 2L, "score", 1.0, "cleared", "old"));
        predictions.forEachValue(1, secondRow::put);
        assertEquals(2L, secondRow.get("id"));
        assertEquals("recommended", secondRow.get("label"));
        assertNull(secondRow.get("score"));
        assertNull(secondRow.get("cleared"));
        assertNull(predictions.getValue("id", 1));
        assertNull(predictions.getValue("missing", 0));
        assertEquals("recommended", predictions.getValue("label", 5));
        assertEquals(99L, predictions.getValue("id", 0));
        assertEquals(Arrays.asList(99L), columns.get("id"));
    }

    @Test
    void emptyResponseLeavesBatchInputsIntact() {
        Map<String, Object> input = new LinkedHashMap<>(Map.of("id", 1L));
        new PredictionResult(null).forEachValue(0, input::put);
        assertEquals(Map.of("id", 1L), input);
    }
}
