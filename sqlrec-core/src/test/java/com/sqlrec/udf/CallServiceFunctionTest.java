package com.sqlrec.udf;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.udf.table.CallServiceFunction;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class CallServiceFunctionTest {

    @Test
    public void testMergePredictionsListPrediction() {
        List<Object[]> inputData = new ArrayList<>();
        inputData.add(new Object[]{1, "A"});
        inputData.add(new Object[]{2, "B"});

        Map<String, Object> predictions = new LinkedHashMap<>();
        predictions.put("score", Arrays.asList(0.9, 0.8));

        List<FieldSchema> outputFields = new ArrayList<>();
        outputFields.add(new FieldSchema("score", "DOUBLE"));

        List<Object[]> result = CallServiceFunction.mergePredictions(inputData, predictions, outputFields);

        assertEquals(2, result.size());
        assertEquals(3, result.get(0).length);
        assertEquals(1, result.get(0)[0]);
        assertEquals("A", result.get(0)[1]);
        assertEquals(0.9, result.get(0)[2]);
        assertEquals(2, result.get(1)[0]);
        assertEquals("B", result.get(1)[1]);
        assertEquals(0.8, result.get(1)[2]);
    }

    @Test
    public void testMergePredictionsScalarPrediction() {
        List<Object[]> inputData = new ArrayList<>();
        inputData.add(new Object[]{1, "A"});
        inputData.add(new Object[]{2, "B"});

        Map<String, Object> predictions = new LinkedHashMap<>();
        predictions.put("label", "positive");

        List<FieldSchema> outputFields = new ArrayList<>();
        outputFields.add(new FieldSchema("label", "VARCHAR"));

        List<Object[]> result = CallServiceFunction.mergePredictions(inputData, predictions, outputFields);

        assertEquals(2, result.size());
        assertEquals("positive", result.get(0)[2]);
        assertEquals("positive", result.get(1)[2]);
    }

    @Test
    public void testMergePredictionsMultipleOutputFields() {
        List<Object[]> inputData = new ArrayList<>();
        inputData.add(new Object[]{1});

        Map<String, Object> predictions = new LinkedHashMap<>();
        predictions.put("score", Arrays.asList(0.5));
        predictions.put("label", Arrays.asList("yes"));

        List<FieldSchema> outputFields = new ArrayList<>();
        outputFields.add(new FieldSchema("score", "DOUBLE"));
        outputFields.add(new FieldSchema("label", "VARCHAR"));

        List<Object[]> result = CallServiceFunction.mergePredictions(inputData, predictions, outputFields);

        assertEquals(1, result.size());
        assertEquals(3, result.get(0).length);
        assertEquals(1, result.get(0)[0]);
        assertEquals(0.5, result.get(0)[1]);
        assertEquals("yes", result.get(0)[2]);
    }

    @Test
    public void testMergePredictionsEmptyInput() {
        List<Object[]> inputData = new ArrayList<>();

        Map<String, Object> predictions = new LinkedHashMap<>();
        predictions.put("score", Arrays.asList());

        List<FieldSchema> outputFields = new ArrayList<>();
        outputFields.add(new FieldSchema("score", "DOUBLE"));

        List<Object[]> result = CallServiceFunction.mergePredictions(inputData, predictions, outputFields);

        assertEquals(0, result.size());
    }

    @Test
    public void testMergePredictionsPredictionListShorterThanInput() {
        List<Object[]> inputData = new ArrayList<>();
        inputData.add(new Object[]{1, "A"});
        inputData.add(new Object[]{2, "B"});
        inputData.add(new Object[]{3, "C"});

        Map<String, Object> predictions = new LinkedHashMap<>();
        predictions.put("score", Arrays.asList(0.9, 0.8));

        List<FieldSchema> outputFields = new ArrayList<>();
        outputFields.add(new FieldSchema("score", "DOUBLE"));

        List<Object[]> result = CallServiceFunction.mergePredictions(inputData, predictions, outputFields);

        assertEquals(3, result.size());
        assertEquals(0.9, result.get(0)[2]);
        assertEquals(0.8, result.get(1)[2]);
        assertNull(result.get(2)[2]);
    }

    @Test
    public void testMergePredictionsNoPredictions() {
        List<Object[]> inputData = new ArrayList<>();
        inputData.add(new Object[]{1, "A"});

        Map<String, Object> predictions = new LinkedHashMap<>();

        List<FieldSchema> outputFields = new ArrayList<>();

        List<Object[]> result = CallServiceFunction.mergePredictions(inputData, predictions, outputFields);

        assertEquals(1, result.size());
        assertEquals(2, result.get(0).length);
    }
}
