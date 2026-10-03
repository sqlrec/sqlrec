package com.sqlrec.udf.udtf;

import com.google.gson.*;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.udf.table.CallServiceFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@FunctionHint(output = @DataTypeHint("ROW<" +
        "long_map MAP<STRING, BIGINT>, " +
        "double_map MAP<STRING, DOUBLE>, " +
        "string_map MAP<STRING, STRING>, " +
        "long_array_map MAP<STRING, ARRAY<BIGINT>>, " +
        "double_array_map MAP<STRING, ARRAY<DOUBLE>>, " +
        "string_array_map MAP<STRING, ARRAY<STRING>>" +
        ">"))
public class BatchCallServiceUDTF extends TableFunction<Row> {
    private static final Gson gson = JsonUtils.getGson();

    private List<Map<String, Object>> buffer;
    private int batchSize;
    private String serviceUrl;
    private transient BatchPredictionClient predictionClient;

    public BatchCallServiceUDTF() {
    }

    BatchCallServiceUDTF(BatchPredictionClient predictionClient) {
        this.predictionClient = predictionClient;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        buffer = new ArrayList<>();
        batchSize = 0;
        serviceUrl = null;
        if (predictionClient == null) {
            predictionClient = CallServiceFunction::callPredictionService;
        }
    }

    public void eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... args) {
        if (args == null || args.length < 3) {
            throw new IllegalArgumentException(
                    "At least 3 arguments required: serviceUrl, batchSize, "
                            + "and at least one fieldName-value pair");
        }

        if (!(args[0] instanceof String)) {
            throw new IllegalArgumentException("First argument (serviceUrl) must be a String");
        }
        String requestedServiceUrl = (String) args[0];
        if (requestedServiceUrl.isEmpty()) {
            throw new IllegalArgumentException("serviceUrl must not be empty");
        }

        if (!(args[1] instanceof Integer)) {
            throw new IllegalArgumentException("Second argument (batchSize) must be an Integer");
        }
        int requestedBatchSize = (Integer) args[1];
        if (requestedBatchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }

        if (this.serviceUrl == null) {
            this.serviceUrl = requestedServiceUrl;
        } else if (!this.serviceUrl.equals(requestedServiceUrl)) {
            throw new IllegalArgumentException("serviceUrl must remain constant for one function instance");
        }
        if (this.batchSize == 0) {
            this.batchSize = requestedBatchSize;
        } else if (this.batchSize != requestedBatchSize) {
            throw new IllegalArgumentException("batchSize must remain constant for one function instance");
        }

        int pairCount = args.length - 2;
        if (pairCount % 2 != 0) {
            throw new IllegalArgumentException("fieldNameValuePairs must be in pairs of (fieldName, value)");
        }

        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 2; i < args.length; i += 2) {
            if (!(args[i] instanceof String)) {
                Object fieldName = args[i];
                String actualType = fieldName == null ? "null" : fieldName.getClass().getName();
                throw new IllegalArgumentException("Field name at position " + (i - 2)
                        + " must be a String, but got: " + actualType);
            }
            String fieldName = (String) args[i];
            Object value = args[i + 1];
            row.put(fieldName, value);
        }

        buffer.add(row);

        if (buffer.size() >= this.batchSize) {
            processBatch();
        }
    }

    @Override
    public void finish() {
        processBatch();
    }

    private void processBatch() {
        if (buffer.isEmpty()) {
            return;
        }

        try {
            String jsonData = buildJsonArray(buffer);
            Map<String, Object> predictions = predictionClient.call(serviceUrl, jsonData);
            outputResults(predictions);
        } catch (Exception e) {
            throw new RuntimeException("Failed to process batch: " + e.getMessage(), e);
        } finally {
            buffer.clear();
        }
    }

    private String buildJsonArray(List<Map<String, Object>> rows) {
        JsonArray jsonArray = new JsonArray();
        for (Map<String, Object> row : rows) {
            JsonObject jsonObject = new JsonObject();
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();
                if (value != null) {
                    jsonObject.add(fieldName, gson.toJsonTree(value));
                }
            }
            jsonArray.add(jsonObject);
        }
        return gson.toJson(jsonArray);
    }

    private void outputResults(Map<String, Object> predictions) {
        for (int i = 0; i < buffer.size(); i++) {
            Map<String, Object> values = mergePredictionRow(buffer.get(i), predictions, i);
            collect(toOutputRow(values));
        }
    }

    private static Map<String, Object> mergePredictionRow(
            Map<String, Object> inputRow, Map<String, Object> predictions, int rowIndex) {
        Map<String, Object> combinedMap = new LinkedHashMap<>(inputRow);

        if (predictions != null) {
            for (Map.Entry<String, Object> entry : predictions.entrySet()) {
                Object prediction = entry.getValue();
                if (prediction instanceof List) {
                    List<?> predictionList = (List<?>) prediction;
                    if (rowIndex < predictionList.size()) {
                        combinedMap.put(entry.getKey(), predictionList.get(rowIndex));
                    }
                } else {
                    combinedMap.put(entry.getKey(), prediction);
                }
            }
        }
        return combinedMap;
    }

    private static Row toOutputRow(Map<String, Object> values) {
        OutputFields fields = new OutputFields();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            fields.add(entry.getKey(), entry.getValue());
        }
        return fields.toRow();
    }

    private static boolean isInteger(Object value) {
        return value instanceof Long || value instanceof Integer
                || value instanceof Short || value instanceof Byte;
    }

    private static boolean isFloatingPoint(Object value) {
        return value instanceof Double || value instanceof Float;
    }

    private static Long toLong(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
        return ((Number) value).longValue();
    }

    private static Double toDouble(Object value) {
        if (value instanceof Double) {
            return (Double) value;
        }
        return ((Number) value).doubleValue();
    }

    private static Long[] toLongArray(Integer[] intArr) {
        Long[] arr = new Long[intArr.length];
        for (int j = 0; j < intArr.length; j++) {
            arr[j] = intArr[j].longValue();
        }
        return arr;
    }

    private static Double[] toDoubleArray(Float[] floatArr) {
        Double[] arr = new Double[floatArr.length];
        for (int j = 0; j < floatArr.length; j++) {
            arr[j] = floatArr[j].doubleValue();
        }
        return arr;
    }

    /** Collects values in the six maps declared by the function's output schema. */
    private static final class OutputFields {
        private final Map<String, Long> longs = new LinkedHashMap<>();
        private final Map<String, Double> doubles = new LinkedHashMap<>();
        private final Map<String, String> strings = new LinkedHashMap<>();
        private final Map<String, Long[]> longArrays = new LinkedHashMap<>();
        private final Map<String, Double[]> doubleArrays = new LinkedHashMap<>();
        private final Map<String, String[]> stringArrays = new LinkedHashMap<>();

        private void add(String key, Object value) {
            if (isInteger(value)) {
                longs.put(key, toLong(value));
            } else if (isFloatingPoint(value)) {
                doubles.put(key, toDouble(value));
            } else if (value instanceof String) {
                strings.put(key, (String) value);
            } else if (value instanceof List) {
                addList(key, (List<?>) value);
            } else if (value instanceof Long[]) {
                longArrays.put(key, (Long[]) value);
            } else if (value instanceof Integer[]) {
                longArrays.put(key, toLongArray((Integer[]) value));
            } else if (value instanceof Double[]) {
                doubleArrays.put(key, (Double[]) value);
            } else if (value instanceof Float[]) {
                doubleArrays.put(key, toDoubleArray((Float[]) value));
            } else if (value instanceof String[]) {
                stringArrays.put(key, (String[]) value);
            }
        }

        private void addList(String key, List<?> values) {
            if (values.isEmpty()) {
                return;
            }
            // The first element determines the array type; other types keep the original fallbacks.
            Object first = values.get(0);
            if (isInteger(first)) {
                longArrays.put(key, values.stream()
                        .map(value -> {
                            if (isInteger(value)) return toLong(value);
                            return 0L;
                        })
                        .toArray(Long[]::new));
            } else if (isFloatingPoint(first)) {
                doubleArrays.put(key, values.stream()
                        .map(value -> {
                            if (isFloatingPoint(value)) return toDouble(value);
                            return 0.0;
                        })
                        .toArray(Double[]::new));
            } else if (first instanceof String) {
                stringArrays.put(key, values.stream()
                        .map(Object::toString)
                        .toArray(String[]::new));
            }
        }

        private Row toRow() {
            return Row.of(longs, doubles, strings, longArrays, doubleArrays, stringArrays);
        }
    }

    @FunctionalInterface
    interface BatchPredictionClient {
        Map<String, Object> call(String serviceUrl, String jsonData);
    }
}
