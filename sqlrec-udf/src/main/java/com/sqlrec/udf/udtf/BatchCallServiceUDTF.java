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

        Object[] fieldNameValuePairs = new Object[args.length - 2];
        System.arraycopy(args, 2, fieldNameValuePairs, 0, args.length - 2);

        if (fieldNameValuePairs.length == 0) {
            return;
        }

        if (fieldNameValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("fieldNameValuePairs must be in pairs of (fieldName, value)");
        }

        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < fieldNameValuePairs.length; i += 2) {
            if (!(fieldNameValuePairs[i] instanceof String)) {
                Object fieldName = fieldNameValuePairs[i];
                String actualType = fieldName == null ? "null" : fieldName.getClass().getName();
                throw new IllegalArgumentException("Field name at position " + i
                        + " must be a String, but got: " + actualType);
            }
            String fieldName = (String) fieldNameValuePairs[i];
            Object value = fieldNameValuePairs[i + 1];
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
            Map<String, Object> inputRow = buffer.get(i);
            Map<String, Object> combinedMap = new LinkedHashMap<>(inputRow);

            if (predictions != null) {
                for (Map.Entry<String, Object> entry : predictions.entrySet()) {
                    Object prediction = entry.getValue();
                    if (prediction instanceof List) {
                        List<?> predictionList = (List<?>) prediction;
                        if (i < predictionList.size()) {
                            combinedMap.put(entry.getKey(), predictionList.get(i));
                        }
                    } else {
                        combinedMap.put(entry.getKey(), prediction);
                    }
                }
            }

            Map<String, Long> longMap = new LinkedHashMap<>();
            Map<String, Double> doubleMap = new LinkedHashMap<>();
            Map<String, String> stringMap = new LinkedHashMap<>();
            Map<String, Long[]> longArrayMap = new LinkedHashMap<>();
            Map<String, Double[]> doubleArrayMap = new LinkedHashMap<>();
            Map<String, String[]> stringArrayMap = new LinkedHashMap<>();

            for (Map.Entry<String, Object> entry : combinedMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (value instanceof Long) {
                    longMap.put(key, (Long) value);
                } else if (value instanceof Integer) {
                    longMap.put(key, ((Integer) value).longValue());
                } else if (value instanceof Short) {
                    longMap.put(key, ((Short) value).longValue());
                } else if (value instanceof Byte) {
                    longMap.put(key, ((Byte) value).longValue());
                } else if (value instanceof Double) {
                    doubleMap.put(key, (Double) value);
                } else if (value instanceof Float) {
                    doubleMap.put(key, ((Float) value).doubleValue());
                } else if (value instanceof String) {
                    stringMap.put(key, (String) value);
                } else if (value instanceof List) {
                    List<?> list = (List<?>) value;
                    if (!list.isEmpty()) {
                        Object first = list.get(0);
                        if (first instanceof Long || first instanceof Integer
                                || first instanceof Short || first instanceof Byte) {
                            Long[] arr = list.stream()
                                    .map(v -> {
                                        if (v instanceof Long) return (Long) v;
                                        if (v instanceof Integer) return ((Integer) v).longValue();
                                        if (v instanceof Short) return ((Short) v).longValue();
                                        if (v instanceof Byte) return ((Byte) v).longValue();
                                        return 0L;
                                    })
                                    .toArray(Long[]::new);
                            longArrayMap.put(key, arr);
                        } else if (first instanceof Double || first instanceof Float) {
                            Double[] arr = list.stream()
                                    .map(v -> {
                                        if (v instanceof Double) return (Double) v;
                                        if (v instanceof Float) return ((Float) v).doubleValue();
                                        return 0.0;
                                    })
                                    .toArray(Double[]::new);
                            doubleArrayMap.put(key, arr);
                        } else if (first instanceof String) {
                            String[] arr = list.stream()
                                    .map(Object::toString)
                                    .toArray(String[]::new);
                            stringArrayMap.put(key, arr);
                        }
                    }
                } else if (value instanceof Long[]) {
                    longArrayMap.put(key, (Long[]) value);
                } else if (value instanceof Integer[]) {
                    Integer[] intArr = (Integer[]) value;
                    Long[] arr = new Long[intArr.length];
                    for (int j = 0; j < intArr.length; j++) {
                        arr[j] = intArr[j].longValue();
                    }
                    longArrayMap.put(key, arr);
                } else if (value instanceof Double[]) {
                    doubleArrayMap.put(key, (Double[]) value);
                } else if (value instanceof Float[]) {
                    Float[] floatArr = (Float[]) value;
                    Double[] arr = new Double[floatArr.length];
                    for (int j = 0; j < floatArr.length; j++) {
                        arr[j] = floatArr[j].doubleValue();
                    }
                    doubleArrayMap.put(key, arr);
                } else if (value instanceof String[]) {
                    stringArrayMap.put(key, (String[]) value);
                }
            }

            Row outputRow = Row.of(longMap, doubleMap, stringMap, longArrayMap, doubleArrayMap, stringArrayMap);
            collect(outputRow);
        }
    }

    @FunctionalInterface
    interface BatchPredictionClient {
        Map<String, Object> call(String serviceUrl, String jsonData);
    }
}
