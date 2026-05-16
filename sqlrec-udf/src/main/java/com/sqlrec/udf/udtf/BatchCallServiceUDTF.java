package com.sqlrec.udf.udtf;

import com.google.gson.*;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
    private static final Gson gson = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .create();

    private List<Map<String, Object>> buffer;
    private int batchSize;
    private String serviceUrl;

    @Override
    public void open(FunctionContext context) throws Exception {
        this.buffer = new ArrayList<>();
    }

    public void eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... args) {
        if (args == null || args.length < 3) {
            throw new IllegalArgumentException("At least 3 arguments required: serviceUrl, batchSize, and at least one fieldName-value pair");
        }

        if (!(args[0] instanceof String)) {
            throw new IllegalArgumentException("First argument (serviceUrl) must be a String");
        }
        String serviceUrl = (String) args[0];

        if (!(args[1] instanceof Integer)) {
            throw new IllegalArgumentException("Second argument (batchSize) must be an Integer");
        }
        Integer batchSize = (Integer) args[1];

        if (this.serviceUrl == null) {
            this.serviceUrl = serviceUrl;
        }
        if (this.batchSize == 0) {
            this.batchSize = batchSize != null && batchSize > 0 ? batchSize : 128;
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
                throw new IllegalArgumentException("Field name at position " + i + " must be a String, but got: " + fieldNameValuePairs[i].getClass().getName());
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
    public void close() throws Exception {
        if (!buffer.isEmpty()) {
            processBatch();
        }
    }

    private void processBatch() {
        if (buffer.isEmpty()) {
            return;
        }

        try {
            String jsonData = buildJsonArray(buffer);
            Map<String, Object> predictions = callPredictionService(serviceUrl, jsonData);
            outputResults(predictions);
        } catch (Exception e) {
            throw new RuntimeException("Failed to process batch: " + e.getMessage(), e);
        } finally {
            buffer.clear();
        }
    }

    private Map<String, Object> callPredictionService(String serviceUrl, String jsonData) {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(serviceUrl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);
            connection.setConnectTimeout(30000);
            connection.setReadTimeout(30000);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonData.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("HTTP request failed with response code: " + responseCode);
            }

            StringBuilder response = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
            }

            JsonElement jsonElement = JsonParser.parseString(response.toString());
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                result.put(entry.getKey(), gson.fromJson(entry.getValue(), Object.class));
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to call prediction service: " + e.getMessage(), e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
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
                        if (first instanceof Long || first instanceof Integer || first instanceof Short || first instanceof Byte) {
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
}
