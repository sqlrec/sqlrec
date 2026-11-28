package com.sqlrec.udf.udtf;

import com.sqlrec.common.utils.JsonRows;
import com.sqlrec.udf.inference.PredictionClient;
import com.sqlrec.udf.inference.PredictionResult;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Batches model requests across eval calls and returns buffered inputs with predictions.
 * Use with bounded, append-only input and INNER LATERAL TABLE, selecting row fields
 * only from the returned maps. Flink associates all rows emitted by a batch with
 * the current outer row, not their original outer rows: do not project outer row
 * fields, filter results using outer row fields, or use LEFT LATERAL JOIN.
 * Filter source rows before this function. The final partial batch is emitted by
 * finish(); an unbounded idle stream does not flush a partial batch automatically.
 */
@FunctionHint(output = @DataTypeHint("ROW<" +
        "long_map MAP<STRING, BIGINT>, " +
        "double_map MAP<STRING, DOUBLE>, " +
        "string_map MAP<STRING, STRING>, " +
        "long_array_map MAP<STRING, ARRAY<BIGINT>>, " +
        "double_array_map MAP<STRING, ARRAY<DOUBLE>>, " +
        "string_array_map MAP<STRING, ARRAY<STRING>>" +
        ">"))
public class BatchCallServiceUDTF extends TableFunction<Row> {
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
            predictionClient = PredictionClient.getDefault()::predict;
        }
    }

    public void eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... args) {
        configureBatch(args);
        Map<String, Object> row = parseInputRow(args);

        // Delayed outputs retain their original fields in these maps, but not
        // their original Flink lateral association. See the class usage restrictions.
        buffer.add(row);

        if (buffer.size() >= batchSize) {
            processBatch();
        }
    }

    @Override
    public void finish() {
        processBatch();
    }

    private void configureBatch(Object[] args) {
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
    }

    private static Map<String, Object> parseInputRow(Object[] args) {
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

        return row;
    }

    private void processBatch() {
        if (buffer.isEmpty()) {
            return;
        }

        try {
            String jsonData = JsonRows.encodeRows(buffer);
            Map<String, Object> predictions = predictionClient.call(serviceUrl, jsonData);
            outputResults(predictions);
        } catch (Exception e) {
            throw new RuntimeException("Failed to process batch: " + e.getMessage(), e);
        } finally {
            buffer.clear();
        }
    }

    private void outputResults(Map<String, Object> predictions) {
        PredictionResult result = new PredictionResult(predictions);
        // Only numeric arrays participate in promotion. Other rows keep their own conversion rules.
        Map<String, ArrayType> numericArrayTypes = new LinkedHashMap<>();
        for (int i = 0; i < buffer.size(); i++) {
            Map<String, Object> values = buffer.get(i);
            result.forEachValue(i, values::put);
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                String field = entry.getKey();
                if (numericArrayTypes.get(field) == ArrayType.DOUBLE) {
                    continue;
                }
                Object value = entry.getValue();
                ArrayType type = arrayType(value);
                if (type == ArrayType.BIGINT && value instanceof List
                        && ((List<?>) value).stream().anyMatch(BatchCallServiceUDTF::isFloatingPoint)) {
                    type = ArrayType.DOUBLE;
                }
                if (type == ArrayType.BIGINT || type == ArrayType.DOUBLE) {
                    numericArrayTypes.put(field, type);
                }
            }
        }

        List<Row> output = new ArrayList<>(buffer.size());
        for (Map<String, Object> values : buffer) {
            OutputFields fields = new OutputFields();
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                String field = entry.getKey();
                fields.add(field, entry.getValue(), numericArrayTypes.get(field));
            }
            output.add(fields.toRow());
        }
        output.forEach(this::collect);
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

    private enum ArrayType { BIGINT, DOUBLE, STRING, EMPTY }

    private static ArrayType arrayType(Object value) {
        if (value instanceof Long[] || value instanceof Integer[]) {
            return ArrayType.BIGINT;
        }
        if (value instanceof Double[] || value instanceof Float[]) {
            return ArrayType.DOUBLE;
        }
        if (value instanceof String[]) {
            return ArrayType.STRING;
        }
        if (value instanceof List) {
            List<?> values = (List<?>) value;
            Object first = values.stream().filter(Objects::nonNull).findFirst().orElse(null);
            if (first == null) {
                return ArrayType.EMPTY;
            }
            if (first instanceof String) {
                return ArrayType.STRING;
            }
            if (isInteger(first)) {
                return ArrayType.BIGINT;
            }
            if (isFloatingPoint(first)) {
                return ArrayType.DOUBLE;
            }
        }
        return null;
    }

    private static Double toArrayDouble(String field, Object value) {
        if (isInteger(value)) {
            long integer = ((Number) value).longValue();
            double converted = integer;
            if (converted >= 0x1p63 || (long) converted != integer) {
                throw new IllegalArgumentException("Output array '" + field
                        + "' has an integer that cannot be represented exactly as DOUBLE: " + integer);
            }
        }
        return toDouble(value);
    }

    /** Collects values in the six maps declared by the function's output schema. */
    private static final class OutputFields {
        private final Map<String, Long> longs = new LinkedHashMap<>();
        private final Map<String, Double> doubles = new LinkedHashMap<>();
        private final Map<String, String> strings = new LinkedHashMap<>();
        private final Map<String, Long[]> longArrays = new LinkedHashMap<>();
        private final Map<String, Double[]> doubleArrays = new LinkedHashMap<>();
        private final Map<String, String[]> stringArrays = new LinkedHashMap<>();

        private void add(String key, Object value, ArrayType batchNumericType) {
            if (isInteger(value)) {
                longs.put(key, toLong(value));
            } else if (isFloatingPoint(value)) {
                doubles.put(key, toDouble(value));
            } else if (value instanceof String) {
                strings.put(key, (String) value);
            } else {
                ArrayType type = arrayType(value);
                if (type == ArrayType.BIGINT || type == ArrayType.EMPTY) {
                    type = batchNumericType;
                }
                if (type != null) {
                    addArray(key, value, type);
                }
            }
        }

        private void addArray(String key, Object value, ArrayType type) {
            List<?> elements = value instanceof List ? (List<?>) value : Arrays.asList((Object[]) value);
            switch (type) {
                case BIGINT:
                    longArrays.put(key, value instanceof Long[] ? (Long[]) value : elements.stream()
                            .map(element -> {
                                if (element == null) return null;
                                if (isInteger(element)) return toLong(element);
                                return 0L;
                            }).toArray(Long[]::new));
                    break;
                case DOUBLE:
                    doubleArrays.put(key, value instanceof Double[] ? (Double[]) value : elements.stream()
                            .map(element -> {
                                if (element == null) return null;
                                if (isInteger(element) || isFloatingPoint(element)) return toArrayDouble(key, element);
                                return 0.0;
                            }).toArray(Double[]::new));
                    break;
                case STRING:
                    stringArrays.put(key, value instanceof String[] ? (String[]) value : elements.stream()
                            .map(element -> element == null ? null : element.toString())
                            .toArray(String[]::new));
                    break;
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
