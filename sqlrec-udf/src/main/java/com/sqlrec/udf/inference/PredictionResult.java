package com.sqlrec.udf.inference;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** Reads column predictions while preserving the distinction between missing rows and null values. */
public final class PredictionResult {
    private final Map<String, Object> predictions;

    public PredictionResult(Map<String, Object> predictions) {
        this.predictions = predictions;
    }

    /** Online callers append null for missing fields or short prediction columns. */
    public Object getValue(String fieldName, int rowIndex) {
        Object prediction = predictions.get(fieldName);
        return hasRow(prediction, rowIndex) ? valueAt(prediction, rowIndex) : null;
    }

    /** Batch callers only overwrite inputs when a prediction row is present, including explicit nulls. */
    public void forEachValue(int rowIndex, BiConsumer<String, Object> consumer) {
        if (predictions == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : predictions.entrySet()) {
            Object prediction = entry.getValue();
            if (hasRow(prediction, rowIndex)) {
                consumer.accept(entry.getKey(), valueAt(prediction, rowIndex));
            }
        }
    }

    private static boolean hasRow(Object prediction, int rowIndex) {
        return !(prediction instanceof List) || rowIndex < ((List<?>) prediction).size();
    }

    private static Object valueAt(Object prediction, int rowIndex) {
        return prediction instanceof List ? ((List<?>) prediction).get(rowIndex) : prediction;
    }
}
