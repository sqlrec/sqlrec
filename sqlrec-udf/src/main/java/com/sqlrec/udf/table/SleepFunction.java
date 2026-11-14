package com.sqlrec.udf.table;

public class SleepFunction {
    public Void evaluate(Long millis) {
        if (millis == null) {
            throw new IllegalArgumentException("millis parameter cannot be null");
        }

        if (millis < 0) {
            throw new IllegalArgumentException("millis parameter must be non-negative");
        }

        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Sleep was interrupted", e);
        }

        return null;
    }
}
