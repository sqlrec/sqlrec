package com.sqlrec.udf.table;

public class SleepFunction {
    public Void evaluate(String millisStr) {
        if (millisStr == null) {
            throw new IllegalArgumentException("millis parameter cannot be null");
        }

        long millis;
        try {
            millis = Long.parseLong(millisStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("millis parameter must be a valid long integer");
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
