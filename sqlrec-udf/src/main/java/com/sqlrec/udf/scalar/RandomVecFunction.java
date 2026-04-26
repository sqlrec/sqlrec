package com.sqlrec.udf.scalar;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomVecFunction extends UDF {
    private static final Random random = new Random();

    public Object evaluate(String dimensionStr) {
        if (dimensionStr == null) {
            return null;
        }

        int dimension;
        try {
            dimension = Integer.parseInt(dimensionStr);
        } catch (NumberFormatException e) {
            throw new RuntimeException("RandomVecFunction dimension must be a valid integer string");
        }

        if (dimension <= 0) {
            throw new RuntimeException("RandomVecFunction dimension must be positive");
        }

        List<Double> vector = new ArrayList<>();
        double sum = 0;

        for (int i = 0; i < dimension; i++) {
            double value = random.nextDouble();
            vector.add(value);
            sum += value * value;
        }

        if (sum <= 0) {
            return vector;
        }

        double norm = Math.sqrt(sum);
        List<Double> result = new ArrayList<>();
        for (Double value : vector) {
            result.add(value / norm);
        }

        return result;
    }
}
