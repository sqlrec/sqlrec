package com.sqlrec.udf.scalar;

import java.util.List;

public class L2NormFunction {
    public Object eval(Object vector) {
        if (vector == null) {
            return null;
        }

        if (vector instanceof List) {
            List<?> list = (List<?>) vector;
            double sum = 0;
            for (Object o : list) {
                if (o instanceof Number) {
                    sum += Math.pow(((Number) o).doubleValue(), 2);
                } else {
                    throw new RuntimeException("L2NormFunction only support number list");
                }
            }

            if (sum <= 0) {
                return vector;
            }
            double norm = Math.sqrt(sum);

            List<Double> result = new java.util.ArrayList<>();
            for (Object o : list) {
                result.add(((Number) o).doubleValue() / norm);
            }
            return result;
        } else {
            throw new RuntimeException("L2NormFunction only support number list");
        }
    }
}
