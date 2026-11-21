package com.sqlrec.common.utils;

import java.util.ArrayList;
import java.util.List;

/** Vector conversion, normalization and similarity operations. */
public final class VectorMathUtils {
    private VectorMathUtils() {
    }

    public static List<Float> convertToFloatVec(Object obj) {
        if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            List<Float> floatList = new ArrayList<>(list.size());
            for (Object element : list) {
                if (element instanceof Number) {
                    floatList.add(((Number) element).floatValue());
                } else {
                    throw new IllegalArgumentException("list contains non-number element");
                }
            }
            return floatList;
        }
        throw new IllegalArgumentException("obj is not list");
    }

    /**
     * Convert a vector object to double array.
     * Supports List<? extends Number>, double[], float[].
     */
    public static double[] toDoubleArray(Object vecObj) {
        if (vecObj instanceof List) {
            List<?> list = (List<?>) vecObj;
            double[] arr = new double[list.size()];
            for (int i = 0; i < list.size(); i++) {
                arr[i] = ((Number) list.get(i)).doubleValue();
            }
            return arr;
        }
        if (vecObj instanceof double[]) {
            return (double[]) vecObj;
        }
        if (vecObj instanceof float[]) {
            float[] floats = (float[]) vecObj;
            double[] arr = new double[floats.length];
            for (int i = 0; i < floats.length; i++) {
                arr[i] = floats[i];
            }
            return arr;
        }
        throw new IllegalArgumentException("Unsupported vector type: " + vecObj.getClass().getName());
    }

    /**
     * L2 normalize a double array in place.
     */
    public static void l2Normalize(double[] vec) {
        double norm = 0.0;
        for (double v : vec) {
            norm += v * v;
        }
        norm = Math.sqrt(norm);
        if (norm > 1e-10) {
            for (int i = 0; i < vec.length; i++) {
                vec[i] /= norm;
            }
        }
    }

    /**
     * L2 normalize a number list, returning a new List<Double>.
     */
    public static List<Double> l2NormalizeList(List<?> list) {
        List<Double> result = new ArrayList<>(list.size());
        double sum = 0;
        double largest = 0;
        for (Object element : list) {
            double value = toDouble(element);
            result.add(value);
            sum += Math.pow(value, 2);
            largest = Math.max(largest, Math.abs(value));
        }
        if (largest == 0) {
            return result;
        }

        double scale = 1;
        if ((sum < Double.MIN_NORMAL || Double.isInfinite(sum)) && Double.isFinite(largest)) {
            // Scale before squaring to avoid overflow and underflow in the norm.
            scale = largest;
            sum = 0;
            for (double value : result) {
                double scaled = value / scale;
                sum += scaled * scaled;
            }
        }
        double norm = Math.sqrt(sum);
        for (int i = 0; i < result.size(); i++) {
            result.set(i, (result.get(i) / scale) / norm);
        }
        return result;
    }

    /**
     * Compute inner product of two number lists.
     */
    public static double innerProduct(List<?> list1, List<?> list2) {
        if (list1.size() != list2.size()) {
            throw new IllegalArgumentException("vectors must have same length");
        }
        double ip = 0.0;
        for (int i = 0; i < list1.size(); i++) {
            ip += toDouble(list1.get(i)) * toDouble(list2.get(i));
        }
        return ip;
    }

    /**
     * Converts an object to double, handling Number, Hadoop Writable, and String types.
     */
    private static double toDouble(Object o) {
        if (o == null) {
            throw new IllegalArgumentException("element is null");
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        }
        // Handle Hadoop Writable types (e.g. DoubleWritable) and other numeric types
        return Double.parseDouble(o.toString());
    }
}
