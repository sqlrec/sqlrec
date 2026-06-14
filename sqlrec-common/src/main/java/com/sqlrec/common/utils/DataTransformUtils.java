package com.sqlrec.common.utils;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.*;
import java.util.stream.Collectors;

public class DataTransformUtils {
    public static List<Float> convertToFloatVec(Object obj) {
        if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            List<Float> floatList = new ArrayList<>(list.size());
            for (Object o : list) {
                if (o instanceof Number) {
                    floatList.add(((Number) o).floatValue());
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
        } else if (vecObj instanceof double[]) {
            return (double[]) vecObj;
        } else if (vecObj instanceof float[]) {
            float[] farr = (float[]) vecObj;
            double[] arr = new double[farr.length];
            for (int i = 0; i < farr.length; i++) {
                arr[i] = farr[i];
            }
            return arr;
        } else {
            throw new IllegalArgumentException("Unsupported vector type: " + vecObj.getClass().getName());
        }
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
        double sum = 0;
        for (Object o : list) {
            if (!(o instanceof Number)) {
                throw new IllegalArgumentException("list contains non-number element");
            }
            sum += Math.pow(((Number) o).doubleValue(), 2);
        }

        if (sum <= 0) {
            List<Double> result = new ArrayList<>(list.size());
            for (Object o : list) {
                result.add(((Number) o).doubleValue());
            }
            return result;
        }

        double norm = Math.sqrt(sum);
        List<Double> result = new ArrayList<>(list.size());
        for (Object o : list) {
            result.add(((Number) o).doubleValue() / norm);
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
            if (!(list1.get(i) instanceof Number) || !(list2.get(i) instanceof Number)) {
                throw new IllegalArgumentException("vectors must contain numbers only");
            }
            ip += ((Number) list1.get(i)).doubleValue() * ((Number) list2.get(i)).doubleValue();
        }
        return ip;
    }

    public static List<Map<String, Object>> convertToMapList(List<Object[]> results, List<RelDataTypeField> fields) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object[] row : results) {
            Map<String, Object> map = new LinkedHashMap<>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                RelDataTypeField field = fields.get(i);
                if (row.length > i && row[i] != null) {
                    map.put(field.getName(), row[i]);
                }
            }
            result.add(map);
        }
        return result;
    }

    public static Enumerable<Object[]> convertDataToEnumerable(
            List<Map<String, Object>> data,
            List<RelDataTypeField> dataFields
    ) {
        List<Object[]> list = new ArrayList<>();
        for (Map<String, Object> map : data) {
            Object[] row = new Object[dataFields.size()];
            for (int i = 0; i < dataFields.size(); i++) {
                RelDataTypeField field = dataFields.get(i);
                row[i] = map.get(field.getName());
            }
            list.add(row);
        }

        return Linq4j.asEnumerable(list);
    }

    public static Enumerable<Object[]> getJsonValueEnumerable(Enumerable<Object[]> enumerable, int index) {
        if (enumerable == null) {
            return null;
        }

        List<Object[]> list = new ArrayList<>();
        for (Object[] objects : enumerable) {
            Object object = objects[index];
            Object[] newObjects = new Object[1];
            if (object == null) {
                newObjects[0] = null;
            } else {
                newObjects[0] = JsonUtils.toJson(object);
            }
            list.add(newObjects);
        }
        return Linq4j.asEnumerable(list);
    }

    public static Enumerable<Object[]> getMsgEnumerable(String msg) {
        if (msg == null) {
            return null;
        }
        return Linq4j.asEnumerable(Collections.singletonList(new String[]{msg}));
    }

    public static Enumerable<Object[]> convertListToEnumerable(List<String> list) {
        if (list == null) {
            return null;
        }
        return Linq4j.asEnumerable(list.stream().map(o -> new String[]{o}).collect(Collectors.toList()));
    }

    public static <T> Enumerable<Object[]> convertListToArrayToEnumerable(List<List<T>> list) {
        if (list == null) {
            return null;
        }
        return Linq4j.asEnumerable(list.stream().map(List::toArray).collect(Collectors.toList()));
    }

    public static List<String> formatAsTable(Enumerable<Object[]> enumerable, List<RelDataTypeField> fields) {
        List<String> lines = new ArrayList<>();
        if (enumerable == null || fields == null || fields.isEmpty()) {
            return lines;
        }

        int colCount = fields.size();
        String[] headers = new String[colCount];
        int[] colWidths = new int[colCount];

        for (int i = 0; i < colCount; i++) {
            headers[i] = fields.get(i).getName();
            colWidths[i] = headers[i].length();
        }

        List<String[]> displayRows = new ArrayList<>();
        for (Object[] row : enumerable) {
            String[] strRow = new String[colCount];
            for (int i = 0; i < colCount; i++) {
                strRow[i] = row != null && i < row.length && row[i] != null ? String.valueOf(row[i]) : "null";
                if (strRow[i].length() > colWidths[i]) {
                    colWidths[i] = strRow[i].length();
                }
            }
            displayRows.add(strRow);
        }

        String separator = buildTableSeparator(colWidths, colCount);
        lines.add(separator);
        lines.add(buildTableRow(headers, colWidths, colCount));
        lines.add(separator);
        for (String[] row : displayRows) {
            lines.add(buildTableRow(row, colWidths, colCount));
        }
        lines.add(separator);

        return lines;
    }

    private static String buildTableSeparator(int[] colWidths, int colCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < colCount; i++) {
            sb.append("+").append("-".repeat(colWidths[i] + 2));
        }
        sb.append("+");
        return sb.toString();
    }

    private static String buildTableRow(String[] values, int[] colWidths, int colCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < colCount; i++) {
            sb.append("| ").append(padRight(values[i], colWidths[i])).append(" ");
        }
        sb.append("|");
        return sb.toString();
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }
}
