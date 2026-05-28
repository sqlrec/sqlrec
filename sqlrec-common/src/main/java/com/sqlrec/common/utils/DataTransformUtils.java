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
