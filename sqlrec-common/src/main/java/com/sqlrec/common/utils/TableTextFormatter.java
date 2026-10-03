package com.sqlrec.common.utils;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;

/** Formats query results as an ASCII table for CLI and diagnostic output. */
public final class TableTextFormatter {
    private TableTextFormatter() {
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

        String separator = buildTableSeparator(colWidths);
        lines.add(separator);
        lines.add(buildTableRow(headers, colWidths));
        lines.add(separator);
        for (String[] row : displayRows) {
            lines.add(buildTableRow(row, colWidths));
        }
        lines.add(separator);

        return lines;
    }

    private static String buildTableSeparator(int[] colWidths) {
        StringBuilder sb = new StringBuilder();
        for (int colWidth : colWidths) {
            sb.append("+").append("-".repeat(colWidth + 2));
        }
        sb.append("+");
        return sb.toString();
    }

    private static String buildTableRow(String[] values, int[] colWidths) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < colWidths.length; i++) {
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
