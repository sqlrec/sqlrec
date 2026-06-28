package com.sqlrec.frontend.cli;

import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.JsonUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;

/**
 * Renders SQL query result rows into printable text lines for the CLI.
 * <p>
 * Supported formats (matched case-insensitively, default {@code table}):
 * <ul>
 *     <li>{@code table} — ASCII box table via {@link DataTransformUtils#formatAsTable}</li>
 *     <li>{@code csv}  — comma-separated, RFC-4180 style quoting</li>
 *     <li>{@code tsv}  — tab-separated, same quoting rules</li>
 *     <li>{@code json} — one JSON object per row wrapped in a JSON array</li>
 * </ul>
 * The class produces {@code List<String>} lines rather than writing to a stream,
 * which keeps it easy to unit-test.
 */
public final class SqlOutputFormatter {

    public static final String TABLE = "table";
    public static final String CSV = "csv";
    public static final String TSV = "tsv";
    public static final String JSON = "json";

    private SqlOutputFormatter() {
    }

    /**
     * Format the given rows for the requested output format.
     *
     * @param rows         result rows; may be empty
     * @param fields       column descriptors (provides column names)
     * @param outputFormat one of {@code table}/{@code csv}/{@code tsv}/{@code json};
     *                     unknown values fall back to {@code table}
     * @return the lines to print; never {@code null}
     */
    public static List<String> format(List<Object[]> rows, List<RelDataTypeField> fields, String outputFormat) {
        String fmt = outputFormat == null ? TABLE : outputFormat.toLowerCase();
        switch (fmt) {
            case CSV:
                return delimited(rows, fields, ",");
            case TSV:
                return delimited(rows, fields, "\t");
            case JSON:
                return json(rows, fields);
            case TABLE:
            default:
                Enumerable<Object[]> enumerable = Linq4j.asEnumerable(rows);
                return DataTransformUtils.formatAsTable(enumerable, fields);
        }
    }

    private static List<String> delimited(List<Object[]> rows, List<RelDataTypeField> fields, String sep) {
        List<String> lines = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(sep);
            }
            sb.append(quoteField(fields.get(i).getName(), sep));
        }
        lines.add(sb.toString());

        for (Object[] row : rows) {
            sb.setLength(0);
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) {
                    sb.append(sep);
                }
                Object v = (row != null && i < row.length) ? row[i] : null;
                sb.append(quoteField(v == null ? "NULL" : String.valueOf(v), sep));
            }
            lines.add(sb.toString());
        }
        return lines;
    }

    private static List<String> json(List<Object[]> rows, List<RelDataTypeField> fields) {
        List<String> lines = new ArrayList<>();
        lines.add("[");
        for (int r = 0; r < rows.size(); r++) {
            String rowJson = JsonUtils.toJsonByFields(rows.get(r), fields);
            lines.add("  " + rowJson + (r < rows.size() - 1 ? "," : ""));
        }
        lines.add("]");
        return lines;
    }

    /**
     * Quote a field value for a delimited format, following RFC-4180 style:
     * if the value contains the separator, a double-quote, or a newline, wrap
     * it in double quotes and escape embedded double-quotes by doubling them.
     */
    public static String quoteField(String value, String sep) {
        if (value.contains(sep) || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
