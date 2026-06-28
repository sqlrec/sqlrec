package com.sqlrec.frontend.cli;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SqlOutputFormatter}.
 * Uses {@link FakeRelDataTypeField} so the tests stay free of Calcite schema setup.
 */
class SqlOutputFormatterTest {

    private static List<RelDataTypeField> fields(String... names) {
        java.util.List<RelDataTypeField> list = new java.util.ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            list.add(new FakeRelDataTypeField(names[i], i));
        }
        return list;
    }

    // ===================== quoteField =====================

    @Test
    void quoteFieldSimpleValueUnchanged() {
        assertEquals("abc", SqlOutputFormatter.quoteField("abc", ","));
    }

    @Test
    void quoteFieldValueWithSeparatorIsQuoted() {
        assertEquals("\"a,b\"", SqlOutputFormatter.quoteField("a,b", ","));
    }

    @Test
    void quoteFieldValueWithoutSeparatorUnchangedEvenIfContainsTab() {
        // For CSV (comma separator), a tab does not trigger quoting
        assertEquals("a\tb", SqlOutputFormatter.quoteField("a\tb", ","));
    }

    @Test
    void quoteFieldTsvValueWithTabIsQuoted() {
        assertEquals("\"a\tb\"", SqlOutputFormatter.quoteField("a\tb", "\t"));
    }

    @Test
    void quoteFieldValueWithDoubleQuoteIsQuotedAndDoubled() {
        assertEquals("\"a\"\"b\"", SqlOutputFormatter.quoteField("a\"b", ","));
    }

    @Test
    void quoteFieldValueWithNewlineIsQuoted() {
        assertEquals("\"a\nb\"", SqlOutputFormatter.quoteField("a\nb", ","));
    }

    @Test
    void quoteFieldValueWithCarriageReturnIsQuoted() {
        assertEquals("\"a\rb\"", SqlOutputFormatter.quoteField("a\rb", ","));
    }

    @Test
    void quoteFieldEmptyValueUnchanged() {
        assertEquals("", SqlOutputFormatter.quoteField("", ","));
    }

    @Test
    void quoteFieldSeparatorOnlyValueIsQuoted() {
        assertEquals("\",\"", SqlOutputFormatter.quoteField(",", ","));
    }

    // ===================== format: csv =====================

    @Test
    void formatCsvHeaderAndRows() {
        List<RelDataTypeField> f = fields("id", "name");
        List<Object[]> rows = Arrays.asList(new Object[]{1, "alice"}, new Object[]{2, "bob"});

        List<String> lines = SqlOutputFormatter.format(rows, f, "csv");

        assertEquals(Arrays.asList("id,name", "1,alice", "2,bob"), lines);
    }

    @Test
    void formatCsvNullValueRenderedAsNULL() {
        List<RelDataTypeField> f = fields("a");
        List<Object[]> rows = Collections.singletonList(new Object[]{null});

        List<String> lines = SqlOutputFormatter.format(rows, f, "csv");

        assertEquals(Arrays.asList("a", "NULL"), lines);
    }

    @Test
    void formatCsvValueWithCommaIsQuoted() {
        List<RelDataTypeField> f = fields("a");
        List<Object[]> rows = Collections.singletonList(new Object[]{"x,y"});

        List<String> lines = SqlOutputFormatter.format(rows, f, "csv");

        assertEquals(Arrays.asList("a", "\"x,y\""), lines);
    }

    @Test
    void formatCsvEmptyRowsStillEmitsHeader() {
        List<RelDataTypeField> f = fields("id", "name");
        List<Object[]> rows = Collections.emptyList();

        List<String> lines = SqlOutputFormatter.format(rows, f, "csv");

        assertEquals(Collections.singletonList("id,name"), lines);
    }

    @Test
    void formatCsvIsCaseInsensitive() {
        List<RelDataTypeField> f = fields("a");
        List<Object[]> rows = Collections.singletonList(new Object[]{1});

        List<String> upper = SqlOutputFormatter.format(rows, f, "CSV");
        List<String> mixed = SqlOutputFormatter.format(rows, f, "CsV");

        assertEquals(Arrays.asList("a", "1"), upper);
        assertEquals(Arrays.asList("a", "1"), mixed);
    }

    // ===================== format: tsv =====================

    @Test
    void formatTsvUsesTabSeparator() {
        List<RelDataTypeField> f = fields("id", "name");
        List<Object[]> rows = Collections.singletonList(new Object[]{1, "alice"});

        List<String> lines = SqlOutputFormatter.format(rows, f, "tsv");

        assertEquals(Arrays.asList("id\tname", "1\talice"), lines);
    }

    @Test
    void formatTsvValueWithTabIsQuoted() {
        List<RelDataTypeField> f = fields("a");
        List<Object[]> rows = Collections.singletonList(new Object[]{"x\ty"});

        List<String> lines = SqlOutputFormatter.format(rows, f, "tsv");

        assertEquals(Arrays.asList("a", "\"x\ty\""), lines);
    }

    // ===================== format: json =====================

    @Test
    void formatJsonStructure() {
        List<RelDataTypeField> f = fields("id", "name");
        List<Object[]> rows = Arrays.asList(new Object[]{1, "alice"}, new Object[]{2, "bob"});

        List<String> lines = SqlOutputFormatter.format(rows, f, "json");

        assertEquals(4, lines.size());
        assertEquals("[", lines.get(0));
        assertEquals("]", lines.get(3));
        // Rows are wrapped with 2-space indent; the second-to-last row ends with a comma
        assertTrue(lines.get(1).startsWith("  "), "row should be indented");
        assertTrue(lines.get(1).endsWith(","), "non-last row should end with comma");
        assertFalse(lines.get(2).endsWith(","), "last row should not end with comma");
    }

    @Test
    void formatJsonEmptyRowsEmitsEmptyArray() {
        List<RelDataTypeField> f = fields("a");
        List<Object[]> rows = Collections.emptyList();

        List<String> lines = SqlOutputFormatter.format(rows, f, "json");

        assertEquals(Arrays.asList("[", "]"), lines);
    }

    // ===================== format: table / fallback =====================

    @Test
    void formatTableProducesNonEmptyOutputForRows() {
        List<RelDataTypeField> f = fields("id", "name");
        List<Object[]> rows = Collections.singletonList(new Object[]{1, "alice"});

        List<String> lines = SqlOutputFormatter.format(rows, f, "table");

        // DataTransformUtils.formatAsTable draws a header + separator + data row
        assertFalse(lines.isEmpty());
        // Header line should contain the column name
        assertTrue(lines.stream().anyMatch(l -> l.contains("id") && l.contains("name")));
    }

    @Test
    void formatUnknownFormatFallsBackToTable() {
        List<RelDataTypeField> f = fields("id");
        List<Object[]> rows = Collections.singletonList(new Object[]{1});

        List<String> unknown = SqlOutputFormatter.format(rows, f, "xml");
        List<String> table = SqlOutputFormatter.format(rows, f, "table");

        assertEquals(table, unknown);
    }

    @Test
    void formatNullFormatFallsBackToTable() {
        List<RelDataTypeField> f = fields("id");
        List<Object[]> rows = Collections.singletonList(new Object[]{1});

        List<String> withNull = SqlOutputFormatter.format(rows, f, null);
        List<String> table = SqlOutputFormatter.format(rows, f, "table");

        assertEquals(table, withNull);
    }
}
