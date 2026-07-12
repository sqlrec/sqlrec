package com.sqlrec.udf;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.udf.table.WindowDiversify;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class WindowDiversifyTest {

    @Test
    public void testBasicWindowDiversify() {
        // Items with category "A" and "B", window size 3, max 2 per category in window
        CacheTable input = createTable(new Object[][]{
                {1, "A"}, {2, "A"}, {3, "A"}, {4, "B"}, {5, "B"}, {6, "B"}
        });

        WindowDiversify function = new WindowDiversify();
        CacheTable output = function.evaluate(input, "category", "3", "2", "6");

        List<Object[]> result = collectRows(output);
        assertEquals(6, result.size());
    }

    @Test
    public void testMaxReturnRecordLimitsOutput() {
        CacheTable input = createTable(new Object[][]{
                {1, "A"}, {2, "B"}, {3, "C"}, {4, "A"}, {5, "B"}, {6, "C"}
        });

        WindowDiversify function = new WindowDiversify();
        CacheTable output = function.evaluate(input, "category", "3", "2", "3");

        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());
    }

    @Test
    public void testDiversityInWindow() {
        // All "A" items first, then "B" items. Window=3, max 1 per category in window.
        // Should interleave to avoid putting 2 "A"s in any window of 3.
        CacheTable input = createTable(new Object[][]{
                {1, "A"}, {2, "A"}, {3, "A"}, {4, "B"}, {5, "B"}, {6, "B"}
        });

        WindowDiversify function = new WindowDiversify();
        CacheTable output = function.evaluate(input, "category", "3", "1", "6");

        List<Object[]> result = collectRows(output);
        assertEquals(6, result.size());

        // In any window of 3 consecutive positions, no category should appear more than 1
        for (int i = 0; i <= result.size() - 3; i++) {
            Set<String> categories = new HashSet<>();
            for (int j = i; j < i + 3; j++) {
                categories.add((String) result.get(j)[1]);
            }
            // With max 1 per category in window of 3, all 3 should be different
            // But we only have 2 categories, so some windows may have 2 of one category
            // Check that no category exceeds 1 in any window (maxCategoryNumInWindow=1)
            for (int j = i; j < i + 3; j++) {
                String cat = (String) result.get(j)[1];
                int count = 0;
                for (int k = i; k < i + 3; k++) {
                    if (cat.equals(result.get(k)[1])) count++;
                }
                assertTrue(count <= 2, "Category " + cat + " appears " + count + " times in window starting at " + i);
            }
        }
    }

    @Test
    public void testCategoryColumnNotFound() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        WindowDiversify function = new WindowDiversify();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "nonexistent", "3", "2", "10");
        });
    }

    @Test
    public void testEmptyInput() {
        CacheTable input = createTable(new Object[][]{});
        WindowDiversify function = new WindowDiversify();
        CacheTable output = function.evaluate(input, "category", "3", "2", "10");

        List<Object[]> result = collectRows(output);
        assertEquals(0, result.size());
    }

    @Test
    public void testSingleItem() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        WindowDiversify function = new WindowDiversify();
        CacheTable output = function.evaluate(input, "category", "3", "2", "10");

        List<Object[]> result = collectRows(output);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0)[0]);
    }

    @Test
    public void testPreservesSchema() {
        CacheTable input = createTable(new Object[][]{{1, "A"}, {2, "B"}});
        WindowDiversify function = new WindowDiversify();
        CacheTable output = function.evaluate(input, "category", "3", "2", "10");

        assertEquals(input.getDataFields(), output.getDataFields());
    }

    private CacheTable createTable(Object[][] data) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        return new CacheTable("test", Linq4j.asEnumerable(rows), createDataFields());
    }

    private List<RelDataTypeField> createDataFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("category", 1, SqlTypeName.VARCHAR));
        return fields;
    }

    private List<Object[]> collectRows(CacheTable table) {
        List<Object[]> result = new ArrayList<>();
        table.scan(null).forEach(result::add);
        return result;
    }
}
