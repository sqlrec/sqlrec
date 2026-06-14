package com.sqlrec.udf;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.udf.table.WeightedMergeFunction;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class WeightedMergeFunctionTest {

    @Test
    public void testBasicWeightedMerge() {
        // Table1: [1,A], [2,B], [3,C]
        // Table2: [4,D], [5,E], [6,F]
        // Weights: 2,1
        // Round1: T1 takes 2 (1,A;2,B), T2 takes 1 (4,D)
        // Round2: T1 takes 1 (3,C), T2 takes 1 (5,E)
        // Round3: T1 exhausted, T2 takes 1 (6,F)
        CacheTable t1 = createTable(new Object[][]{{1, "A"}, {2, "B"}, {3, "C"}});
        CacheTable t2 = createTable(new Object[][]{{4, "D"}, {5, "E"}, {6, "F"}});

        WeightedMergeFunction function = new WeightedMergeFunction();
        CacheTable output = function.evaluate("0", "2,1", "10", t1, t2);

        List<Object[]> result = collectRows(output);
        assertEquals(6, result.size());
        assertArrayEquals(new Object[]{1, "A"}, result.get(0));
        assertArrayEquals(new Object[]{2, "B"}, result.get(1));
        assertArrayEquals(new Object[]{4, "D"}, result.get(2));
        assertArrayEquals(new Object[]{3, "C"}, result.get(3));
        assertArrayEquals(new Object[]{5, "E"}, result.get(4));
        assertArrayEquals(new Object[]{6, "F"}, result.get(5));
    }

    @Test
    public void testDedup() {
        // Table1: [1,A], [2,B]
        // Table2: [3,C], [2,B2]  -- id=2 is duplicate, appears after T1's
        // Weights: 1,1
        // Round1: T1(1,A), T2(3,C)
        // Round2: T1(2,B), T2(2,B2) dup -> skip
        CacheTable t1 = createTable(new Object[][]{{1, "A"}, {2, "B"}});
        CacheTable t2 = createTable(new Object[][]{{3, "C"}, {2, "B2"}});

        WeightedMergeFunction function = new WeightedMergeFunction();
        CacheTable output = function.evaluate("0", "1,1", "10", t1, t2);

        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());
        assertArrayEquals(new Object[]{1, "A"}, result.get(0));
        assertArrayEquals(new Object[]{3, "C"}, result.get(1));
        assertArrayEquals(new Object[]{2, "B"}, result.get(2));
    }

    @Test
    public void testDedupDoesNotConsumeWeightQuota() {
        // Table1: [1,A], [2,B]
        // Table2: [1,A2], [3,C], [4,D]
        // Weights: 1,2 => T1 takes 1 (1,A); T2 should take 2 non-dup: skip 1,A2, take 3,C and 4,D
        CacheTable t1 = createTable(new Object[][]{{1, "A"}, {2, "B"}});
        CacheTable t2 = createTable(new Object[][]{{1, "A2"}, {3, "C"}, {4, "D"}});

        WeightedMergeFunction function = new WeightedMergeFunction();
        CacheTable output = function.evaluate("0", "1,2", "10", t1, t2);

        List<Object[]> result = collectRows(output);
        assertEquals(4, result.size());
        // Round1: T1 takes 1 (1,A); T2 takes 2 non-dup: skip (1,A2), take (3,C), (4,D)
        assertArrayEquals(new Object[]{1, "A"}, result.get(0));
        assertArrayEquals(new Object[]{3, "C"}, result.get(1));
        assertArrayEquals(new Object[]{4, "D"}, result.get(2));
        // Round2: T1 takes 1 (2,B); T2 exhausted
        assertArrayEquals(new Object[]{2, "B"}, result.get(3));
    }

    @Test
    public void testLimit() {
        CacheTable t1 = createTable(new Object[][]{{1, "A"}, {2, "B"}, {3, "C"}});
        CacheTable t2 = createTable(new Object[][]{{4, "D"}, {5, "E"}, {6, "F"}});

        WeightedMergeFunction function = new WeightedMergeFunction();
        CacheTable output = function.evaluate("0", "1,1", "3", t1, t2);

        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());
        assertArrayEquals(new Object[]{1, "A"}, result.get(0));
        assertArrayEquals(new Object[]{4, "D"}, result.get(1));
        assertArrayEquals(new Object[]{2, "B"}, result.get(2));
    }

    @Test
    public void testThreeTableMerge() {
        CacheTable t1 = createTable(new Object[][]{{1, "A"}, {4, "D"}});
        CacheTable t2 = createTable(new Object[][]{{2, "B"}, {5, "E"}});
        CacheTable t3 = createTable(new Object[][]{{3, "C"}, {6, "F"}});

        WeightedMergeFunction function = new WeightedMergeFunction();
        CacheTable output = function.evaluate("0", "1,1,1", "10", t1, t2, t3);

        List<Object[]> result = collectRows(output);
        assertEquals(6, result.size());
        // Round1: T1(1,A), T2(2,B), T3(3,C)
        assertEquals(1, result.get(0)[0]);
        assertEquals(2, result.get(1)[0]);
        assertEquals(3, result.get(2)[0]);
        // Round2: T1(4,D), T2(5,E), T3(6,F)
        assertEquals(4, result.get(3)[0]);
        assertEquals(5, result.get(4)[0]);
        assertEquals(6, result.get(5)[0]);
    }

    @Test
    public void testUnevenWeights() {
        // Weights 3,1: T1 takes 3 per round, T2 takes 1
        CacheTable t1 = createTable(new Object[][]{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}});
        CacheTable t2 = createTable(new Object[][]{{5, "E"}, {6, "F"}});

        WeightedMergeFunction function = new WeightedMergeFunction();
        CacheTable output = function.evaluate("0", "3,1", "10", t1, t2);

        List<Object[]> result = collectRows(output);
        assertEquals(6, result.size());
        // Round1: T1 takes 3, T2 takes 1
        assertArrayEquals(new Object[]{1, "A"}, result.get(0));
        assertArrayEquals(new Object[]{2, "B"}, result.get(1));
        assertArrayEquals(new Object[]{3, "C"}, result.get(2));
        assertArrayEquals(new Object[]{5, "E"}, result.get(3));
        // Round2: T1 takes 1, T2 takes 1
        assertArrayEquals(new Object[]{4, "D"}, result.get(4));
        assertArrayEquals(new Object[]{6, "F"}, result.get(5));
    }

    @Test
    public void testEmptyTable() {
        CacheTable t1 = createTable(new Object[][]{{1, "A"}});
        CacheTable t2 = createTable(new Object[][]{});

        WeightedMergeFunction function = new WeightedMergeFunction();
        CacheTable output = function.evaluate("0", "1,1", "10", t1, t2);

        List<Object[]> result = collectRows(output);
        assertEquals(1, result.size());
        assertArrayEquals(new Object[]{1, "A"}, result.get(0));
    }

    @Test
    public void testSchemaMismatchColumnCount() {
        List<RelDataTypeField> fields2 = new ArrayList<>(createTestDataFields());
        fields2.add(DataTypeUtils.getRelDataTypeField("extra", 2, SqlTypeName.VARCHAR));

        List<Object[]> rows2 = new ArrayList<>();
        rows2.add(new Object[]{1, "A", "X"});
        CacheTable t1 = createTable(new Object[][]{{1, "A"}});
        CacheTable t2 = new CacheTable("t2", Linq4j.asEnumerable(rows2), fields2);

        WeightedMergeFunction function = new WeightedMergeFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate("0", "1,1", "10", t1, t2);
        });
    }

    @Test
    public void testSchemaMismatchColumnName() {
        List<RelDataTypeField> fields2 = new ArrayList<>();
        fields2.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        fields2.add(DataTypeUtils.getRelDataTypeField("name", 1, SqlTypeName.INTEGER));

        List<Object[]> rows2 = new ArrayList<>();
        rows2.add(new Object[]{1, 1});
        CacheTable t1 = createTable(new Object[][]{{1, "A"}});
        CacheTable t2 = new CacheTable("t2", Linq4j.asEnumerable(rows2), fields2);

        WeightedMergeFunction function = new WeightedMergeFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate("0", "1,1", "10", t1, t2);
        });
    }

    @Test
    public void testInvalidPrimaryKeyIndex() {
        CacheTable t1 = createTable(new Object[][]{{1, "A"}});

        WeightedMergeFunction function = new WeightedMergeFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate("5", "1", "10", t1);
        });
    }

    @Test
    public void testWeightCountMismatch() {
        CacheTable t1 = createTable(new Object[][]{{1, "A"}});
        CacheTable t2 = createTable(new Object[][]{{2, "B"}});

        WeightedMergeFunction function = new WeightedMergeFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate("0", "1", "10", t1, t2);
        });
    }

    @Test
    public void testNullPrimaryKeyValue() {
        // T1: (null,A), (2,B)
        // T2: (null,C), (3,D)
        // Weights: 1,1
        // Round1: T1 takes 1 (null,A); T2 takes 1: skip (null,C) dup, take (3,D)
        // Round2: T1 takes 1 (2,B); T2 exhausted
        CacheTable t1 = createTable(new Object[][]{{null, "A"}, {2, "B"}});
        CacheTable t2 = createTable(new Object[][]{{null, "C"}, {3, "D"}});

        WeightedMergeFunction function = new WeightedMergeFunction();
        CacheTable output = function.evaluate("0", "1,1", "10", t1, t2);

        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());
        assertArrayEquals(new Object[]{null, "A"}, result.get(0));
        assertArrayEquals(new Object[]{3, "D"}, result.get(1));
        assertArrayEquals(new Object[]{2, "B"}, result.get(2));
    }

    private CacheTable createTable(Object[][] data) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        return new CacheTable("test", Linq4j.asEnumerable(rows), createTestDataFields());
    }

    private List<RelDataTypeField> createTestDataFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("name", 1, SqlTypeName.VARCHAR));
        return fields;
    }

    private List<Object[]> collectRows(CacheTable table) {
        List<Object[]> result = new ArrayList<>();
        table.scan(null).forEach(result::add);
        return result;
    }
}
