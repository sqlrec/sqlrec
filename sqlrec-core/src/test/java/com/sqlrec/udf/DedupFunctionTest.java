package com.sqlrec.udf;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.udf.table.DedupFunction;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DedupFunctionTest {

    @Test
    public void testBasicDedup() {
        CacheTable input = createTable(new Object[][]{{1, "A"}, {2, "B"}, {3, "C"}});
        CacheTable dedup = createTable(new Object[][]{{2, "X"}});
        DedupFunction function = new DedupFunction();
        CacheTable output = function.evaluate(input, dedup, "id", "id");

        List<Object[]> result = collectRows(output);
        assertEquals(2, result.size());
        assertEquals(1, result.get(0)[0]);
        assertEquals(3, result.get(1)[0]);
    }

    @Test
    public void testNoDuplicates() {
        CacheTable input = createTable(new Object[][]{{1, "A"}, {2, "B"}});
        CacheTable dedup = createTable(new Object[][]{{3, "X"}, {4, "Y"}});
        DedupFunction function = new DedupFunction();
        CacheTable output = function.evaluate(input, dedup, "id", "id");

        List<Object[]> result = collectRows(output);
        assertEquals(2, result.size());
    }

    @Test
    public void testAllDuplicates() {
        CacheTable input = createTable(new Object[][]{{1, "A"}, {2, "B"}});
        CacheTable dedup = createTable(new Object[][]{{1, "X"}, {2, "Y"}});
        DedupFunction function = new DedupFunction();
        CacheTable output = function.evaluate(input, dedup, "id", "id");

        List<Object[]> result = collectRows(output);
        assertEquals(0, result.size());
    }

    @Test
    public void testNullDedupTable() {
        CacheTable input = createTable(new Object[][]{{1, "A"}, {2, "B"}});
        DedupFunction function = new DedupFunction();
        CacheTable output = function.evaluate(input, null, "id", "id");

        List<Object[]> result = collectRows(output);
        assertEquals(2, result.size());
    }

    @Test
    public void testEmptyInput() {
        CacheTable input = createTable(new Object[][]{});
        CacheTable dedup = createTable(new Object[][]{{1, "X"}});
        DedupFunction function = new DedupFunction();
        CacheTable output = function.evaluate(input, dedup, "id", "id");

        List<Object[]> result = collectRows(output);
        assertEquals(0, result.size());
    }

    @Test
    public void testEmptyDedupTable() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        CacheTable dedup = createTable(new Object[][]{});
        DedupFunction function = new DedupFunction();
        CacheTable output = function.evaluate(input, dedup, "id", "id");

        List<Object[]> result = collectRows(output);
        assertEquals(1, result.size());
    }

    @Test
    public void testDifferentColumnNames() {
        CacheTable input = createTable(new Object[][]{{1, "A"}, {2, "B"}, {3, "C"}});
        CacheTable dedup = createDedupTable(new Object[][]{{"B"}, {"C"}});
        DedupFunction function = new DedupFunction();
        CacheTable output = function.evaluate(input, dedup, "name", "dedup_name");

        List<Object[]> result = collectRows(output);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0)[0]);
    }

    @Test
    public void testNullInputThrows() {
        CacheTable dedup = createTable(new Object[][]{{1, "X"}});
        DedupFunction function = new DedupFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(null, dedup, "id", "id");
        });
    }

    @Test
    public void testEmptyCol1Throws() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        DedupFunction function = new DedupFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, null, "", "id");
        });
    }

    @Test
    public void testEmptyCol2Throws() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        DedupFunction function = new DedupFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, null, "id", "");
        });
    }

    @Test
    public void testCol1NotFoundThrows() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        DedupFunction function = new DedupFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, null, "nonexistent", "id");
        });
    }

    @Test
    public void testCol2NotFoundThrows() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        CacheTable dedup = createTable(new Object[][]{{1, "X"}});
        DedupFunction function = new DedupFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, dedup, "id", "nonexistent");
        });
    }

    @Test
    public void testNullKeyValueDedup() {
        CacheTable input = createTable(new Object[][]{{null, "A"}, {1, "B"}});
        CacheTable dedup = createTable(new Object[][]{{null, "X"}});
        DedupFunction function = new DedupFunction();
        CacheTable output = function.evaluate(input, dedup, "id", "id");

        List<Object[]> result = collectRows(output);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0)[0]);
    }

    private CacheTable createTable(Object[][] data) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        return new CacheTable("test", Linq4j.asEnumerable(rows), createDataFields());
    }

    private CacheTable createDedupTable(Object[][] data) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("dedup_name", 0, SqlTypeName.VARCHAR));
        return new CacheTable("dedup", Linq4j.asEnumerable(rows), fields);
    }

    private List<RelDataTypeField> createDataFields() {
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
