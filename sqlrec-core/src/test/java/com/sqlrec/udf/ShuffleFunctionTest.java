package com.sqlrec.udf;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.udf.table.ShuffleFunction;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class ShuffleFunctionTest {

    @Test
    public void testShufflePreservesAllRows() {
        CacheTable input = createTable(new Object[][]{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}});
        ShuffleFunction function = new ShuffleFunction();
        CacheTable output = function.evaluate(input);

        List<Object[]> result = collectRows(output);
        assertEquals(5, result.size());

        Set<Integer> ids = new HashSet<>();
        for (Object[] row : result) {
            ids.add((Integer) row[0]);
        }
        assertEquals(5, ids.size());
        assertTrue(ids.contains(1));
        assertTrue(ids.contains(2));
        assertTrue(ids.contains(3));
        assertTrue(ids.contains(4));
        assertTrue(ids.contains(5));
    }

    @Test
    public void testShufflePreservesSchema() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        ShuffleFunction function = new ShuffleFunction();
        CacheTable output = function.evaluate(input);

        assertEquals(input.getDataFields(), output.getDataFields());
    }

    @Test
    public void testShuffleEmptyTable() {
        CacheTable input = createTable(new Object[][]{});
        ShuffleFunction function = new ShuffleFunction();
        CacheTable output = function.evaluate(input);

        List<Object[]> result = collectRows(output);
        assertEquals(0, result.size());
    }

    @Test
    public void testShuffleSingleRow() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        ShuffleFunction function = new ShuffleFunction();
        CacheTable output = function.evaluate(input);

        List<Object[]> result = collectRows(output);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0)[0]);
        assertEquals("A", result.get(0)[1]);
    }

    @Test
    public void testShufflePreservesRowData() {
        CacheTable input = createTable(new Object[][]{{1, "A"}, {2, "B"}, {3, "C"}});
        ShuffleFunction function = new ShuffleFunction();
        CacheTable output = function.evaluate(input);

        List<Object[]> result = collectRows(output);
        for (Object[] row : result) {
            int id = (Integer) row[0];
            String name = (String) row[1];
            if (id == 1) assertEquals("A", name);
            if (id == 2) assertEquals("B", name);
            if (id == 3) assertEquals("C", name);
        }
    }

    @Test
    public void testShuffleTableName() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        ShuffleFunction function = new ShuffleFunction();
        CacheTable output = function.evaluate(input);

        assertEquals("output", output.getTableName());
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
        fields.add(DataTypeUtils.getRelDataTypeField("name", 1, SqlTypeName.VARCHAR));
        return fields;
    }

    private List<Object[]> collectRows(CacheTable table) {
        List<Object[]> result = new ArrayList<>();
        table.scan(null).forEach(result::add);
        return result;
    }
}
