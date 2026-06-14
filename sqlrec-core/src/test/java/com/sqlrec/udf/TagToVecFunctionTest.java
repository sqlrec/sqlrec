package com.sqlrec.udf;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.udf.table.TagToVecFunction;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TagToVecFunctionTest {

    @Test
    public void testSingleTagPerRow() {
        // tags: A, B, A -> tagIndexMap: {A:0, B:1}
        CacheTable input = createTable(new Object[][]{
                {1, "A"},
                {2, "B"},
                {3, "A"}
        });

        TagToVecFunction function = new TagToVecFunction();
        CacheTable output = function.evaluate(input, "tag", "vec");

        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());
        assertEquals(3, result.get(0).length); // id, tag, vec

        assertArrayEquals(new Float[]{1.0f, 0.0f}, (Float[]) result.get(0)[2]);
        assertArrayEquals(new Float[]{0.0f, 1.0f}, (Float[]) result.get(1)[2]);
        assertArrayEquals(new Float[]{1.0f, 0.0f}, (Float[]) result.get(2)[2]);
    }

    @Test
    public void testArrayTagPerRow() {
        // tags: [A,B], [C], [A,C] -> tagIndexMap: {A:0, B:1, C:2}
        CacheTable input = createTable(new Object[][]{
                {1, Arrays.asList("A", "B")},
                {2, Arrays.asList("C")},
                {3, Arrays.asList("A", "C")}
        });

        TagToVecFunction function = new TagToVecFunction();
        CacheTable output = function.evaluate(input, "tag", "vec");

        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());

        assertArrayEquals(new Float[]{1.0f, 1.0f, 0.0f}, (Float[]) result.get(0)[2]);
        assertArrayEquals(new Float[]{0.0f, 0.0f, 1.0f}, (Float[]) result.get(1)[2]);
        assertArrayEquals(new Float[]{1.0f, 0.0f, 1.0f}, (Float[]) result.get(2)[2]);
    }

    @Test
    public void testNullTagValue() {
        CacheTable input = createTable(new Object[][]{
                {1, "A"},
                {2, null},
                {3, "B"}
        });

        TagToVecFunction function = new TagToVecFunction();
        CacheTable output = function.evaluate(input, "tag", "vec");

        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());

        assertArrayEquals(new Float[]{1.0f, 0.0f}, (Float[]) result.get(0)[2]);
        assertArrayEquals(new Float[]{0.0f, 0.0f}, (Float[]) result.get(1)[2]);
        assertArrayEquals(new Float[]{0.0f, 1.0f}, (Float[]) result.get(2)[2]);
    }

    @Test
    public void testEmptyTable() {
        CacheTable input = createTable(new Object[][]{});

        TagToVecFunction function = new TagToVecFunction();
        CacheTable output = function.evaluate(input, "tag", "vec");

        List<Object[]> result = collectRows(output);
        assertEquals(0, result.size());
    }

    @Test
    public void testOutputColumnNameExists() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});

        TagToVecFunction function = new TagToVecFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "tag", "tag");
        });
    }

    @Test
    public void testTagColumnNotFound() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});

        TagToVecFunction function = new TagToVecFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "nonexistent", "vec");
        });
    }

    @Test
    public void testEmptyTagColName() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});

        TagToVecFunction function = new TagToVecFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "", "vec");
        });
    }

    @Test
    public void testEmptyOutputColName() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});

        TagToVecFunction function = new TagToVecFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "tag", "");
        });
    }

    @Test
    public void testOutputSchema() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});

        TagToVecFunction function = new TagToVecFunction();
        CacheTable output = function.evaluate(input, "tag", "vec");

        List<RelDataTypeField> fields = output.getDataFields();
        assertEquals(3, fields.size());
        assertEquals("id", fields.get(0).getName());
        assertEquals("tag", fields.get(1).getName());
        assertEquals("vec", fields.get(2).getName());
        assertEquals(SqlTypeName.ARRAY, fields.get(2).getType().getSqlTypeName());
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
        fields.add(DataTypeUtils.getRelDataTypeField("tag", 1, SqlTypeName.VARCHAR));
        return fields;
    }

    private List<Object[]> collectRows(CacheTable table) {
        List<Object[]> result = new ArrayList<>();
        table.scan(null).forEach(result::add);
        return result;
    }
}
