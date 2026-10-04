package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

        assertEquals(Arrays.asList(1.0d, 0.0d), result.get(0)[2]);
        assertEquals(Arrays.asList(0.0d, 1.0d), result.get(1)[2]);
        assertEquals(Arrays.asList(1.0d, 0.0d), result.get(2)[2]);
    }

    @Test
    public void testArrayTagPerRow() {
        // tags: [A,B], [C], [A,C] -> tagIndexMap: {A:0, B:1, C:2}
        CacheTable input = createTable(new Object[][]{
                {1, Arrays.asList("A", "B")},
                {2, Arrays.asList("C")},
                {3, Arrays.asList("A", "C")}
        }, "ARRAY<VARCHAR>");

        TagToVecFunction function = new TagToVecFunction();
        CacheTable output = function.evaluate(input, "tag", "vec");

        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());

        assertEquals(Arrays.asList(1.0d, 1.0d, 0.0d), result.get(0)[2]);
        assertEquals(Arrays.asList(0.0d, 0.0d, 1.0d), result.get(1)[2]);
        assertEquals(Arrays.asList(1.0d, 0.0d, 1.0d), result.get(2)[2]);
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

        assertEquals(Arrays.asList(1.0d, 0.0d), result.get(0)[2]);
        assertEquals(Arrays.asList(0.0d, 0.0d), result.get(1)[2]);
        assertEquals(Arrays.asList(0.0d, 1.0d), result.get(2)[2]);
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
        assertEquals(SqlTypeName.FLOAT, fields.get(2).getType().getComponentType().getSqlTypeName());
    }

    private CacheTable createTable(Object[][] data) {
        return createTable(data, "VARCHAR");
    }

    private CacheTable createTable(Object[][] data, String tagType) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        return new CacheTable("test", Linq4j.asEnumerable(rows), createTestDataFields(tagType));
    }

    private List<RelDataTypeField> createTestDataFields(String tagType) {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("tag", 1, tagType));
        return fields;
    }

    private List<Object[]> collectRows(CacheTable table) {
        List<Object[]> result = new ArrayList<>();
        table.scan(null).forEach(result::add);
        return result;
    }

    @Test
    void vectorsSupportSqlSubscriptsAndCardinality() throws Exception {
        CacheTable output = new TagToVecFunction().evaluate(
                createTable(new Object[][]{{1, Arrays.asList("A", null, "B")}}, "ARRAY<VARCHAR>"),
                "tag", "vec");
        List<Object[]> rows = queryTable(output, "select vec[1], cardinality(vec) from vectors");
        assertEquals(1, rows.size());
        assertEquals(1.0f, ((Number) rows.get(0)[0]).floatValue());
        assertEquals(2, rows.get(0)[1]);
    }

    @Test
    void sqlArrayInputsRetainOriginalFieldsAndProduceQueryableVectors() throws Exception {
        CacheTable source = createTable(new Object[][]{
                {1, Arrays.asList("B", "A")},
                {2, Arrays.asList("C")}
        }, "ARRAY<VARCHAR>");
        List<Object[]> sqlInputs = queryTable(source, "select id, tag from vectors order by id");
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(sqlInputs), source.getDataFields());
        CacheTable output = new TagToVecFunction().evaluate(input, "tag", "vec");

        List<Object[]> rows = queryTable(output,
                "select id, tag, tag[1], vec, cardinality(vec), "
                        + "cast(vec[1] as integer), cast(vec[3] as integer) from vectors order by id");
        assertEquals(2, rows.size());
        assertArrayEquals(new Object[]{1, Arrays.asList("B", "A"), "B",
                Arrays.asList(1.0d, 1.0d, 0.0d), 3, 1, 0}, rows.get(0));
        assertArrayEquals(new Object[]{2, Arrays.asList("C"), "C",
                Arrays.asList(0.0d, 0.0d, 1.0d), 3, 0, 1}, rows.get(1));
    }

    @Test
    void absentTagsProduceEmptyVectorsQueryableThroughSql() throws Exception {
        CacheTable input = createTable(new Object[][]{
                {1, null},
                {2, List.of()},
                {3, Arrays.asList(null, null)}
        }, "ARRAY<VARCHAR>");
        CacheTable output = new TagToVecFunction().evaluate(input, "tag", "vec");

        List<Object[]> rows = queryTable(output,
                "select id, vec, cardinality(vec), vec is null from vectors order by id");
        assertEquals(3, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertArrayEquals(new Object[]{i + 1, List.of(), 0, false}, rows.get(i));
        }
    }

    @Test
    void repeatedTagsRemainMultiHotAndKeepFirstSeenDimensionOrder() throws Exception {
        CacheTable input = createTable(new Object[][]{
                {1, Arrays.asList("B", "B", "A", "B")},
                {2, Arrays.asList("C", "A", "C", null)},
                {3, Arrays.asList("C")}
        }, "ARRAY<VARCHAR>");
        CacheTable output = new TagToVecFunction().evaluate(input, "tag", "vec");

        List<Object[]> rows = queryTable(output,
                "select id, vec, cardinality(vec) from vectors order by id");
        assertEquals(3, rows.size());
        assertArrayEquals(new Object[]{1, Arrays.asList(1.0d, 1.0d, 0.0d), 3}, rows.get(0));
        assertArrayEquals(new Object[]{2, Arrays.asList(0.0d, 1.0d, 1.0d), 3}, rows.get(1));
        assertArrayEquals(new Object[]{3, Arrays.asList(0.0d, 0.0d, 1.0d), 3}, rows.get(2));
    }

    private List<Object[]> queryTable(CacheTable table, String sql) throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("default", new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Map.of("vectors", table);
            }
        });
        return NormalSqlCompiler.getNormalSqlBindable(sql, schema, "default")
                .bind(schema, new ExecuteContextImpl()).toList();
    }

}
