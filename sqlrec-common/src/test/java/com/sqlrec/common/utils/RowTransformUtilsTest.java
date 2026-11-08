package com.sqlrec.common.utils;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RowTransformUtilsTest {
    @Test
    void normalizesRowsInsideArraysAndMapsWithoutChangingNamedInputs() {
        List<RelDataTypeField> fields = List.of(
                DataTypeUtils.getRelDataTypeField("records", 0, "ARRAY<ROW<id INTEGER, scores ARRAY<FLOAT>>>"),
                DataTypeUtils.getRelDataTypeField("by_key", 1, "MAP<VARCHAR, ROW<id BIGINT>>"));
        Map<String, Object> record = Map.of("scores", List.of(0.25f), "id", 3L);
        Map<String, Object> input = Map.of("records", Arrays.asList(record, null),
                "by_key", Map.of("first", Map.of("id", 7)));

        Object[] row = RowTransformUtils.convertDataToEnumerable(List.of(input), fields).single();
        assertArrayEquals(new Object[]{3, List.of(0.25d)}, (Object[]) ((List<?>) row[0]).get(0));
        assertNull(((List<?>) row[0]).get(1));
        assertArrayEquals(new Object[]{7L}, (Object[]) ((Map<?, ?>) row[1]).get("first"));
        RowTransformUtils.convertRowTypes(Collections.singletonList(row), fields);
        assertArrayEquals(new Object[]{3, List.of(0.25d)}, (Object[]) ((List<?>) row[0]).get(0));
        assertEquals(Map.of("scores", List.of(0.25f), "id", 3L), record);
    }

    @Test
    void normalizesJsonArraysAtNamedRowInputBoundary() {
        List<Long> ids = Arrays.asList(2L, null);
        Map<String, Object> input = Map.of("ids", ids, "count", 3L);
        List<RelDataTypeField> fields = List.of(
                DataTypeUtils.getRelDataTypeField("count", 0, "INTEGER"),
                DataTypeUtils.getRelDataTypeField("ids", 1, "ARRAY<INTEGER>"),
                DataTypeUtils.getRelDataTypeField("missing", 2, "ARRAY<FLOAT>"));

        Object[] row = RowTransformUtils.convertDataToEnumerable(List.of(input), fields).single();

        assertArrayEquals(new Object[]{3, Arrays.asList(2, null), null}, row);
        assertEquals(Long.class, ids.get(0).getClass());
    }

    @Test
    void convertsArrayElementsRecursivelyWithoutMutatingInputs() {
        List<RelDataTypeField> given = List.of(
                DataTypeUtils.getRelDataTypeField("nested", 0, "ARRAY<ARRAY<REAL>>"),
                DataTypeUtils.getRelDataTypeField("ints", 1, "ARRAY<BIGINT>"));
        List<RelDataTypeField> desired = List.of(
                DataTypeUtils.getRelDataTypeField("ints", 0, "ARRAY<INTEGER>"),
                DataTypeUtils.getRelDataTypeField("nested", 1, "ARRAY<ARRAY<FLOAT>>"));
        List<Float> inner = Arrays.asList(0.25f, null);
        List<List<Float>> nested = Arrays.asList(inner, null, Collections.emptyList());
        List<Long> ints = Arrays.asList(2L, null);
        Object[] source = {nested, ints};

        Object[] row = RowTransformUtils.adaptRowsToSchema(
                Collections.singletonList(source), desired, given).get(0);

        assertEquals(Arrays.asList(2, null), row[0]);
        assertEquals(Arrays.asList(Arrays.asList(0.25d, null), null, Collections.emptyList()), row[1]);
        assertEquals(Float.class, inner.get(0).getClass());
        assertEquals(Long.class, ints.get(0).getClass());
        assertSame(nested, source[0]);
    }

    @Test
    void tableRowConversionNormalizesListsAndJavaArrays() {
        List<RelDataTypeField> fields = List.of(
                DataTypeUtils.getRelDataTypeField("scores", 0, "ARRAY<FLOAT>"),
                DataTypeUtils.getRelDataTypeField("ids", 1, "ARRAY<BIGINT>"));
        float[] scores = {0.25f, 0.5f};
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{scores, new Integer[]{2, null}});
        rows.add(new Object[]{Collections.emptyList(), null});
        rows.add(null);

        RowTransformUtils.convertRowTypes(rows, fields);

        assertEquals(List.of(0.25d, 0.5d), rows.get(0)[0]);
        assertEquals(Arrays.asList(2L, null), rows.get(0)[1]);
        assertEquals(Collections.emptyList(), rows.get(1)[0]);
        assertNull(rows.get(1)[1]);
        assertNull(rows.get(2));
        assertArrayEquals(new float[]{0.25f, 0.5f}, scores);
    }

    @Test
    void rejectsIncompatibleArrayElementSchemasEvenWhenRowsAreEmpty() {
        List<RelDataTypeField> desired = List.of(
                DataTypeUtils.getRelDataTypeField("xs", 0, "ARRAY<ARRAY<INTEGER>>"));
        List<RelDataTypeField> given = List.of(
                DataTypeUtils.getRelDataTypeField("xs", 0, "ARRAY<ARRAY<VARCHAR>>"));
        assertThrows(RuntimeException.class,
                () -> RowTransformUtils.adaptRowsToSchema(Collections.emptyList(), desired, given));
    }

    @Test
    void materializationPreservesOrderAndArrayReferencesAndReadsSourceOnce() {
        Object[] row = {7, "value"};
        int[] arrayCell = {1, 2};
        AtomicInteger reads = new AtomicInteger();
        Enumerable<Object> source = Linq4j.asEnumerable(Arrays.asList("scalar", row, null, arrayCell))
                .select(value -> {
                    reads.incrementAndGet();
                    return value;
                });

        List<Object[]> rows = RowTransformUtils.materializeArrayRows(source);

        assertEquals(4, reads.get());
        assertEquals(4, rows.size());
        assertArrayEquals(new Object[]{"scalar"}, rows.get(0));
        assertSame(row, rows.get(1));
        assertArrayEquals(new Object[]{null}, rows.get(2));
        assertArrayEquals(new Object[]{arrayCell}, rows.get(3));
        assertSame(arrayCell, rows.get(3)[0]);
        assertTrue(RowTransformUtils.materializeArrayRows(Linq4j.emptyEnumerable()).isEmpty());
    }

    // --- getMsgEnumerable ---

    @Test
    public void testGetMsgEnumerable() {
        Object[] result = RowTransformUtils.getMsgEnumerable("hello").toList().toArray(new Object[0]);

        assertEquals(1, result.length);
        assertArrayEquals(new String[]{"hello"}, (String[]) result[0]);
    }

    @Test
    public void testGetMsgEnumerableNull() {
        assertNull(RowTransformUtils.getMsgEnumerable(null));
    }

    // --- convertListToEnumerable ---

    @Test
    public void testConvertListToEnumerable() {
        List<String> input = Arrays.asList("a", "b", "c");
        List<Object[]> result = RowTransformUtils.convertListToEnumerable(input).toList();

        assertEquals(3, result.size());
        assertEquals("a", result.get(0)[0]);
        assertEquals("b", result.get(1)[0]);
        assertEquals("c", result.get(2)[0]);
    }

    @Test
    public void testConvertListToEnumerableNull() {
        assertNull(RowTransformUtils.convertListToEnumerable(null));
    }

    // --- convertListToArrayToEnumerable ---

    @Test
    public void testConvertListToArrayToEnumerable() {
        List<List<Integer>> input = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4)
        );
        List<Object[]> result = RowTransformUtils.convertListToArrayToEnumerable(input).toList();

        assertEquals(2, result.size());
        assertArrayEquals(new Object[]{1, 2}, result.get(0));
        assertArrayEquals(new Object[]{3, 4}, result.get(1));
    }

    @Test
    public void testConvertListToArrayToEnumerableNull() {
        assertNull(RowTransformUtils.convertListToArrayToEnumerable(null));
    }


    private static RelDataTypeField field(String name, int index, Sql typeName) {
        return DataTypeUtils.getRelDataTypeField(name, index, typeName.toSqlTypeName());
    }

    private static List<RelDataTypeField> fields(RelDataTypeField... fields) {
        return new ArrayList<>(Arrays.asList(fields));
    }

    private enum Sql {
        VARCHAR(SqlTypeName.VARCHAR),
        CHAR(SqlTypeName.CHAR),
        INTEGER(SqlTypeName.INTEGER),
        BIGINT(SqlTypeName.BIGINT),
        SMALLINT(SqlTypeName.SMALLINT),
        TINYINT(SqlTypeName.TINYINT),
        FLOAT(SqlTypeName.FLOAT),
        DOUBLE(SqlTypeName.DOUBLE),
        DECIMAL(SqlTypeName.DECIMAL),
        BOOLEAN(SqlTypeName.BOOLEAN),
        DATE(SqlTypeName.DATE);

        private final SqlTypeName sqlTypeName;

        Sql(SqlTypeName sqlTypeName) {
            this.sqlTypeName = sqlTypeName;
        }

        SqlTypeName toSqlTypeName() {
            return sqlTypeName;
        }
    }

    @Test
    public void testSameOrderAndTypes() {
        List<RelDataTypeField> desired = fields(
                field("name", 0, Sql.VARCHAR),
                field("age", 1, Sql.INTEGER)
        );
        List<RelDataTypeField> given = fields(
                field("name", 0, Sql.VARCHAR),
                field("age", 1, Sql.INTEGER)
        );
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{"Alice", 30});
        rows.add(new Object[]{"Bob", 25});

        List<Object[]> result = RowTransformUtils.adaptRowsToSchema(rows, desired, given);

        assertEquals(2, result.size());
        assertEquals("Alice", result.get(0)[0]);
        assertEquals(Integer.valueOf(30), result.get(0)[1]);
        assertEquals("Bob", result.get(1)[0]);
        assertEquals(Integer.valueOf(25), result.get(1)[1]);
    }

    @Test
    public void testReorderFields() {
        // given fields are in swapped order; result must follow desired order
        List<RelDataTypeField> desired = fields(
                field("name", 0, Sql.VARCHAR),
                field("age", 1, Sql.INTEGER)
        );
        List<RelDataTypeField> given = fields(
                field("age", 0, Sql.INTEGER),
                field("name", 1, Sql.VARCHAR)
        );
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{30, "Alice"});
        rows.add(new Object[]{25, "Bob"});

        List<Object[]> result = RowTransformUtils.adaptRowsToSchema(rows, desired, given);

        assertEquals(2, result.size());
        assertEquals("Alice", result.get(0)[0]);
        assertEquals(Integer.valueOf(30), result.get(0)[1]);
        assertEquals("Bob", result.get(1)[0]);
        assertEquals(Integer.valueOf(25), result.get(1)[1]);
    }

    @Test
    public void testCaseInsensitiveNameMatching() {
        List<RelDataTypeField> desired = fields(
                field("Name", 0, Sql.VARCHAR),
                field("AGE", 1, Sql.INTEGER)
        );
        List<RelDataTypeField> given = fields(
                field("name", 0, Sql.VARCHAR),
                field("age", 1, Sql.INTEGER)
        );
        List<Object[]> rows = Collections.singletonList(new Object[]{"Alice", 30});

        List<Object[]> result = RowTransformUtils.adaptRowsToSchema(rows, desired, given);

        assertEquals(1, result.size());
        assertEquals("Alice", result.get(0)[0]);
        assertEquals(Integer.valueOf(30), result.get(0)[1]);
    }

    @Test
    public void testAnyTypeToString() {
        // desired VARCHAR accepts numeric and boolean sources
        List<RelDataTypeField> desired = fields(
                field("age", 0, Sql.VARCHAR),
                field("flag", 1, Sql.VARCHAR)
        );
        List<RelDataTypeField> given = fields(
                field("age", 0, Sql.INTEGER),
                field("flag", 1, Sql.BOOLEAN)
        );
        List<Object[]> rows = Collections.singletonList(new Object[]{30, true});

        List<Object[]> result = RowTransformUtils.adaptRowsToSchema(rows, desired, given);

        assertEquals(1, result.size());
        assertEquals("30", result.get(0)[0]);
        assertEquals("true", result.get(0)[1]);
    }

    @Test
    public void testNumericToNumericConversion() {
        // INTEGER -> DOUBLE
        List<RelDataTypeField> desiredDouble = fields(field("v", 0, Sql.DOUBLE));
        List<RelDataTypeField> givenInt = fields(field("v", 0, Sql.INTEGER));
        List<Object[]> rows = Collections.singletonList(new Object[]{30});

        List<Object[]> result = RowTransformUtils.adaptRowsToSchema(rows, desiredDouble, givenInt);
        assertEquals(Double.valueOf(30.0), result.get(0)[0]);

        // BIGINT -> INTEGER
        List<RelDataTypeField> desiredInt = fields(field("v", 0, Sql.INTEGER));
        List<RelDataTypeField> givenBig = fields(field("v", 0, Sql.BIGINT));
        List<Object[]> rows2 = Collections.singletonList(new Object[]{42L});

        List<Object[]> result2 = RowTransformUtils.adaptRowsToSchema(rows2, desiredInt, givenBig);
        assertEquals(Integer.valueOf(42), result2.get(0)[0]);

        // DOUBLE -> BIGINT (truncating)
        List<RelDataTypeField> desiredBig = fields(field("v", 0, Sql.BIGINT));
        List<RelDataTypeField> givenDouble = fields(field("v", 0, Sql.DOUBLE));
        List<Object[]> rows3 = Collections.singletonList(new Object[]{30.7});

        List<Object[]> result3 = RowTransformUtils.adaptRowsToSchema(rows3, desiredBig, givenDouble);
        assertEquals(Long.valueOf(30L), result3.get(0)[0]);
    }

    @Test
    public void testExtraGivenFieldsIgnored() {
        List<RelDataTypeField> desired = fields(field("name", 0, Sql.VARCHAR));
        List<RelDataTypeField> given = fields(
                field("name", 0, Sql.VARCHAR),
                field("age", 1, Sql.INTEGER)
        );
        List<Object[]> rows = Collections.singletonList(new Object[]{"Alice", 30});

        List<Object[]> result = RowTransformUtils.adaptRowsToSchema(rows, desired, given);

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).length);
        assertEquals("Alice", result.get(0)[0]);
    }

    @Test
    public void testNullValuesPreserved() {
        List<RelDataTypeField> desired = fields(
                field("name", 0, Sql.VARCHAR),
                field("age", 1, Sql.INTEGER)
        );
        List<RelDataTypeField> given = fields(
                field("name", 0, Sql.VARCHAR),
                field("age", 1, Sql.INTEGER)
        );
        List<Object[]> rows = Collections.singletonList(new Object[]{null, null});

        List<Object[]> result = RowTransformUtils.adaptRowsToSchema(rows, desired, given);

        assertEquals(1, result.size());
        assertNull(result.get(0)[0]);
        assertNull(result.get(0)[1]);
    }

    @Test
    public void testNullRowPreserved() {
        List<RelDataTypeField> desired = fields(field("name", 0, Sql.VARCHAR));
        List<RelDataTypeField> given = fields(field("name", 0, Sql.VARCHAR));
        List<Object[]> rows = new ArrayList<>();
        rows.add(null);

        List<Object[]> result = RowTransformUtils.adaptRowsToSchema(rows, desired, given);

        assertEquals(1, result.size());
        assertNull(result.get(0));
    }

    @Test
    public void testMissingFieldThrows() {
        List<RelDataTypeField> desired = fields(
                field("name", 0, Sql.VARCHAR),
                field("age", 1, Sql.INTEGER)
        );
        List<RelDataTypeField> given = fields(field("name", 0, Sql.VARCHAR));
        List<Object[]> rows = Collections.singletonList(new Object[]{"Alice"});

        assertThrows(RuntimeException.class,
                () -> RowTransformUtils.adaptRowsToSchema(rows, desired, given));
    }

    @Test
    public void testStringToNumericThrows() {
        // string -> numeric is not one of the allowed conversions
        List<RelDataTypeField> desired = fields(field("age", 0, Sql.INTEGER));
        List<RelDataTypeField> given = fields(field("age", 0, Sql.VARCHAR));
        List<Object[]> rows = Collections.singletonList(new Object[]{"30"});

        assertThrows(RuntimeException.class,
                () -> RowTransformUtils.adaptRowsToSchema(rows, desired, given));
    }

    @Test
    public void testNumericToBooleanThrows() {
        // numeric -> boolean is not allowed; boolean requires exact match
        List<RelDataTypeField> desired = fields(field("flag", 0, Sql.BOOLEAN));
        List<RelDataTypeField> given = fields(field("flag", 0, Sql.INTEGER));
        List<Object[]> rows = Collections.singletonList(new Object[]{1});

        assertThrows(RuntimeException.class,
                () -> RowTransformUtils.adaptRowsToSchema(rows, desired, given));
    }

    @Test
    public void testNullArgumentsThrow() {
        assertThrows(RuntimeException.class,
                () -> RowTransformUtils.adaptRowsToSchema(null,
                        fields(field("a", 0, Sql.VARCHAR)),
                        fields(field("a", 0, Sql.VARCHAR))));
    }
}
