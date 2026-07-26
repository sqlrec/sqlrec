package com.sqlrec.common.utils;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DataTypeUtilsTest {

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

        List<Object[]> result = DataTypeUtils.adaptRowsToSchema(rows, desired, given);

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

        List<Object[]> result = DataTypeUtils.adaptRowsToSchema(rows, desired, given);

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

        List<Object[]> result = DataTypeUtils.adaptRowsToSchema(rows, desired, given);

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

        List<Object[]> result = DataTypeUtils.adaptRowsToSchema(rows, desired, given);

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

        List<Object[]> result = DataTypeUtils.adaptRowsToSchema(rows, desiredDouble, givenInt);
        assertEquals(Double.valueOf(30.0), result.get(0)[0]);

        // BIGINT -> INTEGER
        List<RelDataTypeField> desiredInt = fields(field("v", 0, Sql.INTEGER));
        List<RelDataTypeField> givenBig = fields(field("v", 0, Sql.BIGINT));
        List<Object[]> rows2 = Collections.singletonList(new Object[]{42L});

        List<Object[]> result2 = DataTypeUtils.adaptRowsToSchema(rows2, desiredInt, givenBig);
        assertEquals(Integer.valueOf(42), result2.get(0)[0]);

        // DOUBLE -> BIGINT (truncating)
        List<RelDataTypeField> desiredBig = fields(field("v", 0, Sql.BIGINT));
        List<RelDataTypeField> givenDouble = fields(field("v", 0, Sql.DOUBLE));
        List<Object[]> rows3 = Collections.singletonList(new Object[]{30.7});

        List<Object[]> result3 = DataTypeUtils.adaptRowsToSchema(rows3, desiredBig, givenDouble);
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

        List<Object[]> result = DataTypeUtils.adaptRowsToSchema(rows, desired, given);

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

        List<Object[]> result = DataTypeUtils.adaptRowsToSchema(rows, desired, given);

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

        List<Object[]> result = DataTypeUtils.adaptRowsToSchema(rows, desired, given);

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
                () -> DataTypeUtils.adaptRowsToSchema(rows, desired, given));
    }

    @Test
    public void testStringToNumericThrows() {
        // string -> numeric is not one of the allowed conversions
        List<RelDataTypeField> desired = fields(field("age", 0, Sql.INTEGER));
        List<RelDataTypeField> given = fields(field("age", 0, Sql.VARCHAR));
        List<Object[]> rows = Collections.singletonList(new Object[]{"30"});

        assertThrows(RuntimeException.class,
                () -> DataTypeUtils.adaptRowsToSchema(rows, desired, given));
    }

    @Test
    public void testNumericToBooleanThrows() {
        // numeric -> boolean is not allowed; boolean requires exact match
        List<RelDataTypeField> desired = fields(field("flag", 0, Sql.BOOLEAN));
        List<RelDataTypeField> given = fields(field("flag", 0, Sql.INTEGER));
        List<Object[]> rows = Collections.singletonList(new Object[]{1});

        assertThrows(RuntimeException.class,
                () -> DataTypeUtils.adaptRowsToSchema(rows, desired, given));
    }

    @Test
    public void testNullArgumentsThrow() {
        assertThrows(RuntimeException.class,
                () -> DataTypeUtils.adaptRowsToSchema(null,
                        fields(field("a", 0, Sql.VARCHAR)),
                        fields(field("a", 0, Sql.VARCHAR))));
    }
}
