package com.sqlrec.common.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataTypeUtilsTest {
    private final RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    @Test
    void libraryParserKeepsCalciteDefaultsAndNumericRepresentations() {
        for (String name : List.of("DECIMAL", "VARCHAR", "TIMESTAMP", "REAL", "FLOAT", "ANY", "NULL", "UNKNOWN",
                "TIMESTAMP_WITH_LOCAL_TIME_ZONE")) {
            RelDataType expected = factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.get(name)), true);
            assertEquals(expected, DataTypeUtils.getRelDataType(factory, name), name);
        }
        RelDataType varchar = DataTypeUtils.getRelDataType("VARCHAR");
        assertEquals(varchar, DataTypeUtils.getRelDataType("STRING"));
        assertEquals(varchar, DataTypeUtils.getRelDataType("TEXT"));
        assertEquals(factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.UNKNOWN), false),
                DataTypeUtils.getRelDataType("UNKNOWN NOT NULL"));
    }

    @Test
    void preservesAliasesInsideCollections() {
        RelDataType map = DataTypeUtils.getRelDataType("MAP<TEXT, ARRAY<STRING NOT NULL>>");
        assertEquals(DataTypeUtils.getRelDataType("VARCHAR"), map.getKeyType());
        assertEquals(DataTypeUtils.getRelDataType("VARCHAR NOT NULL"), map.getValueType().getComponentType());
    }

    @Test
    void libraryParserRejectsBrokenNestingAndTrailingContent() {
        for (String invalid : List.of("MAP<INTEGER>", "ARRAY<INTEGER", "ARRAY<INTEGER>>", "INTEGER trailing")) {
            assertThrows(IllegalArgumentException.class, () -> DataTypeUtils.getRelDataType(invalid), invalid);
        }
    }

    @Test
    void mapRequiresKeyAndValueTypes() {
        for (String type : List.of("MAP", "ARRAY<MAP>")) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> DataTypeUtils.getRelDataType(type));
            assertEquals("MAP requires key and value types", error.getMessage());
        }
    }

    @Test
    void parsesNestedCollectionTypesWithoutLosingBounds() {
        RelDataType type = DataTypeUtils.getRelDataType(
                "MAP<VARCHAR(12) NOT NULL, ARRAY<DECIMAL(8, 2)>> NOT NULL");
        assertEquals(SqlTypeName.MAP, type.getSqlTypeName());
        assertFalse(type.isNullable());
        assertFalse(type.getKeyType().isNullable());
        assertEquals(12, type.getKeyType().getPrecision());
        RelDataType amount = type.getValueType().getComponentType();
        assertEquals(8, amount.getPrecision());
        assertEquals(2, amount.getScale());
    }

    @Test
    void rejectsRowColumnTypesAtAnyDepth() {
        for (String type : List.of("ROW<id INT>", "ARRAY<ROW<id INT>>", "MAP<ROW<id INT>, INT>",
                "MAP<STRING, ARRAY<ROW<id INT>>>")) {
            var error = assertThrows(IllegalArgumentException.class, () -> DataTypeUtils.getRelDataType(type));
            assertEquals("ROW column types are not supported", error.getMessage());
        }
    }

    @Test
    void preservesNullabilityForScalarAndNestedTypes() {
        for (String type : List.of("INTEGER", "VARCHAR(20)", "DECIMAL(12,2)", "TIMESTAMP(3)",
                "ARRAY<INTEGER>")) {
            assertTrue(DataTypeUtils.getRelDataType(type).isNullable(), type);
            assertFalse(DataTypeUtils.getRelDataType(type + " NOT NULL").isNullable(), type);
        }
        RelDataType array = DataTypeUtils.getRelDataType("ARRAY<ARRAY<INTEGER NOT NULL> NOT NULL>");
        assertTrue(array.isNullable());
        assertFalse(array.getComponentType().isNullable());
        assertFalse(array.getComponentType().getComponentType().isNullable());
    }

    @Test
    public void testGetRelDataTypeDecimalWithPrecisionAndScale() {
        RelDataType type = DataTypeUtils.getRelDataType("decimal(18, 6)");

        assertEquals(SqlTypeName.DECIMAL, type.getSqlTypeName());
        assertEquals(18, type.getPrecision());
        assertEquals(6, type.getScale());
    }

    @Test
    public void testGetRelDataTypeDecimalWithDefaultScale() {
        RelDataType type = DataTypeUtils.getRelDataType("DECIMAL(12)");

        assertEquals(SqlTypeName.DECIMAL, type.getSqlTypeName());
        assertEquals(12, type.getPrecision());
        assertEquals(0, type.getScale());
    }

    @Test
    public void testGetRelDataTypeParameterizedCharacterTypes() {
        RelDataType charType = DataTypeUtils.getRelDataType("CHAR(20)");
        RelDataType varcharType = DataTypeUtils.getRelDataType(" varchar ( 100 ) not null ");

        assertEquals(SqlTypeName.CHAR, charType.getSqlTypeName());
        assertEquals(20, charType.getPrecision());
        assertEquals(SqlTypeName.VARCHAR, varcharType.getSqlTypeName());
        assertEquals(100, varcharType.getPrecision());
    }

    @Test
    void parsesParameterizedBinaryAndTemporalTypes() {
        assertType("BINARY(1)", SqlTypeName.BINARY, 1);
        assertType(" varbinary ( 16 ) not null ", SqlTypeName.VARBINARY, 16);
        assertType("TIME(0)", SqlTypeName.TIME, 0);
        assertType(" time ( 3 ) not null ", SqlTypeName.TIME, 3);
        assertType("TIMESTAMP(3)", SqlTypeName.TIMESTAMP, 3);
        // Calcite 1.32 represents timestamps with at most millisecond precision.
        assertType("TIMESTAMP(6) NOT NULL", SqlTypeName.TIMESTAMP, 3);
        assertType("timestamp ( 3 ) with local time zone not null",
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);
    }

    @Test
    void parsesParameterizedTypesInsideArraysAndRejectsMalformedTypes() {
        RelDataType element = DataTypeUtils.getRelDataType("ARRAY<TIMESTAMP(6) NOT NULL>").getComponentType();
        assertNotNull(element);
        assertEquals(SqlTypeName.TIMESTAMP, element.getSqlTypeName());
        for (String invalid : List.of("BINARY(foo)", "TIME(-1)", "TIMESTAMP(3, 2)",
                "BOOLEAN(1)", "BINARY(1) WITH LOCAL TIME ZONE")) {
            assertThrows(RuntimeException.class, () -> DataTypeUtils.getRelDataType(invalid), invalid);
        }
    }

    @Test
    public void testGetRelDataTypeNestedParameterizedArray() {
        RelDataType type = DataTypeUtils.getRelDataType("ARRAY<ARRAY<DECIMAL(10, 2)>>");

        assertEquals(SqlTypeName.ARRAY, type.getSqlTypeName());
        RelDataType nestedArrayType = type.getComponentType();
        assertNotNull(nestedArrayType);
        assertEquals(SqlTypeName.ARRAY, nestedArrayType.getSqlTypeName());
        RelDataType decimalType = nestedArrayType.getComponentType();
        assertNotNull(decimalType);
        assertEquals(SqlTypeName.DECIMAL, decimalType.getSqlTypeName());
        assertEquals(10, decimalType.getPrecision());
        assertEquals(2, decimalType.getScale());
    }

    @Test
    public void testGetRelDataTypeAliasesAndInvalidType() {
        assertEquals(SqlTypeName.INTEGER, DataTypeUtils.getRelDataType(" int ").getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, DataTypeUtils.getRelDataType("string not null").getSqlTypeName());
        assertThrows(RuntimeException.class, () -> DataTypeUtils.getRelDataType("DECIMAL(foo, 2)"));
    }

    @Test
    void rejectsDifferentArrayAndMultisetElementTypesAtAnyDepth() {
        RelDataType integer = factory.createSqlType(SqlTypeName.INTEGER);
        RelDataType bigint = factory.createSqlType(SqlTypeName.BIGINT);
        assertIncompatible(integer, bigint);
        assertIncompatible(factory.createArrayType(integer, -1), factory.createArrayType(bigint, -1));
        assertIncompatible(factory.createArrayType(integer, -1),
                factory.createArrayType(factory.createSqlType(SqlTypeName.ANY), -1));
        assertIncompatible(factory.createMultisetType(integer, -1), factory.createMultisetType(bigint, -1));
        assertIncompatible(DataTypeUtils.getRelDataType("ARRAY<ARRAY<INTEGER>>"),
                DataTypeUtils.getRelDataType("ARRAY<ARRAY<BIGINT>>"));
    }

    @Test
    void rejectsDifferentMapKeyAndValueTypes() {
        RelDataType integer = factory.createSqlType(SqlTypeName.INTEGER);
        RelDataType bigint = factory.createSqlType(SqlTypeName.BIGINT);
        assertIncompatible(factory.createMapType(integer, integer), factory.createMapType(bigint, integer));
        assertIncompatible(factory.createMapType(integer, integer), factory.createMapType(integer, bigint));
        assertIncompatible(factory.createMapType(integer, factory.createArrayType(integer, -1)),
                factory.createMapType(integer, factory.createArrayType(bigint, -1)));
    }

    @Test
    void acceptsIdenticalNestedTypes() {
        RelDataType type = factory.createArrayType(factory.createMapType(
                factory.createSqlType(SqlTypeName.INTEGER),
                factory.createSqlType(SqlTypeName.BIGINT)), -1);
        DataTypeUtils.checkTableSchemaSame(fields(type), fields(type));
        DataTypeUtils.checkTableSchemaCompatible(fields(type), fields(type));
        assertEquals(type, DataTypeUtils.mergeTableSchemas(fields(type), fields(type)).get(0).getType());
    }

    @Test
    void preservesNestedNullabilityAndStringBoundsMerging() {
        RelDataType first = factory.createArrayType(factory.createSqlType(SqlTypeName.CHAR, 1), -1);
        RelDataType second = factory.createTypeWithNullability(factory.createArrayType(
                factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.VARCHAR, 20), true), -1), true);

        DataTypeUtils.checkTableSchemaCompatible(fields(first), fields(second));
        RelDataType merged = DataTypeUtils.mergeTableSchemas(fields(first), fields(second)).get(0).getType();
        assertTrue(merged.isNullable());
        assertTrue(merged.getComponentType().isNullable());
        assertEquals(SqlTypeName.VARCHAR, merged.getComponentType().getSqlTypeName());
        assertEquals(20, merged.getComponentType().getPrecision());
    }

    private void assertIncompatible(RelDataType first, RelDataType second) {
        for (List<RelDataType> pair : List.of(List.of(first, second), List.of(second, first))) {
            List<RelDataTypeField> left = fields(pair.get(0));
            List<RelDataTypeField> right = fields(pair.get(1));
            RuntimeException error = assertThrows(RuntimeException.class,
                    () -> DataTypeUtils.checkTableSchemaSame(left, right));
            assertTrue(error.getMessage().contains(first.toString()), error.getMessage());
            assertTrue(error.getMessage().contains(second.toString()), error.getMessage());
            assertThrows(RuntimeException.class, () -> DataTypeUtils.checkTableSchemaCompatible(left, right));
            assertThrows(RuntimeException.class, () -> DataTypeUtils.mergeTableSchemas(left, right));
        }
    }

    private List<RelDataTypeField> fields(RelDataType type) {
        return List.of(new RelDataTypeFieldImpl("value", 0, type));
    }

    private void assertType(String sql, SqlTypeName expectedType, int precision) {
        RelDataType type = DataTypeUtils.getRelDataType(sql);
        assertEquals(expectedType, type.getSqlTypeName(), sql);
        assertEquals(precision, type.getPrecision(), sql);
    }
}
