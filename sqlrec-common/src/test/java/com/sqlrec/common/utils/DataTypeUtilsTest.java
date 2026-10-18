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
    void rejectsDifferentRowFieldTypesNamesAndCounts() {
        RelDataType row = factory.builder().add("id", SqlTypeName.INTEGER).build();
        assertIncompatible(row, factory.builder().add("id", SqlTypeName.BIGINT).build());
        assertIncompatible(row, factory.builder().add("other", SqlTypeName.INTEGER).build());
        assertIncompatible(row, factory.builder().add("id", SqlTypeName.INTEGER)
                .add("extra", SqlTypeName.INTEGER).build());
        assertIncompatible(factory.createArrayType(row, -1), factory.createArrayType(
                factory.builder().add("id", SqlTypeName.BIGINT).build(), -1));
    }

    @Test
    void acceptsIdenticalNestedTypes() {
        RelDataType type = factory.createArrayType(factory.createMapType(
                factory.createSqlType(SqlTypeName.INTEGER),
                factory.builder().add("id", SqlTypeName.INTEGER).build()), -1);
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
}
