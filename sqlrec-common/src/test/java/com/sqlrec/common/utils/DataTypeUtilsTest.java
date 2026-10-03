package com.sqlrec.common.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataTypeUtilsTest {
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

}
