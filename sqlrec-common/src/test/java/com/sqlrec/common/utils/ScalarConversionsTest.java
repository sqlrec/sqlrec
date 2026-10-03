package com.sqlrec.common.utils;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

class ScalarConversionsTest {
    @Test
    void stringAndSqlTypeTargetsUseTheSameNumericConversions() {
        String[] types = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "REAL", "DOUBLE", "DECIMAL"};
        Class<?>[] classes = {Byte.class, Short.class, Integer.class, Long.class,
                Float.class, Float.class, Double.class, BigDecimal.class};
        for (int i = 0; i < types.length; i++) {
            Object parsed = ScalarConversions.convert("42", types[i]);
            assertEquals(classes[i], parsed.getClass(), types[i]);
            assertEquals(parsed, ScalarConversions.convert("42", SqlTypeName.get(types[i])));
            assertEquals(parsed, ScalarConversions.convert(42, SqlTypeName.get(types[i])));
        }
        assertEquals(new BigDecimal("30.5"), ScalarConversions.convert(30.5d, SqlTypeName.DECIMAL));
        assertEquals(new BigDecimal("42.75"), ScalarConversions.convert("42.75", SqlTypeName.DECIMAL));
        assertEquals(30L, ScalarConversions.convert(30.7, SqlTypeName.BIGINT));
    }

    @Test
    void preservesAliasesBooleanNullAndPassThroughTypes() {
        assertEquals(42, ScalarConversions.convert("42", "int"));
        assertEquals(new BigDecimal("1.25"), ScalarConversions.convert("1.25", "numeric"));
        assertEquals("42", ScalarConversions.convert(42, "TEXT"));
        assertEquals("true", ScalarConversions.convert(true, "STRING"));
        assertEquals(true, ScalarConversions.convert("TRUE", "BOOLEAN"));
        assertEquals(false, ScalarConversions.convert("not-a-boolean", "BOOLEAN"));
        assertNull(ScalarConversions.convert(null, "INTEGER"));
        Object value = new Object();
        assertSame(value, ScalarConversions.convert(value, "unknown"));
        assertSame(value, ScalarConversions.convert(value, "ARRAY<INTEGER>"));
        assertEquals("2026-10-04", ScalarConversions.convert("2026-10-04", "DATE"));
    }

    @Test
    void rejectsMalformedNumbersAndUnsupportedNumericSources() {
        assertThrows(IllegalArgumentException.class, () -> ScalarConversions.convert("128", "TINYINT"));
        assertThrows(IllegalArgumentException.class, () -> ScalarConversions.convert("1.5", "INTEGER"));
        assertThrows(IllegalArgumentException.class, () -> ScalarConversions.convert("bad", "DOUBLE"));
        assertThrows(IllegalArgumentException.class,
                () -> ScalarConversions.convert(new Object(), SqlTypeName.INTEGER));
    }

    @Test
    void typeNamesAreIndependentOfTheDefaultLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            assertEquals(42, ScalarConversions.convert("42", "integer"));
            assertEquals(SqlTypeName.INTEGER, DataTypeUtils.getRelDataType("integer").getSqlTypeName());
        } finally {
            Locale.setDefault(previous);
        }
    }
}
