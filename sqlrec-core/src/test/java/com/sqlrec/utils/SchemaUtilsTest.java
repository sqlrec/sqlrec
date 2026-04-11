package com.sqlrec.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaUtilsTest {

    @Test
    public void testRemoveQuotesWithSingleQuotes() {
        String input = "'test_value'";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("test_value", result);
    }

    @Test
    public void testRemoveQuotesWithDoubleQuotes() {
        String input = "\"test_value\"";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("test_value", result);
    }

    @Test
    public void testRemoveQuotesWithNoQuotes() {
        String input = "test_value";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("test_value", result);
    }

    @Test
    public void testRemoveQuotesWithNull() {
        String result = SchemaUtils.removeQuotes(null);
        assertNull(result);
    }

    @Test
    public void testRemoveQuotesWithOnlyStartQuote() {
        String input = "'test_value";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("'test_value", result);
    }

    @Test
    public void testRemoveQuotesWithOnlyEndQuote() {
        String input = "test_value'";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("test_value'", result);
    }

    @Test
    public void testRemoveQuotesWithEmptyString() {
        String input = "";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("", result);
    }

    @Test
    public void testRemoveQuotesWithQuotedEmptyString() {
        String input = "''";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("", result);
    }

    @Test
    public void testRemoveQuotesWithMismatchedQuotes() {
        String input = "'test_value\"";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("'test_value\"", result);
    }

    @Test
    public void testGetValueOfStringLiteralWithNull() {
        String result = SchemaUtils.getValueOfStringLiteral(null);
        assertNull(result);
    }

    @Test
    public void testConvertFieldListWithNull() {
        var result = SchemaUtils.convertFieldList(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testConvertPropertyListWithNull() {
        var result = SchemaUtils.convertPropertyList(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetTableObjWithNullSchema() {
        var result = SchemaUtils.getTableObj(null, "test_table");
        assertNull(result);
    }

    @Test
    public void testGetTableObjWithNullTableName() {
        var schema = org.apache.calcite.jdbc.CalciteSchema.createRootSchema(false);
        var result = SchemaUtils.getTableObj(schema, null);
        assertNull(result);
    }

    @Test
    public void testGetTableObjWithNonExistentTable() {
        var schema = org.apache.calcite.jdbc.CalciteSchema.createRootSchema(false);
        var result = SchemaUtils.getTableObj(schema, "non_existent_table");
        assertNull(result);
    }
}
