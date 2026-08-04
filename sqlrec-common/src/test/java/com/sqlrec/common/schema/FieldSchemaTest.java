package com.sqlrec.common.schema;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FieldSchemaTest {

    @Test
    public void testConstructorAndGetters() {
        FieldSchema field = new FieldSchema("user_id", "BIGINT");

        assertEquals("user_id", field.getName());
        assertEquals("BIGINT", field.getType());
    }

    @Test
    public void testSetName() {
        FieldSchema field = new FieldSchema("old", "VARCHAR");
        field.setName("new");

        assertEquals("new", field.getName());
    }

    @Test
    public void testSetType() {
        FieldSchema field = new FieldSchema("col", "INTEGER");
        field.setType("BIGINT");

        assertEquals("BIGINT", field.getType());
    }

    @Test
    public void testNullValues() {
        FieldSchema field = new FieldSchema(null, null);

        assertNull(field.getName());
        assertNull(field.getType());
    }
}
