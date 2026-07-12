package com.sqlrec.udf.scalar;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class UuidFunctionTest {

    private final UuidFunction function = new UuidFunction();

    @Test
    public void testUuidNotNull() {
        String result = function.evaluate();
        assertNotNull(result);
    }

    @Test
    public void testUuidFormat() {
        String result = function.evaluate();
        assertDoesNotThrow(() -> UUID.fromString(result));
    }

    @Test
    public void testUuidLength() {
        String result = function.evaluate();
        assertEquals(36, result.length());
    }

    @Test
    public void testUuidUniqueness() {
        String result1 = function.evaluate();
        String result2 = function.evaluate();
        assertNotEquals(result1, result2);
    }

    @Test
    public void testUuidMultipleCalls() {
        for (int i = 0; i < 100; i++) {
            String result = function.evaluate();
            assertNotNull(result);
            assertDoesNotThrow(() -> UUID.fromString(result));
        }
    }
}
