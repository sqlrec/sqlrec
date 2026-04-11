package com.sqlrec.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class KvJoinUtilsTest {

    @Test
    public void testCopyValuesWithBothValues() {
        Object[] leftValue = new Object[]{1, "Alice"};
        Object[] rightValue = new Object[]{100, "Engineer"};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 2, 2);

        assertNotNull(result);
        assertEquals(4, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
        assertEquals(100, result[2]);
        assertEquals("Engineer", result[3]);
    }

    @Test
    public void testCopyValuesWithNullRightValue() {
        Object[] leftValue = new Object[]{1, "Alice"};

        Object[] result = KvJoinUtils.copyValues(leftValue, null, 2, 2);

        assertNotNull(result);
        assertEquals(4, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
        assertNull(result[2]);
        assertNull(result[3]);
    }

    @Test
    public void testCopyValuesWithEmptyLeftValue() {
        Object[] leftValue = new Object[]{};
        Object[] rightValue = new Object[]{100, "Engineer"};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 0, 2);

        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals(100, result[0]);
        assertEquals("Engineer", result[1]);
    }

    @Test
    public void testCopyValuesWithEmptyRightValue() {
        Object[] leftValue = new Object[]{1, "Alice"};
        Object[] rightValue = new Object[]{};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 2, 0);

        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
    }

    @Test
    public void testCopyValuesWithDifferentSizes() {
        Object[] leftValue = new Object[]{1, "Alice", 30};
        Object[] rightValue = new Object[]{100};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 3, 1);

        assertNotNull(result);
        assertEquals(4, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
        assertEquals(30, result[2]);
        assertEquals(100, result[3]);
    }

    @Test
    public void testCopyValuesWithNullElements() {
        Object[] leftValue = new Object[]{1, null, "Alice"};
        Object[] rightValue = new Object[]{null, "Engineer"};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 3, 2);

        assertNotNull(result);
        assertEquals(5, result.length);
        assertEquals(1, result[0]);
        assertNull(result[1]);
        assertEquals("Alice", result[2]);
        assertNull(result[3]);
        assertEquals("Engineer", result[4]);
    }

    @Test
    public void testCopyValuesPreservesOriginalArrays() {
        Object[] leftValue = new Object[]{1, "Alice"};
        Object[] rightValue = new Object[]{100, "Engineer"};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 2, 2);

        result[0] = 999;
        result[3] = "Modified";

        assertEquals(1, leftValue[0]);
        assertEquals(100, rightValue[0]);
    }
}
