package com.sqlrec.udf.scalar;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class ArrayContainsFunctionTest {

    @Test
    public void testContainsElement() {
        assertTrue(ArrayContainsFunction.evaluate(Arrays.asList(1, 2, 3), 2));
    }

    @Test
    public void testNotContainsElement() {
        assertFalse(ArrayContainsFunction.evaluate(Arrays.asList(1, 2, 3), 4));
    }

    @Test
    public void testContainsString() {
        assertTrue(ArrayContainsFunction.evaluate(Arrays.asList("a", "b", "c"), "b"));
    }

    @Test
    public void testNotContainsString() {
        assertFalse(ArrayContainsFunction.evaluate(Arrays.asList("a", "b", "c"), "d"));
    }

    @Test
    public void testNullList() {
        assertNull(ArrayContainsFunction.evaluate(null, 1));
    }

    @Test
    public void testNullElement() {
        assertNull(ArrayContainsFunction.evaluate(Arrays.asList(1, 2, 3), null));
    }

    @Test
    public void testEmptyList() {
        assertFalse(ArrayContainsFunction.evaluate(Collections.emptyList(), 1));
    }

    @Test
    public void testContainsNullInList() {
        assertNull(ArrayContainsFunction.evaluate(Arrays.asList(1, null, 3), null));
    }
}
