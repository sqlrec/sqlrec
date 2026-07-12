package com.sqlrec.udf.scalar;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class ArrayContainsAnyFunctionTest {

    @Test
    public void testContainsAnyTrue() {
        assertTrue(ArrayContainsAnyFunction.evaluate(Arrays.asList(1, 2, 3), Arrays.asList(2, 4)));
    }

    @Test
    public void testContainsAnyFalse() {
        assertFalse(ArrayContainsAnyFunction.evaluate(Arrays.asList(1, 2, 3), Arrays.asList(4, 5)));
    }

    @Test
    public void testContainsAnyStrings() {
        assertTrue(ArrayContainsAnyFunction.evaluate(Arrays.asList("a", "b", "c"), Arrays.asList("b", "d")));
    }

    @Test
    public void testNullList() {
        assertNull(ArrayContainsAnyFunction.evaluate(null, Arrays.asList(1)));
    }

    @Test
    public void testNullElements() {
        assertNull(ArrayContainsAnyFunction.evaluate(Arrays.asList(1, 2), null));
    }

    @Test
    public void testEmptyList() {
        assertFalse(ArrayContainsAnyFunction.evaluate(Collections.emptyList(), Arrays.asList(1)));
    }

    @Test
    public void testEmptyElements() {
        assertFalse(ArrayContainsAnyFunction.evaluate(Arrays.asList(1, 2, 3), Collections.emptyList()));
    }

    @Test
    public void testBothEmpty() {
        assertFalse(ArrayContainsAnyFunction.evaluate(Collections.emptyList(), Collections.emptyList()));
    }

    @Test
    public void testSingleMatch() {
        assertTrue(ArrayContainsAnyFunction.evaluate(Arrays.asList(1, 2, 3), Arrays.asList(3)));
    }
}
