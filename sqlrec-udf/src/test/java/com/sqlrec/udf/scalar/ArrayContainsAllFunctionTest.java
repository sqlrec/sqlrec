package com.sqlrec.udf.scalar;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class ArrayContainsAllFunctionTest {

    @Test
    public void testContainsAllTrue() {
        assertTrue(ArrayContainsAllFunction.evaluate(Arrays.asList(1, 2, 3), Arrays.asList(1, 2)));
    }

    @Test
    public void testContainsAllExactMatch() {
        assertTrue(ArrayContainsAllFunction.evaluate(Arrays.asList(1, 2, 3), Arrays.asList(1, 2, 3)));
    }

    @Test
    public void testContainsAllFalse() {
        assertFalse(ArrayContainsAllFunction.evaluate(Arrays.asList(1, 2, 3), Arrays.asList(1, 4)));
    }

    @Test
    public void testContainsAllStrings() {
        assertTrue(ArrayContainsAllFunction.evaluate(Arrays.asList("a", "b", "c"), Arrays.asList("a", "c")));
    }

    @Test
    public void testNullList() {
        assertNull(ArrayContainsAllFunction.evaluate(null, Arrays.asList(1)));
    }

    @Test
    public void testNullElements() {
        assertNull(ArrayContainsAllFunction.evaluate(Arrays.asList(1, 2), null));
    }

    @Test
    public void testEmptyElements() {
        assertTrue(ArrayContainsAllFunction.evaluate(Arrays.asList(1, 2, 3), Collections.emptyList()));
    }

    @Test
    public void testBothEmpty() {
        assertTrue(ArrayContainsAllFunction.evaluate(Collections.emptyList(), Collections.emptyList()));
    }

    @Test
    public void testSubsetOfEmpty() {
        assertFalse(ArrayContainsAllFunction.evaluate(Collections.emptyList(), Arrays.asList(1)));
    }
}
