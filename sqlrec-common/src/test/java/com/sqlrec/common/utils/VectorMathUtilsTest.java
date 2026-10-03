package com.sqlrec.common.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VectorMathUtilsTest {
    // --- convertToFloatVec ---

    @Test
    public void testConvertToFloatVecFromIntList() {
        List<Float> result = VectorMathUtils.convertToFloatVec(Arrays.asList(1, 2, 3));

        assertEquals(Arrays.asList(1.0f, 2.0f, 3.0f), result);
    }

    @Test
    public void testConvertToFloatVecFromDoubleList() {
        List<Float> result = VectorMathUtils.convertToFloatVec(Arrays.asList(1.5, 2.5));

        assertEquals(Arrays.asList(1.5f, 2.5f), result);
    }

    @Test
    public void testConvertToFloatVecEmptyList() {
        List<Float> result = VectorMathUtils.convertToFloatVec(Collections.emptyList());

        assertTrue(result.isEmpty());
    }

    @Test
    public void testConvertToFloatVecNonListThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorMathUtils.convertToFloatVec("not a list"));
    }

    @Test
    public void testConvertToFloatVecNonNumberElementThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorMathUtils.convertToFloatVec(Arrays.asList(1, "two", 3)));
    }

    // --- toDoubleArray ---

    @Test
    public void testToDoubleArrayFromList() {
        double[] result = VectorMathUtils.toDoubleArray(Arrays.asList(1, 2, 3));

        assertArrayEquals(new double[]{1.0, 2.0, 3.0}, result, 1e-9);
    }

    @Test
    public void testToDoubleArrayFromDoubleArray() {
        double[] input = {1.0, 2.0, 3.0};
        double[] result = VectorMathUtils.toDoubleArray(input);

        assertSame(input, result);
    }

    @Test
    public void testToDoubleArrayFromFloatArray() {
        float[] input = {1.0f, 2.0f, 3.0f};
        double[] result = VectorMathUtils.toDoubleArray(input);

        assertArrayEquals(new double[]{1.0, 2.0, 3.0}, result, 1e-9);
    }

    @Test
    public void testToDoubleArrayUnsupportedThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorMathUtils.toDoubleArray("string"));
    }

    // --- l2Normalize ---

    @Test
    public void testL2Normalize() {
        double[] vec = {3.0, 4.0};
        VectorMathUtils.l2Normalize(vec);

        assertArrayEquals(new double[]{0.6, 0.8}, vec, 1e-9);
    }

    @Test
    public void testL2NormalizeZeroVector() {
        double[] vec = {0.0, 0.0, 0.0};
        VectorMathUtils.l2Normalize(vec);

        assertArrayEquals(new double[]{0.0, 0.0, 0.0}, vec, 1e-9);
    }

    @Test
    public void testL2NormalizeSingleElement() {
        double[] vec = {5.0};
        VectorMathUtils.l2Normalize(vec);

        assertArrayEquals(new double[]{1.0}, vec, 1e-9);
    }

    // --- l2NormalizeList ---

    @Test
    public void testL2NormalizeList() {
        List<Double> result = VectorMathUtils.l2NormalizeList(Arrays.asList(3, 4));

        assertEquals(2, result.size());
        assertEquals(0.6, result.get(0), 1e-9);
        assertEquals(0.8, result.get(1), 1e-9);
    }

    @Test
    public void testL2NormalizeListZeroVector() {
        List<Double> result = VectorMathUtils.l2NormalizeList(Arrays.asList(0, 0));

        assertEquals(Arrays.asList(0.0, 0.0), result);
    }

    @Test
    public void testL2NormalizeListNonNumberThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorMathUtils.l2NormalizeList(Arrays.asList(1, "x")));
    }

    // --- innerProduct ---

    @Test
    public void testInnerProduct() {
        double result = VectorMathUtils.innerProduct(
                Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6));

        assertEquals(32.0, result, 1e-9);
    }

    @Test
    public void testInnerProductWithDoubles() {
        double result = VectorMathUtils.innerProduct(
                Arrays.asList(1.5, 2.5), Arrays.asList(3.0, 4.0));

        assertEquals(14.5, result, 1e-9);
    }

    @Test
    public void testInnerProductZeroResult() {
        double result = VectorMathUtils.innerProduct(
                Arrays.asList(1, 0), Arrays.asList(0, 1));

        assertEquals(0.0, result, 1e-9);
    }

    @Test
    public void testInnerProductLengthMismatchThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorMathUtils.innerProduct(Arrays.asList(1, 2), Arrays.asList(3)));
    }

    @Test
    public void testInnerProductNonNumberThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorMathUtils.innerProduct(Arrays.asList(1, "x"), Arrays.asList(3, 4)));
    }
}
