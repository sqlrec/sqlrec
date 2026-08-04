package com.sqlrec.common.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DataTransformUtilsTest {

    // --- convertToFloatVec ---

    @Test
    public void testConvertToFloatVecFromIntList() {
        List<Float> result = DataTransformUtils.convertToFloatVec(Arrays.asList(1, 2, 3));

        assertEquals(Arrays.asList(1.0f, 2.0f, 3.0f), result);
    }

    @Test
    public void testConvertToFloatVecFromDoubleList() {
        List<Float> result = DataTransformUtils.convertToFloatVec(Arrays.asList(1.5, 2.5));

        assertEquals(Arrays.asList(1.5f, 2.5f), result);
    }

    @Test
    public void testConvertToFloatVecEmptyList() {
        List<Float> result = DataTransformUtils.convertToFloatVec(Collections.emptyList());

        assertTrue(result.isEmpty());
    }

    @Test
    public void testConvertToFloatVecNonListThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> DataTransformUtils.convertToFloatVec("not a list"));
    }

    @Test
    public void testConvertToFloatVecNonNumberElementThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> DataTransformUtils.convertToFloatVec(Arrays.asList(1, "two", 3)));
    }

    // --- toDoubleArray ---

    @Test
    public void testToDoubleArrayFromList() {
        double[] result = DataTransformUtils.toDoubleArray(Arrays.asList(1, 2, 3));

        assertArrayEquals(new double[]{1.0, 2.0, 3.0}, result, 1e-9);
    }

    @Test
    public void testToDoubleArrayFromDoubleArray() {
        double[] input = {1.0, 2.0, 3.0};
        double[] result = DataTransformUtils.toDoubleArray(input);

        assertSame(input, result);
    }

    @Test
    public void testToDoubleArrayFromFloatArray() {
        float[] input = {1.0f, 2.0f, 3.0f};
        double[] result = DataTransformUtils.toDoubleArray(input);

        assertArrayEquals(new double[]{1.0, 2.0, 3.0}, result, 1e-9);
    }

    @Test
    public void testToDoubleArrayUnsupportedThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> DataTransformUtils.toDoubleArray("string"));
    }

    // --- l2Normalize ---

    @Test
    public void testL2Normalize() {
        double[] vec = {3.0, 4.0};
        DataTransformUtils.l2Normalize(vec);

        assertArrayEquals(new double[]{0.6, 0.8}, vec, 1e-9);
    }

    @Test
    public void testL2NormalizeZeroVector() {
        double[] vec = {0.0, 0.0, 0.0};
        DataTransformUtils.l2Normalize(vec);

        assertArrayEquals(new double[]{0.0, 0.0, 0.0}, vec, 1e-9);
    }

    @Test
    public void testL2NormalizeSingleElement() {
        double[] vec = {5.0};
        DataTransformUtils.l2Normalize(vec);

        assertArrayEquals(new double[]{1.0}, vec, 1e-9);
    }

    // --- l2NormalizeList ---

    @Test
    public void testL2NormalizeList() {
        List<Double> result = DataTransformUtils.l2NormalizeList(Arrays.asList(3, 4));

        assertEquals(2, result.size());
        assertEquals(0.6, result.get(0), 1e-9);
        assertEquals(0.8, result.get(1), 1e-9);
    }

    @Test
    public void testL2NormalizeListZeroVector() {
        List<Double> result = DataTransformUtils.l2NormalizeList(Arrays.asList(0, 0));

        assertEquals(Arrays.asList(0.0, 0.0), result);
    }

    @Test
    public void testL2NormalizeListNonNumberThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> DataTransformUtils.l2NormalizeList(Arrays.asList(1, "x")));
    }

    // --- innerProduct ---

    @Test
    public void testInnerProduct() {
        double result = DataTransformUtils.innerProduct(
                Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6));

        assertEquals(32.0, result, 1e-9);
    }

    @Test
    public void testInnerProductWithDoubles() {
        double result = DataTransformUtils.innerProduct(
                Arrays.asList(1.5, 2.5), Arrays.asList(3.0, 4.0));

        assertEquals(14.5, result, 1e-9);
    }

    @Test
    public void testInnerProductZeroResult() {
        double result = DataTransformUtils.innerProduct(
                Arrays.asList(1, 0), Arrays.asList(0, 1));

        assertEquals(0.0, result, 1e-9);
    }

    @Test
    public void testInnerProductLengthMismatchThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> DataTransformUtils.innerProduct(Arrays.asList(1, 2), Arrays.asList(3)));
    }

    @Test
    public void testInnerProductNonNumberThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> DataTransformUtils.innerProduct(Arrays.asList(1, "x"), Arrays.asList(3, 4)));
    }

    // --- getMsgEnumerable ---

    @Test
    public void testGetMsgEnumerable() {
        Object[] result = DataTransformUtils.getMsgEnumerable("hello").toList().toArray(new Object[0]);

        assertEquals(1, result.length);
        assertArrayEquals(new String[]{"hello"}, (String[]) result[0]);
    }

    @Test
    public void testGetMsgEnumerableNull() {
        assertNull(DataTransformUtils.getMsgEnumerable(null));
    }

    // --- convertListToEnumerable ---

    @Test
    public void testConvertListToEnumerable() {
        List<String> input = Arrays.asList("a", "b", "c");
        List<Object[]> result = DataTransformUtils.convertListToEnumerable(input).toList();

        assertEquals(3, result.size());
        assertEquals("a", result.get(0)[0]);
        assertEquals("b", result.get(1)[0]);
        assertEquals("c", result.get(2)[0]);
    }

    @Test
    public void testConvertListToEnumerableNull() {
        assertNull(DataTransformUtils.convertListToEnumerable(null));
    }

    // --- convertListToArrayToEnumerable ---

    @Test
    public void testConvertListToArrayToEnumerable() {
        List<List<Integer>> input = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4)
        );
        List<Object[]> result = DataTransformUtils.convertListToArrayToEnumerable(input).toList();

        assertEquals(2, result.size());
        assertArrayEquals(new Object[]{1, 2}, result.get(0));
        assertArrayEquals(new Object[]{3, 4}, result.get(1));
    }

    @Test
    public void testConvertListToArrayToEnumerableNull() {
        assertNull(DataTransformUtils.convertListToArrayToEnumerable(null));
    }
}
