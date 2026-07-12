package com.sqlrec.udf.scalar;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class IpFunctionTest {

    private final IpFunction function = new IpFunction();

    @Test
    public void testInnerProductBasic() {
        Double result = function.evaluate(Arrays.asList(1.0, 0.0), Arrays.asList(0.0, 1.0));
        assertEquals(0.0, result, 0.0001);
    }

    @Test
    public void testInnerProductSameVector() {
        Double result = function.evaluate(Arrays.asList(3.0, 4.0), Arrays.asList(3.0, 4.0));
        assertEquals(25.0, result, 0.0001);
    }

    @Test
    public void testInnerProductParallel() {
        Double result = function.evaluate(Arrays.asList(1.0, 2.0), Arrays.asList(2.0, 4.0));
        assertEquals(10.0, result, 0.0001);
    }

    @Test
    public void testInnerProductWithNullFirst() {
        assertNull(function.evaluate((Object) null, Arrays.asList(1.0, 2.0)));
    }

    @Test
    public void testInnerProductWithNullSecond() {
        assertNull(function.evaluate(Arrays.asList(1.0, 2.0), (Object) null));
    }

    @Test
    public void testInnerProductMixedTypes() {
        Double result = function.evaluate(Arrays.asList(1, 2, 3), Arrays.asList(4.0, 5.0, 6.0));
        assertEquals(32.0, result, 0.0001);
    }

    @Test
    public void testInnerProductDimensionMismatch() {
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(Arrays.asList(1.0, 2.0), Arrays.asList(1.0));
        });
    }

    @Test
    public void testInnerProductNonListInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate((Object) "not a list", Arrays.asList(1.0));
        });
    }

    @Test
    public void testInnerProductSingleElement() {
        Double result = function.evaluate(Arrays.asList(5.0), Arrays.asList(3.0));
        assertEquals(15.0, result, 0.0001);
    }

    @Test
    public void testInnerProductZeroVectors() {
        Double result = function.evaluate(Arrays.asList(0.0, 0.0), Arrays.asList(0.0, 0.0));
        assertEquals(0.0, result, 0.0001);
    }
}
