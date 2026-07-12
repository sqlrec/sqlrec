package com.sqlrec.udf.scalar;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class L2NormFunctionTest {

    private final L2NormFunction function = new L2NormFunction();

    @Test
    public void testL2NormBasic() {
        List<Double> result = function.evaluate(Arrays.asList(3.0, 4.0));
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(0.6, result.get(0), 0.0001);
        assertEquals(0.8, result.get(1), 0.0001);
    }

    @Test
    public void testL2NormAlreadyNormalized() {
        List<Double> result = function.evaluate(Arrays.asList(0.6, 0.8));
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(0.6, result.get(0), 0.0001);
        assertEquals(0.8, result.get(1), 0.0001);
    }

    @Test
    public void testL2NormUnitVector() {
        List<Double> result = function.evaluate(Arrays.asList(1.0, 0.0, 0.0));
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals(1.0, result.get(0), 0.0001);
        assertEquals(0.0, result.get(1), 0.0001);
        assertEquals(0.0, result.get(2), 0.0001);
    }

    @Test
    public void testL2NormWithNull() {
        assertNull(function.evaluate((Object) null));
    }

    @Test
    public void testL2NormNonListInput() {
        assertThrows(RuntimeException.class, () -> {
            function.evaluate((Object) "not a list");
        });
    }

    @Test
    public void testL2NormZeroVector() {
        List<Double> result = function.evaluate(Arrays.asList(0.0, 0.0));
        assertNotNull(result);
        assertEquals(0.0, result.get(0), 0.0001);
        assertEquals(0.0, result.get(1), 0.0001);
    }

    @Test
    public void testL2NormResultIsUnitLength() {
        List<Double> result = function.evaluate(Arrays.asList(1.0, 2.0, 3.0, 4.0));
        double sumOfSquares = 0.0;
        for (Double v : result) {
            sumOfSquares += v * v;
        }
        assertEquals(1.0, Math.sqrt(sumOfSquares), 0.0001);
    }

    @Test
    public void testL2NormIntegerInput() {
        List<Double> result = function.evaluate(Arrays.asList(3, 4));
        assertNotNull(result);
        assertEquals(0.6, result.get(0), 0.0001);
        assertEquals(0.8, result.get(1), 0.0001);
    }
}
