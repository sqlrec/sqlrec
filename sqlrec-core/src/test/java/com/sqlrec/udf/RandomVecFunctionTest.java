package com.sqlrec.udf;

import com.sqlrec.udf.scalar.RandomVecFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class RandomVecFunctionTest {
    private RandomVecFunction function;

    @BeforeEach
    public void setUp() {
        function = new RandomVecFunction();
    }

    @Test
    public void testRandomVecBasic() {
        Object result = function.evaluate("10");

        assertNotNull(result);
        assertTrue(result instanceof List);

        List<?> vector = (List<?>) result;
        assertEquals(10, vector.size());

        for (Object element : vector) {
            assertTrue(element instanceof Double);
            Double value = (Double) element;
            assertTrue(value >= 0.0 && value <= 1.0);
        }
    }

    @Test
    public void testRandomVecNormalized() {
        Object result = function.evaluate("100");

        assertNotNull(result);
        assertTrue(result instanceof List);

        List<?> vector = (List<?>) result;
        assertEquals(100, vector.size());

        double sumOfSquares = 0.0;
        for (Object element : vector) {
            assertTrue(element instanceof Double);
            Double value = (Double) element;
            sumOfSquares += value * value;
        }

        double norm = Math.sqrt(sumOfSquares);
        assertEquals(1.0, norm, 0.0001);
    }

    @Test
    public void testRandomVecDifferentDimensions() {
        Object result1 = function.evaluate("5");
        Object result2 = function.evaluate("50");
        Object result3 = function.evaluate("500");

        assertNotNull(result1);
        assertNotNull(result2);
        assertNotNull(result3);

        assertEquals(5, ((List<?>) result1).size());
        assertEquals(50, ((List<?>) result2).size());
        assertEquals(500, ((List<?>) result3).size());
    }

    @Test
    public void testRandomVecNullInput() {
        Object result = function.evaluate(null);
        assertNull(result);
    }

    @Test
    public void testRandomVecInvalidDimension() {
        assertThrows(RuntimeException.class, () -> {
            function.evaluate("invalid");
        });
    }

    @Test
    public void testRandomVecZeroDimension() {
        assertThrows(RuntimeException.class, () -> {
            function.evaluate("0");
        });
    }

    @Test
    public void testRandomVecNegativeDimension() {
        assertThrows(RuntimeException.class, () -> {
            function.evaluate("-10");
        });
    }

    @Test
    public void testRandomVecMultipleCallsDifferent() {
        Object result1 = function.evaluate("10");
        Object result2 = function.evaluate("10");

        assertNotNull(result1);
        assertNotNull(result2);

        List<?> vector1 = (List<?>) result1;
        List<?> vector2 = (List<?>) result2;

        boolean hasDifference = false;
        for (int i = 0; i < vector1.size(); i++) {
            Double v1 = (Double) vector1.get(i);
            Double v2 = (Double) vector2.get(i);
            if (Math.abs(v1 - v2) > 0.0001) {
                hasDifference = true;
                break;
            }
        }

        assertTrue(hasDifference, "Multiple calls should generate different random vectors");
    }

    @Test
    public void testRandomVecLargeDimension() {
        Object result = function.evaluate("1000");

        assertNotNull(result);
        assertTrue(result instanceof List);

        List<?> vector = (List<?>) result;
        assertEquals(1000, vector.size());

        double sumOfSquares = 0.0;
        for (Object element : vector) {
            assertTrue(element instanceof Double);
            Double value = (Double) element;
            sumOfSquares += value * value;
        }

        double norm = Math.sqrt(sumOfSquares);
        assertEquals(1.0, norm, 0.0001);
    }
}
