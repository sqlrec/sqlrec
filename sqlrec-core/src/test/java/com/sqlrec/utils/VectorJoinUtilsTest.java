package com.sqlrec.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class VectorJoinUtilsTest {

    @Test
    public void testBuildProjectRowWithNullProjectColumns() {
        Object[] joinRow = new Object[]{1, "Alice", 100, "Engineer"};

        Object[] result = VectorJoinUtils.buildProjectRow(joinRow, null, 4);

        assertNotNull(result);
        assertSame(joinRow, result);
    }

    @Test
    public void testBuildProjectRowWithEmptyProjectColumns() {
        Object[] joinRow = new Object[]{1, "Alice", 100, "Engineer"};

        Object[] result = VectorJoinUtils.buildProjectRow(joinRow, Collections.emptyList(), 0);

        assertNotNull(result);
        assertSame(joinRow, result);
    }

    @Test
    public void testBuildProjectRowWithProjectColumns() {
        Object[] joinRow = new Object[]{1, "Alice", 100, "Engineer"};
        List<Integer> projectColumns = Arrays.asList(0, 2);

        Object[] result = VectorJoinUtils.buildProjectRow(joinRow, projectColumns, 2);

        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals(1, result[0]);
        assertEquals(100, result[1]);
    }

    @Test
    public void testBuildProjectRowWithAllColumns() {
        Object[] joinRow = new Object[]{1, "Alice", 100, "Engineer"};
        List<Integer> projectColumns = Arrays.asList(0, 1, 2, 3);

        Object[] result = VectorJoinUtils.buildProjectRow(joinRow, projectColumns, 4);

        assertNotNull(result);
        assertEquals(4, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
        assertEquals(100, result[2]);
        assertEquals("Engineer", result[3]);
    }

    @Test
    public void testBuildProjectRowWithOutOfBoundIndex() {
        Object[] joinRow = new Object[]{1, "Alice"};
        List<Integer> projectColumns = Arrays.asList(0, 5);

        Object[] result = VectorJoinUtils.buildProjectRow(joinRow, projectColumns, 2);

        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals(1, result[0]);
        assertNull(result[1]);
    }

    @Test
    public void testBuildProjectRowWithNullColumnIndex() {
        Object[] joinRow = new Object[]{1, "Alice", 100, "Engineer"};
        List<Integer> projectColumns = Arrays.asList(0, null, 2);

        Object[] result = VectorJoinUtils.buildProjectRow(joinRow, projectColumns, 3);

        assertNotNull(result);
        assertEquals(3, result.length);
        assertEquals(1, result[0]);
        assertNull(result[1]);
        assertEquals(100, result[2]);
    }

    @Test
    public void testBuildProjectRowWithNullValues() {
        Object[] joinRow = new Object[]{null, "Alice", null, "Engineer"};
        List<Integer> projectColumns = Arrays.asList(0, 1, 2, 3);

        Object[] result = VectorJoinUtils.buildProjectRow(joinRow, projectColumns, 4);

        assertNotNull(result);
        assertEquals(4, result.length);
        assertNull(result[0]);
        assertEquals("Alice", result[1]);
        assertNull(result[2]);
        assertEquals("Engineer", result[3]);
    }

    @Test
    public void testBuildProjectRowWithSmallerProjectSize() {
        Object[] joinRow = new Object[]{1, "Alice", 100, "Engineer"};
        List<Integer> projectColumns = Arrays.asList(0, 1, 2, 3);

        Object[] result = VectorJoinUtils.buildProjectRow(joinRow, projectColumns, 2);

        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
    }

    @Test
    public void testBuildProjectRowWithLargerProjectSize() {
        Object[] joinRow = new Object[]{1, "Alice"};
        List<Integer> projectColumns = Arrays.asList(0, 1);

        Object[] result = VectorJoinUtils.buildProjectRow(joinRow, projectColumns, 5);

        assertNotNull(result);
        assertEquals(5, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
        assertNull(result[2]);
        assertNull(result[3]);
        assertNull(result[4]);
    }

    @Test
    public void testBuildProjectRowWithSingleColumn() {
        Object[] joinRow = new Object[]{1, "Alice", 100, "Engineer"};
        List<Integer> projectColumns = Collections.singletonList(2);

        Object[] result = VectorJoinUtils.buildProjectRow(joinRow, projectColumns, 1);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals(100, result[0]);
    }

    @Test
    public void testVectorJoinWithNullLeft() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VectorJoinUtils.vectorJoin(null, null, null, 0, "embedding", 10, null)
        );
        assertEquals("left table is null", exception.getMessage());
    }

    @Test
    public void testVectorJoinConfigDefaultValues() {
        VectorJoinUtils.VectorJoinConfig config = new VectorJoinUtils.VectorJoinConfig();

        assertEquals(0, config.leftEmbeddingColIndex);
        assertEquals(0, config.rightEmbeddingColIndex);
        assertNull(config.rightEmbeddingColName);
        assertNull(config.filterCondition);
        assertEquals(0, config.limit);
        assertNotNull(config.projectColumns);
        assertTrue(config.projectColumns.isEmpty());
        assertNull(config.projectRowType);
        assertNull(config.collation);
    }
}
