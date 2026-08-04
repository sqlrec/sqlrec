package com.sqlrec.common.utils;

import org.apache.calcite.linq4j.Enumerable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MergeUtilsTest {

    @Test
    public void testSnakeMergeTwoSources() {
        List<Integer> a = Arrays.asList(1, 3, 5);
        List<Integer> b = Arrays.asList(2, 4, 6);

        List<Integer> merged = MergeUtils.snakeMerge(a, b);

        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), merged);
    }

    @Test
    public void testSnakeMergeUnequalLengths() {
        List<String> a = Arrays.asList("a", "b", "c", "d");
        List<String> b = Arrays.asList("1", "2");

        List<String> merged = MergeUtils.snakeMerge(a, b);

        assertEquals(Arrays.asList("a", "1", "b", "2", "c", "d"), merged);
    }

    @Test
    public void testSnakeMergeSingleSource() {
        List<Integer> a = Arrays.asList(1, 2, 3);

        List<Integer> merged = MergeUtils.snakeMerge(a);

        assertEquals(Arrays.asList(1, 2, 3), merged);
    }

    @Test
    public void testSnakeMergeEmptySources() {
        List<Integer> empty = Collections.emptyList();

        List<Integer> merged = MergeUtils.snakeMerge(empty, empty);

        assertTrue(merged.isEmpty());
    }

    @Test
    public void testSnakeMergeMixedEmptyAndNonEmpty() {
        List<Integer> empty = Collections.emptyList();
        List<Integer> a = Arrays.asList(1, 2);

        List<Integer> merged = MergeUtils.snakeMerge(empty, a, empty);

        assertEquals(Arrays.asList(1, 2), merged);
    }

    @Test
    public void testSnakeMergeEnumerable() {
        List<Integer> a = Arrays.asList(1, 3);
        List<Integer> b = Arrays.asList(2, 4);

        Enumerable<Integer> enumerable = MergeUtils.snakeMergeEnumerable(a, b);

        List<Integer> result = enumerable.toList();
        assertEquals(Arrays.asList(1, 2, 3, 4), result);
    }

    @Test
    public void testSnakeMergeNoSources() {
        List<Object> merged = MergeUtils.snakeMerge();

        assertTrue(merged.isEmpty());
    }
}
