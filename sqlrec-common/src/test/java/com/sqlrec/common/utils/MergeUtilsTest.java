package com.sqlrec.common.utils;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Functions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
    public void testSnakeMergeNoSources() {
        List<Object> merged = MergeUtils.snakeMerge();

        assertTrue(merged.isEmpty());
    }

    @Test
    public void testSnakeMergeDistinctUsesRowComparer() {
        List<Object[]> a = Arrays.asList(
                new Object[]{1, "a"},
                new Object[]{2, "b"}
        );
        List<Object[]> b = Arrays.asList(
                new Object[]{1, "a"},
                new Object[]{3, "c"}
        );

        List<Object[]> merged = MergeUtils.snakeMergeDistinct(Functions.arrayComparer(), a, b);

        assertEquals(3, merged.size());
        assertArrayEquals(new Object[]{1, "a"}, merged.get(0));
        assertArrayEquals(new Object[]{3, "c"}, merged.get(1));
        assertArrayEquals(new Object[]{2, "b"}, merged.get(2));
    }

    @Test
    public void testWeightedMergeDoesNotChargeDuplicateToQuota() {
        List<Integer> a = Arrays.asList(1, 2);
        List<Integer> b = Arrays.asList(1, 3, 4);

        List<Integer> merged = MergeUtils.weightedMerge(
                new int[]{1, 2},
                10,
                value -> value,
                a,
                b
        );

        assertEquals(Arrays.asList(1, 3, 4, 2), merged);
    }

    @Test
    void weightedMergeClosesTheSourceWithoutReadingPastTheLimit() {
        AtomicBoolean closed = new AtomicBoolean();
        Iterable<Integer> first = new AbstractEnumerable<>() {
            @Override
            public Enumerator<Integer> enumerator() {
                return new Enumerator<>() {
                    private boolean advanced;

                    @Override
                    public Integer current() {
                        return 1;
                    }

                    @Override
                    public boolean moveNext() {
                        assertFalse(advanced, "read past the limit");
                        advanced = true;
                        return true;
                    }

                    @Override
                    public void reset() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {
                        closed.set(true);
                    }
                };
            }
        };
        Iterable<Integer> second = () -> fail("opened another source after reaching the limit");

        assertEquals(List.of(1), MergeUtils.weightedMerge(new int[]{2, 1}, 1, null, first, second));
        assertTrue(closed.get());
    }
}
