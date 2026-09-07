package com.sqlrec.common.utils;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.EqualityComparer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class MergeUtils {
    public static <T> List<T> snakeMerge(Iterable<T>... sources) {
        int[] weights = new int[sources.length];
        Arrays.fill(weights, 1);
        return merge(weights, Integer.MAX_VALUE, value -> true, sources);
    }

    public static <T> List<T> snakeMergeDistinct(
            EqualityComparer<T> comparer,
            Iterable<T>... sources
    ) {
        Predicate<T> isNewValue;
        if (comparer == null) {
            Set<T> seenValues = new HashSet<>();
            isNewValue = seenValues::add;
        } else {
            Set<EqualityKey<T>> seenValues = new HashSet<>();
            isNewValue = value -> seenValues.add(new EqualityKey<>(value, comparer));
        }

        int[] weights = new int[sources.length];
        Arrays.fill(weights, 1);
        return merge(weights, Integer.MAX_VALUE, isNewValue, sources);
    }

    public static <T> List<T> weightedMerge(
            int[] weights,
            int limit,
            Function<? super T, ?> keySelector,
            Iterable<T>... sources
    ) {
        Predicate<T> isNewValue;
        if (keySelector == null) {
            isNewValue = value -> true;
        } else {
            Set<Object> seenKeys = new HashSet<>();
            isNewValue = value -> seenKeys.add(keySelector.apply(value));
        }
        return merge(weights, limit, isNewValue, sources);
    }

    private static <T> List<T> merge(
            int[] weights,
            int limit,
            Predicate<? super T> isNewValue,
            Iterable<T>... sources
    ) {
        if (weights == null || weights.length != sources.length) {
            throw new IllegalArgumentException("Number of weights must match number of sources");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, got: " + limit);
        }
        for (int weight : weights) {
            if (weight <= 0) {
                throw new IllegalArgumentException("Weight must be positive, got: " + weight);
            }
        }

        List<Iterator<T>> iterators = new ArrayList<>();
        try {
            for (Iterable<T> source : sources) {
                iterators.add(source.iterator());
            }

            List<T> merged = new ArrayList<>();
            while (merged.size() < limit) {
                boolean consumedAnyValue = false;
                for (int i = 0; i < iterators.size(); i++) {
                    Iterator<T> iterator = iterators.get(i);
                    int accepted = 0;
                    while (accepted < weights[i]
                            && iterator.hasNext()
                            && merged.size() < limit) {
                        T value = iterator.next();
                        consumedAnyValue = true;
                        if (isNewValue.test(value)) {
                            merged.add(value);
                            accepted++;
                        }
                    }
                }
                if (!consumedAnyValue) {
                    break;
                }
            }

            return merged;
        } finally {
            for (Iterator<T> iterator : iterators) {
                if (iterator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) iterator).close();
                    } catch (Exception e) {
                        // ignore close exception
                    }
                }
            }
        }
    }

    public static <T> Enumerable<T> snakeMergeEnumerable(Iterable<T>... sources) {
        List<T> merged = snakeMerge(sources);
        return Linq4j.asEnumerable(merged);
    }

    public static <T> Enumerable<T> snakeMergeDistinctEnumerable(
            EqualityComparer<T> comparer,
            Iterable<T>... sources
    ) {
        return Linq4j.asEnumerable(snakeMergeDistinct(comparer, sources));
    }

    private static final class EqualityKey<T> {
        private final T value;
        private final EqualityComparer<T> comparer;

        private EqualityKey(T value, EqualityComparer<T> comparer) {
            this.value = value;
            this.comparer = comparer;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof EqualityKey)) {
                return false;
            }
            EqualityKey<?> other = (EqualityKey<?>) obj;
            if (comparer != other.comparer) {
                return false;
            }
            @SuppressWarnings("unchecked")
            T otherValue = (T) other.value;
            return comparer.equal(value, otherValue);
        }

        @Override
        public int hashCode() {
            return comparer.hashCode(value);
        }
    }
}
