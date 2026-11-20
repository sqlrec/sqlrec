package com.sqlrec.common.utils;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.EqualityComparer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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

        List<Enumerator<T>> enumerators = new ArrayList<>(sources.length);
        try {
            List<T> merged = new ArrayList<>();
            while (merged.size() < limit) {
                int previousSize = merged.size();
                for (int i = 0; i < sources.length && merged.size() < limit; i++) {
                    if (i == enumerators.size()) {
                        enumerators.add(Linq4j.iterableEnumerator(sources[i]));
                    }
                    Enumerator<T> enumerator = enumerators.get(i);
                    int accepted = 0;
                    while (accepted < weights[i]
                            && merged.size() < limit
                            && enumerator.moveNext()) {
                        T value = enumerator.current();
                        if (isNewValue.test(value)) {
                            merged.add(value);
                            accepted++;
                        }
                    }
                }
                if (merged.size() == previousSize) {
                    break;
                }
            }

            return merged;
        } finally {
            for (Enumerator<T> enumerator : enumerators) {
                try {
                    enumerator.close();
                } catch (Exception ignored) {
                }
            }
        }
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
