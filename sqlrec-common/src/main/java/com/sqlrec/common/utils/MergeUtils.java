package com.sqlrec.common.utils;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeUtils {
    public static <T> List<T> snakeMerge(Iterable<T>... sources) {
        List<Iterator<T>> iterators = new ArrayList<>();
        for (Iterable<T> source : sources) {
            iterators.add(source.iterator());
        }

        List<T> merged = new ArrayList<>();
        while (true) {
            boolean allEmpty = true;
            for (Iterator<T> iterator : iterators) {
                if (iterator.hasNext()) {
                    merged.add(iterator.next());
                    allEmpty = false;
                }
            }
            if (allEmpty) {
                break;
            }
        }

        return merged;
    }

    public static <T> Enumerable<T> snakeMergeEnumerable(Iterable<T>... sources) {
        List<T> merged = snakeMerge(sources);
        return Linq4j.asEnumerable(merged);
    }
}
