package com.sqlrec.utils;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeUtils {
    public static <TSource> List<TSource> snakeMerge(Iterable<TSource>... sources) {
        List<Iterator<TSource>> iterators = new ArrayList<>();
        for (Iterable<TSource> source : sources) {
            iterators.add(source.iterator());
        }

        List<TSource> merged = new ArrayList<>();
        while (true) {
            boolean allEmpty = true;
            for (Iterator<TSource> iterator : iterators) {
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

    public static <TSource> Enumerable<TSource> snakeMergeEnumerable(Iterable<TSource>... sources) {
        List<TSource> merged = snakeMerge(sources);
        return Linq4j.asEnumerable(merged);
    }
}
