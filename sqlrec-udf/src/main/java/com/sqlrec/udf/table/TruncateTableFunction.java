package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.List;

public class TruncateTableFunction {
    public CacheTable evaluate(CacheTable input, Integer limit) {
        return evaluate(input, 0, limit);
    }

    public CacheTable evaluate(CacheTable input, Integer start, Integer end) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("start and end parameters cannot be null");
        }

        if (start < 0) {
            throw new IllegalArgumentException("start parameter must be non-negative");
        }

        if (end < 0) {
            throw new IllegalArgumentException("end parameter must be non-negative");
        }

        if (start > end) {
            throw new IllegalArgumentException("start parameter must be less than or equal to end parameter");
        }

        Enumerable<Object[]> enumerable = input.scan(null);
        List<Object[]> newData = new ArrayList<>();
        
        if (enumerable != null) {
            int currentIndex = 0;
            for (Object[] data : enumerable) {
                if (currentIndex >= start && currentIndex < end) {
                    newData.add(data);
                }
                currentIndex++;
                if (currentIndex >= end) {
                    break;
                }
            }
        }

        return new CacheTable("output", Linq4j.asEnumerable(newData), input.getDataFields());
    }
}
