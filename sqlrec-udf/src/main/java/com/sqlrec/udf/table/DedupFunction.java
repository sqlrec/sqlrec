package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DedupFunction {
    /** Keeps the first row for each key, preserving input order. */
    public CacheTable evaluate(CacheTable input, String column) {
        if (input == null) {
            throw new IllegalArgumentException("input table cannot be null");
        }
        if (column == null || column.isEmpty()) {
            throw new IllegalArgumentException("column cannot be null or empty");
        }
        int index = DataTypeUtils.findFieldIndex(input.getDataFields(), column);
        if (index < 0) {
            throw new IllegalArgumentException("column not found in input table: " + column);
        }
        Set<Object> seen = new HashSet<>();
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : input.scan(null)) {
            if (seen.add(row[index])) {
                rows.add(row);
            }
        }
        return new CacheTable("dedup_output", Linq4j.asEnumerable(rows), input.getDataFields());
    }

    public CacheTable evaluate(CacheTable input, CacheTable dedupTable, String column) {
        return evaluate(input, dedupTable, column, column);
    }

    public CacheTable evaluate(CacheTable input, CacheTable dedupTable, String col1, String col2) {
        if (input == null) {
            throw new IllegalArgumentException("input table cannot be null");
        }
        if (col1 == null || col1.isEmpty()) {
            throw new IllegalArgumentException("col1 cannot be null or empty");
        }
        if (col2 == null || col2.isEmpty()) {
            throw new IllegalArgumentException("col2 cannot be null or empty");
        }

        int inputColIndex = DataTypeUtils.findFieldIndex(input.getDataFields(), col1);
        if (inputColIndex == -1) {
            throw new IllegalArgumentException("col1 not found in input table: " + col1);
        }

        Set<Object> existingKeys = new HashSet<>();
        if (dedupTable != null) {
            int dedupColIndex = DataTypeUtils.findFieldIndex(dedupTable.getDataFields(), col2);
            if (dedupColIndex == -1) {
                throw new IllegalArgumentException("col2 not found in dedup table: " + col2);
            }

            Enumerable<Object[]> dedupEnumerable = dedupTable.scan(null);
            if (dedupEnumerable != null) {
                for (Object[] row : dedupEnumerable) {
                    Object value = row[dedupColIndex];
                    existingKeys.add(value);
                }
            }
        }

        Enumerable<Object[]> inputEnumerable = input.scan(null);
        List<Object[]> newData = new ArrayList<>();

        if (inputEnumerable != null) {
            for (Object[] row : inputEnumerable) {
                Object value = row[inputColIndex];
                if (!existingKeys.contains(value)) {
                    newData.add(row);
                }
            }
        }

        return new CacheTable("dedup_output", Linq4j.asEnumerable(newData), input.getDataFields());
    }
}
