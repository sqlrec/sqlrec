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
        requireInput(input);
        requireColumnName(column, "column");
        int index = requireColumnIndex(input, column, "column", "input");
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
        requireInput(input);
        requireColumnName(col1, "col1");
        requireColumnName(col2, "col2");
        int inputColIndex = requireColumnIndex(input, col1, "col1", "input");

        Set<Object> existingKeys = new HashSet<>();
        if (dedupTable != null) {
            int dedupColIndex = requireColumnIndex(dedupTable, col2, "col2", "dedup");
            Enumerable<Object[]> dedupEnumerable = dedupTable.scan(null);
            if (dedupEnumerable != null) {
                for (Object[] row : dedupEnumerable) {
                    existingKeys.add(row[dedupColIndex]);
                }
            }
        }

        Enumerable<Object[]> inputEnumerable = input.scan(null);
        List<Object[]> newData = new ArrayList<>();

        if (inputEnumerable != null) {
            for (Object[] row : inputEnumerable) {
                if (!existingKeys.contains(row[inputColIndex])) {
                    newData.add(row);
                }
            }
        }

        return new CacheTable("dedup_output", Linq4j.asEnumerable(newData), input.getDataFields());
    }

    private static void requireInput(CacheTable input) {
        if (input == null) {
            throw new IllegalArgumentException("input table cannot be null");
        }
    }

    private static void requireColumnName(String column, String argument) {
        if (column == null || column.isEmpty()) {
            throw new IllegalArgumentException(argument + " cannot be null or empty");
        }
    }

    private static int requireColumnIndex(CacheTable table, String column, String argument, String tableRole) {
        int index = DataTypeUtils.findFieldIndex(table.getDataFields(), column);
        if (index < 0) {
            throw new IllegalArgumentException(argument + " not found in " + tableRole + " table: " + column);
        }
        return index;
    }
}
