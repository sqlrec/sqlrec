package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DedupFunction {
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

        int inputColIndex = -1;
        List<RelDataTypeField> inputFields = input.getDataFields();
        for (int i = 0; i < inputFields.size(); i++) {
            if (inputFields.get(i).getName().equalsIgnoreCase(col1)) {
                inputColIndex = i;
                break;
            }
        }
        if (inputColIndex == -1) {
            throw new IllegalArgumentException("col1 not found in input table: " + col1);
        }

        Set<String> existingKeys = new HashSet<>();
        if (dedupTable != null) {
            int dedupColIndex = -1;
            List<RelDataTypeField> dedupFields = dedupTable.getDataFields();
            for (int i = 0; i < dedupFields.size(); i++) {
                if (dedupFields.get(i).getName().equalsIgnoreCase(col2)) {
                    dedupColIndex = i;
                    break;
                }
            }
            if (dedupColIndex == -1) {
                throw new IllegalArgumentException("col2 not found in dedup table: " + col2);
            }

            Enumerable<Object[]> dedupEnumerable = dedupTable.scan(null);
            if (dedupEnumerable != null) {
                for (Object[] row : dedupEnumerable) {
                    Object value = row[dedupColIndex];
                    existingKeys.add(value == null ? "null" : value.toString());
                }
            }
        }

        Enumerable<Object[]> inputEnumerable = input.scan(null);
        List<Object[]> newData = new ArrayList<>();

        if (inputEnumerable != null) {
            for (Object[] row : inputEnumerable) {
                Object value = row[inputColIndex];
                String key = value == null ? "null" : value.toString();
                if (!existingKeys.contains(key)) {
                    existingKeys.add(key);
                    newData.add(row);
                }
            }
        }

        return new CacheTable("dedup_output", Linq4j.asEnumerable(newData), input.getDataFields());
    }
}