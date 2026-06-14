package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class WeightedMergeFunction {

    public CacheTable evaluate(String primaryKey, String weights, String limit, CacheTable... tables) {
        if (tables == null || tables.length == 0) {
            throw new IllegalArgumentException("At least one table is required");
        }
        if (primaryKey == null || primaryKey.isEmpty()) {
            throw new IllegalArgumentException("primaryKey cannot be null or empty");
        }
        if (weights == null || weights.isEmpty()) {
            throw new IllegalArgumentException("weights cannot be null or empty");
        }
        if (limit == null || limit.isEmpty()) {
            throw new IllegalArgumentException("limit cannot be null or empty");
        }

        // Check all tables have the same schema
        List<RelDataTypeField> referenceFields = tables[0].getDataFields();
        for (int i = 1; i < tables.length; i++) {
            DataTypeUtils.checkTableSchemaIdentical(referenceFields, tables[i].getDataFields(), i);
        }

        int pkIndex = DataTypeUtils.findFieldIndex(referenceFields, primaryKey.trim());
        if (pkIndex < 0) {
            throw new IllegalArgumentException("primaryKey field not found: " + primaryKey);
        }

        int limitNum;
        try {
            limitNum = Integer.parseInt(limit.trim());
            if (limitNum <= 0) {
                throw new IllegalArgumentException("limit must be positive, got: " + limitNum);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid limit: " + limit);
        }

        String[] weightParts = weights.split(",");
        if (weightParts.length != tables.length) {
            throw new IllegalArgumentException("Number of weights (" + weightParts.length
                    + ") must match number of tables (" + tables.length + ")");
        }

        int[] weightArray = new int[weightParts.length];
        for (int i = 0; i < weightParts.length; i++) {
            try {
                weightArray[i] = Integer.parseInt(weightParts[i].trim());
                if (weightArray[i] <= 0) {
                    throw new IllegalArgumentException("Weight must be positive, got: " + weightArray[i]);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid weight value: " + weightParts[i]);
            }
        }

        // Collect iterators from each table
        List<Iterator<Object[]>> iterators = new ArrayList<>();
        for (CacheTable table : tables) {
            Enumerable<Object[]> enumerable = table.scan(null);
            iterators.add(enumerable == null ? Collections.<Object[]>emptyIterator() : enumerable.iterator());
        }

        Set<String> seenKeys = new HashSet<>();
        List<Object[]> merged = new ArrayList<>();

        while (merged.size() < limitNum) {
            boolean anyHasNext = false;
            for (int i = 0; i < iterators.size(); i++) {
                Iterator<Object[]> it = iterators.get(i);
                int w = weightArray[i];
                int taken = 0;
                while (taken < w && it.hasNext() && merged.size() < limitNum) {
                    Object[] row = it.next();
                    Object pkValue = row[pkIndex];
                    String key = pkValue == null ? "null" : pkValue.toString();
                    if (!seenKeys.contains(key)) {
                        seenKeys.add(key);
                        merged.add(row);
                        taken++;
                    }
                    anyHasNext = true;
                }
                if (it.hasNext()) {
                    anyHasNext = true;
                }
            }
            if (!anyHasNext) {
                break;
            }
        }

        return new CacheTable("weighted_merge_output", Linq4j.asEnumerable(merged), referenceFields);
    }
}
