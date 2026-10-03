package com.sqlrec.udf.table;

import com.sqlrec.common.runtime.UnionLikeTableFunction;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MergeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class WeightedMergeFunction implements UnionLikeTableFunction {

    public CacheTable evaluate(String primaryKey, String weights, String limit, CacheTable... tables) {
        checkArguments(weights, limit, tables);
        List<RelDataTypeField> fields = checkSchemas(tables);
        boolean dedupEnabled = StringUtils.isNotEmpty(primaryKey);
        int primaryKeyIndex = resolvePrimaryKeyIndex(primaryKey, fields);
        int limitNum = parseLimit(limit);
        int[] weightArray = parseWeights(weights, tables.length);

        List<Object[]> merged = MergeUtils.weightedMerge(
                weightArray,
                limitNum,
                dedupEnabled
                        ? row -> row[primaryKeyIndex] == null ? "null" : row[primaryKeyIndex].toString()
                        : null,
                scanTables(tables)
        );
        return new CacheTable("weighted_merge_output", Linq4j.asEnumerable(merged), fields);
    }

    private static void checkArguments(String weights, String limit, CacheTable[] tables) {
        if (tables == null || tables.length == 0) {
            throw new IllegalArgumentException("At least one table is required");
        }
        if (weights == null || weights.isEmpty()) {
            throw new IllegalArgumentException("weights cannot be null or empty");
        }
        if (limit == null || limit.isEmpty()) {
            throw new IllegalArgumentException("limit cannot be null or empty");
        }
    }

    private static List<RelDataTypeField> checkSchemas(CacheTable[] tables) {
        List<RelDataTypeField> referenceFields = tables[0].getDataFields();
        for (int i = 1; i < tables.length; i++) {
            DataTypeUtils.checkTableSchemaIdentical(referenceFields, tables[i].getDataFields(), i);
        }
        return referenceFields;
    }

    private static int resolvePrimaryKeyIndex(String primaryKey, List<RelDataTypeField> fields) {
        if (StringUtils.isEmpty(primaryKey)) {
            return -1;
        }
        int index = DataTypeUtils.findFieldIndex(fields, primaryKey.trim());
        if (index < 0) {
            throw new IllegalArgumentException("primaryKey field not found: " + primaryKey);
        }
        return index;
    }

    private static int parseLimit(String limit) {
        try {
            int limitNum = Integer.parseInt(limit.trim());
            if (limitNum <= 0) {
                throw new IllegalArgumentException("limit must be positive, got: " + limitNum);
            }
            return limitNum;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid limit: " + limit);
        }
    }

    private static int[] parseWeights(String weights, int tableCount) {
        String[] weightParts = weights.split(",");
        if (weightParts.length != tableCount) {
            throw new IllegalArgumentException("Number of weights (" + weightParts.length
                    + ") must match number of tables (" + tableCount + ")");
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
        return weightArray;
    }

    private static Iterable<Object[]>[] scanTables(CacheTable[] tables) {
        @SuppressWarnings("unchecked")
        Iterable<Object[]>[] sources = new Iterable[tables.length];
        for (int i = 0; i < tables.length; i++) {
            Enumerable<Object[]> rows = tables[i].scan(null);
            sources[i] = rows == null ? Linq4j.emptyEnumerable() : rows;
        }
        return sources;
    }
}
