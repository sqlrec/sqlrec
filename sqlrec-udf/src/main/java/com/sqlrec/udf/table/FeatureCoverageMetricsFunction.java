package com.sqlrec.udf.table;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.MetricsUtils;
import io.micrometer.core.instrument.Tags;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class FeatureCoverageMetricsFunction {
    public Void evaluate(ExecuteContext context, String metricsName, CacheTable... tables) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        if (metricsName == null || metricsName.isEmpty()) {
            throw new IllegalArgumentException("metricsName cannot be null or empty");
        }
        if (tables == null || tables.length == 0) {
            throw new IllegalArgumentException("tables cannot be null or empty");
        }

        for (CacheTable table : tables) {
            if (table == null) {
                continue;
            }
            processTable(context, metricsName, table);
        }

        return null;
    }

    private void processTable(ExecuteContext context, String metricsName, CacheTable table) {
        String tableName = table.getTableName();
        List<RelDataTypeField> fields = table.getDataFields();
        Enumerable<Object[]> enumerable = table.scan(null);

        if (fields == null || fields.isEmpty()) {
            return;
        }

        int totalCount = 0;
        int[] fieldNonNullCounts = new int[fields.size()];

        if (enumerable != null) {
            for (Object[] row : enumerable) {
                totalCount++;
                for (int i = 0; i < fields.size(); i++) {
                    if (row != null && i < row.length && !isMissingValue(row[i])) {
                        fieldNonNullCounts[i]++;
                    }
                }
            }
        }

        if (totalCount == 0) {
            return;
        }

        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i).getName();
            double coverage = (double) fieldNonNullCounts[i] / totalCount;

            Tags tags = MetricsUtils.createTags(context.getMetricsTags(),
                    "table", tableName,
                    "field", fieldName);

            MetricsUtils.getCompositeMeterRegistry()
                    .summary(metricsName, tags)
                    .record(coverage);
        }
    }

    private boolean isMissingValue(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof Collection) {
            return ((Collection<?>) value).isEmpty();
        }
        if (value instanceof Map) {
            return ((Map<?, ?>) value).isEmpty();
        }
        return false;
    }
}
