package com.sqlrec.utils;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.VectorSearchRequest;
import com.sqlrec.common.schema.VectorSearchResult;
import com.sqlrec.common.schema.VectorSearchable;
import com.sqlrec.common.utils.DataTransformUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Executes a vector lookup join. Planner-specific extraction lives elsewhere. */
public final class VectorJoinExecutor {
    private static final Logger log = LoggerFactory.getLogger(VectorJoinExecutor.class);

    private VectorJoinExecutor() {
    }

    public static <T> Enumerable<Object[]> execute(
            Enumerable<T> left,
            VectorSearchable rightTable,
            Object pushedFilterObject,
            int leftEmbeddingIndex,
            String rightEmbeddingField,
            int topKPerLeftRow) {
        if (left == null) {
            throw new IllegalArgumentException("left input is null");
        }
        if (rightTable == null) {
            throw new IllegalArgumentException("right table is null");
        }

        RexNode pushedFilter = (RexNode) pushedFilterObject;
        final int effectiveTopK = topKPerLeftRow > 0
                ? topKPerLeftRow
                : SqlRecConfigs.DEFAULT_VECTOR_SEARCH_LIMIT.getValue();
        return left.selectMany(value -> lookupOne(
                value,
                rightTable,
                pushedFilter,
                leftEmbeddingIndex,
                rightEmbeddingField,
                effectiveTopK));
    }

    private static Enumerable<Object[]> lookupOne(
            Object value,
            VectorSearchable rightTable,
            RexNode pushedFilter,
            int leftEmbeddingIndex,
            String rightEmbeddingField,
            int topK) {
        Object[] leftRow = value instanceof Object[] ? (Object[]) value : new Object[]{value};
        if (leftEmbeddingIndex < 0 || leftEmbeddingIndex >= leftRow.length) {
            throw new IllegalArgumentException(
                    "leftEmbeddingIndex is out of bounds: " + leftEmbeddingIndex);
        }
        Object rawEmbedding = leftRow[leftEmbeddingIndex];
        if (rawEmbedding == null) {
            return Linq4j.emptyEnumerable();
        }

        VectorSearchRequest request = new VectorSearchRequest(
                leftRow,
                DataTransformUtils.convertToFloatVec(rawEmbedding),
                rightEmbeddingField,
                pushedFilter,
                topK);
        try {
            List<VectorSearchResult> matches = rightTable.searchByEmbedding(request);
            if (matches == null || matches.isEmpty()) {
                return Linq4j.emptyEnumerable();
            }
            return Linq4j.asEnumerable(matches).select(match -> join(leftRow, match));
        } catch (RuntimeException e) {
            if (!SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.getValue()) {
                throw e;
            }
            log.warn("Failed to execute vector lookup for one left row", e);
            return Linq4j.emptyEnumerable();
        }
    }

    private static Object[] join(Object[] leftRow, VectorSearchResult match) {
        Object[] rightRow = match.getRow();
        Object[] joined = new Object[leftRow.length + rightRow.length + 1];
        System.arraycopy(leftRow, 0, joined, 0, leftRow.length);
        System.arraycopy(rightRow, 0, joined, leftRow.length, rightRow.length);
        joined[joined.length - 1] = match.getScore();
        return joined;
    }
}
