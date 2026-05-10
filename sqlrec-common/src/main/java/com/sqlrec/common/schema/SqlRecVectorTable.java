package com.sqlrec.common.schema;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.MetricsUtils;
import io.micrometer.core.instrument.Tags;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class SqlRecVectorTable extends SqlRecKvTable {
    private static final Logger log = LoggerFactory.getLogger(SqlRecVectorTable.class);

    protected List<Object[]> searchByEmbeddingWithScoreImpl(
            Object[] leftValue,
            List<Float> embedding,
            String annFieldName,
            RexNode filterCondition,
            int limit,
            List<Integer> projectColumns) {
        throw new UnsupportedOperationException("searchByEmbeddingWithScore not support");
    }

    public List<Object[]> searchByEmbeddingWithScore(
            Object[] leftValue,
            List<Float> embedding,
            String annFieldName,
            RexNode filterCondition,
            int limit,
            List<Integer> projectColumns) {
        long startTime = System.currentTimeMillis();
        long count = 0;
        String status = "success";

        try {
            List<Object[]> result = searchByEmbeddingWithScoreImpl(
                    leftValue, embedding, annFieldName, filterCondition, limit, projectColumns);
            if (result != null) {
                count = result.size();
            }
            return result;
        } catch (Throwable e) {
            log.error("searchByEmbeddingWithScore table {} error", getTableName(), e);
            status = "error";
            throw e;
        } finally {
            Tags tags = MetricsUtils.createTags(java.util.Collections.emptyMap(), "table", getTableName(), "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_TABLE_VECTOR_SEARCH_DURATION, tags)
                    .record(System.currentTimeMillis() - startTime, java.util.concurrent.TimeUnit.MILLISECONDS);
            MetricsUtils.getCompositeMeterRegistry()
                    .summary(Consts.METRICS_TABLE_VECTOR_SEARCH_DATA_SIZE, tags)
                    .record(count);
        }
    }
}
