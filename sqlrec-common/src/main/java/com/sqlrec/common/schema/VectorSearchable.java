package com.sqlrec.common.schema;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import io.micrometer.core.instrument.Tags;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public interface VectorSearchable {
    Logger log = LoggerFactory.getLogger(VectorSearchable.class);

    List<Object[]> searchByEmbeddingWithScoreImpl(
            Object[] leftValue,
            List<Float> embedding,
            String annFieldName,
            RexNode filterCondition,
            int limit,
            List<Integer> projectColumns);

    default List<Object[]> searchByEmbeddingWithScore(
            Object[] leftValue,
            List<Float> embedding,
            String annFieldName,
            RexNode filterCondition,
            int limit,
            List<Integer> projectColumns) {
        SqlRecTable table = (SqlRecTable) this;
        long startTime = System.currentTimeMillis();
        long count = 0;
        String status = "success";

        try {
            List<Object[]> result = searchByEmbeddingWithScoreImpl(
                    leftValue, embedding, annFieldName, filterCondition, limit, projectColumns);
            if (result != null) {
                DataTypeUtils.convertRowTypes(
                        result,
                        table.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)).getFieldList()
                );
                count = result.size();
            }
            return result;
        } catch (Throwable e) {
            log.error("searchByEmbeddingWithScore table {} error", table.getTableName(), e);
            status = "error";
            throw e;
        } finally {
            Tags tags = MetricsUtils.createTags(java.util.Collections.emptyMap(), "table", table.getTableName(), "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_TABLE_VECTOR_SEARCH_DURATION, tags)
                    .record(System.currentTimeMillis() - startTime, java.util.concurrent.TimeUnit.MILLISECONDS);
            MetricsUtils.getCompositeMeterRegistry()
                    .summary(Consts.METRICS_TABLE_VECTOR_SEARCH_DATA_SIZE, tags)
                    .record(count);
        }
    }
}
