package com.sqlrec.common.schema;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.common.utils.RowTransformUtils;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** A Calcite table with vector lookup support and a name for logging and metrics. */
public interface VectorSearchable extends Table {
    Logger log = LoggerFactory.getLogger(VectorSearchable.class);

    String getTableName();

    List<VectorSearchResult> searchByEmbeddingImpl(VectorSearchRequest request);

    /** Whether this backend can completely evaluate the join predicate during vector search. */
    default boolean supportsJoinFilter(
            RexNode condition, int leftFieldCount, int rightFieldCount) {
        return false;
    }

    default List<VectorSearchResult> searchByEmbedding(VectorSearchRequest request) {
        long startTime = System.nanoTime();
        long count = 0;
        String status = "success";

        try {
            List<VectorSearchResult> result = searchByEmbeddingImpl(request);
            if (result != null) {
                var fields = getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)).getFieldList();
                for (VectorSearchResult match : result) {
                    RowTransformUtils.convertRowType(match.getRow(), fields);
                }
                count = result.size();
            }
            return result;
        } catch (Throwable e) {
            log.error("searchByEmbedding table {} error", getTableName(), e);
            status = MetricsUtils.outcome(e, false);
            throw e;
        } finally {
            MetricsUtils.recordTableQuery(getTableName(), status, Consts.METRICS_TABLE_VECTOR_SEARCH_DURATION,
                    Consts.METRICS_TABLE_VECTOR_SEARCH_ROWS, System.nanoTime() - startTime, count);
        }
    }
}
