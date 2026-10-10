package com.sqlrec.common.schema;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.common.utils.SilenceLoggers;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class VectorSearchableTest {
    private SimpleMeterRegistry metrics;

    @BeforeEach
    void attachMetrics() {
        metrics = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(metrics);
    }

    @AfterEach
    void detachMetrics() {
        MetricsUtils.getCompositeMeterRegistry().remove(metrics);
        metrics.close();
    }

    @Test
    void supportsCalciteTablesWithoutSqlRecTableInheritance() {
        List<VectorSearchResult> results = List.of(
                new VectorSearchResult(new Object[]{"42", 7}, 0.9));
        VectorSearchRequest request = new VectorSearchRequest(
                new Object[0], List.of(1.0f), "embedding", null, 1);
        AbstractVectorTable table = new AbstractVectorTable(results);

        List<VectorSearchResult> actual = table.searchByEmbedding(request);

        assertSame(results, actual);
        assertArrayEquals(new Object[]{42L, "7"}, actual.get(0).getRow());
        assertEquals(0.9, actual.get(0).getScore());
        assertSame(request, table.receivedRequest);
        assertFalse(table.supportsJoinFilter(null, 0, 0));
        assertMetrics("success", 1, 1);
    }

    @Test
    void handlesNullAndEmptyResultsAndRecordsZeroRows() {
        assertNull(new AbstractVectorTable(null).searchByEmbedding(request()));
        assertEquals(List.of(), new AbstractVectorTable(List.of()).searchByEmbedding(request()));
        assertMetrics("success", 2, 0);
    }

    @Test
    @SilenceLoggers(VectorSearchable.class)
    void preservesBackendFailureAndRecordsErrorMetrics() {
        RuntimeException failure = new IllegalStateException("backend unavailable");
        VectorSearchable table = new AbstractVectorTable(List.of()) {
            @Override
            public List<VectorSearchResult> searchByEmbeddingImpl(VectorSearchRequest request) {
                throw failure;
            }
        };

        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> table.searchByEmbedding(request())));
        assertMetrics("error", 1, 0);
    }

    @Test
    @SilenceLoggers(VectorSearchable.class)
    void propagatesRowConversionFailureAndRecordsErrorMetrics() {
        VectorSearchable table = new AbstractVectorTable(List.of(
                new VectorSearchResult(new Object[]{"invalid-number", "label"}, 0.9)));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> table.searchByEmbedding(request()));
        assertInstanceOf(NumberFormatException.class, failure.getCause());
        assertMetrics("error", 1, 0);
    }

    private void assertMetrics(String status, long calls, double rows) {
        String[] tags = {"table", "independent_vector_table", "status", status};
        assertEquals(calls, metrics.get(Consts.METRICS_TABLE_VECTOR_SEARCH_DURATION).tags(tags).timer().count());
        assertEquals(calls, metrics.get(Consts.METRICS_TABLE_VECTOR_SEARCH_DATA_SIZE).tags(tags).summary().count());
        assertEquals(rows, metrics.get(Consts.METRICS_TABLE_VECTOR_SEARCH_DATA_SIZE).tags(tags).summary().totalAmount());
    }

    private static VectorSearchRequest request() {
        return new VectorSearchRequest(new Object[0], List.of(1.0f), "embedding", null, 1);
    }

    private static class AbstractVectorTable extends AbstractTable implements VectorSearchable {
        private final List<VectorSearchResult> results;
        private VectorSearchRequest receivedRequest;

        private AbstractVectorTable(List<VectorSearchResult> results) {
            this.results = results;
        }

        @Override
        public String getTableName() {
            return "independent_vector_table";
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", SqlTypeName.BIGINT)
                    .add("label", SqlTypeName.VARCHAR)
                    .build();
        }

        @Override
        public List<VectorSearchResult> searchByEmbeddingImpl(VectorSearchRequest request) {
            receivedRequest = request;
            return results;
        }
    }
}
