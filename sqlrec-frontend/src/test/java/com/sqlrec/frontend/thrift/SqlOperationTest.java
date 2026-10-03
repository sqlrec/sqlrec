package com.sqlrec.frontend.thrift;

import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.executor.SqlProcessResult;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hive.service.rpc.thrift.TFetchOrientation;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlOperationTest {
    @Test
    void concurrentOrdinaryFetchesConsumeTheResultOnlyOnce() throws Exception {
        SqlOperation operation = new SqlOperation(SqlProcessResult.msg("result", "msg"), "query");
        List<SqlOperation.ResultPage> pages = fetchConcurrently(operation);
        long rowCount = pages.stream().mapToLong(page -> page.rows().getColumns().get(0).getStringVal().getValuesSize()).sum();
        assertEquals(1, rowCount);
        assertTrue(pages.stream().noneMatch(SqlOperation.ResultPage::hasMoreRows));
    }

    @Test
    void concurrentMetadataFetchesReturnDistinctPages() throws Exception {
        List<Object[]> rows = List.of(new Object[]{"one"}, new Object[]{"two"}, new Object[]{"three"});
        SqlOperation operation = SqlOperation.metadata(SqlProcessResult.of(
                Linq4j.asEnumerable(rows), DataTypeUtils.getStringTypeField("name")), "query");
        List<SqlOperation.ResultPage> pages = fetchConcurrently(operation);
        assertEquals(List.of(0L, 1L, 2L), pages.stream().map(page -> page.rows().getStartRowOffset()).sorted().toList());
        assertEquals(List.of("one", "three", "two"), pages.stream()
                .flatMap(page -> page.rows().getColumns().get(0).getStringVal().getValues().stream()).sorted().toList());
    }

    @Test
    void preservesAsyncCompletionAndRemembersPollingFailures() {
        SqlProcessResult result = mock(SqlProcessResult.class);
        when(result.isCompleted()).thenReturn(false, true).thenThrow(new IllegalStateException("execution failed"));
        SqlOperation operation = new SqlOperation(result, "query");
        assertEquals(TOperationState.RUNNING_STATE, operation.getState());
        assertEquals(TOperationState.FINISHED_STATE, operation.getState());
        assertEquals(TOperationState.ERROR_STATE, operation.getState());
        assertEquals(TOperationState.ERROR_STATE, operation.getState());
        assertTrue(operation.getMsg().contains("execution failed"));
        verify(result, times(3)).isCompleted();
    }

    @Test
    void ordinaryConversionFailureDoesNotConsumeTheResult() {
        assertConversionFailurePreservesRows(false);
    }

    @Test
    void metadataConversionFailureDoesNotAdvanceTheCursor() {
        assertConversionFailurePreservesRows(true);
    }

    @Test
    void failedRewindPreservesThePreviousMetadataPosition() {
        var fields = new ArrayList<>(List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER)));
        SqlOperation operation = SqlOperation.metadata(SqlProcessResult.of(
                Linq4j.asEnumerable(List.of(new Object[]{1}, new Object[]{2})), fields), "query");
        operation.fetch(TFetchOrientation.FETCH_NEXT, 1);
        fields.set(0, DataTypeUtils.getRelDataTypeField("id", 1, SqlTypeName.INTEGER));
        assertThrows(RuntimeException.class, () -> operation.fetch(TFetchOrientation.FETCH_FIRST, 1));
        fields.set(0, DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        SqlOperation.ResultPage next = operation.fetch(TFetchOrientation.FETCH_NEXT, 1);
        assertEquals(1, next.rows().getStartRowOffset());
        assertEquals(List.of(2), next.rows().getColumns().get(0).getI32Val().getValues());
    }

    @Test
    void ordinaryFetchIgnoresMetadataPaginationArguments() {
        SqlOperation operation = new SqlOperation(SqlProcessResult.of(
                Linq4j.asEnumerable(List.of(new Object[]{"one"}, new Object[]{"two"})),
                DataTypeUtils.getStringTypeField("name")), "query");

        SqlOperation.ResultPage page = operation.fetch(TFetchOrientation.FETCH_PRIOR, 0);

        assertEquals(List.of("one", "two"), page.rows().getColumns().get(0).getStringVal().getValues());
        assertFalse(page.hasMoreRows());
        assertTrue(operation.fetch(null, -1).rows().getColumns().get(0).getStringVal().getValues().isEmpty());
    }

    @Test
    void metadataValidatesOrientationBeforePageSizeWithoutReadingRows() {
        SqlProcessResult result = mock(SqlProcessResult.class);
        SqlOperation operation = SqlOperation.metadata(result, "query");

        assertEquals("Only FETCH_NEXT and FETCH_FIRST are supported locally",
                assertThrows(UnsupportedOperationException.class,
                        () -> operation.fetch(null, 0)).getMessage());
        assertThrows(UnsupportedOperationException.class,
                () -> operation.fetch(TFetchOrientation.FETCH_PRIOR, 1));
        for (long maxRows : new long[]{0, -1}) {
            assertEquals("maxRows must be positive", assertThrows(IllegalArgumentException.class,
                    () -> operation.fetch(TFetchOrientation.FETCH_NEXT, maxRows)).getMessage());
        }
        verifyNoInteractions(result);
    }

    @Test
    void metadataCanFetchPastTheEndAndRewindWithAnUnboundedPageSize() {
        SqlOperation operation = SqlOperation.metadata(SqlProcessResult.of(
                Linq4j.asEnumerable(List.of(new Object[]{"one"}, new Object[]{"two"})),
                DataTypeUtils.getStringTypeField("name")), "query");
        SqlOperation.ResultPage first = operation.fetch(TFetchOrientation.FETCH_NEXT, 1);
        assertTrue(first.hasMoreRows());
        assertEquals(0, first.rows().getStartRowOffset());
        SqlOperation.ResultPage second = operation.fetch(TFetchOrientation.FETCH_NEXT, Long.MAX_VALUE);
        assertEquals(List.of("two"), second.rows().getColumns().get(0).getStringVal().getValues());
        assertEquals(1, second.rows().getStartRowOffset());
        assertFalse(second.hasMoreRows());
        SqlOperation.ResultPage exhausted = operation.fetch(TFetchOrientation.FETCH_NEXT, Long.MAX_VALUE);
        assertEquals(2, exhausted.rows().getStartRowOffset());
        assertTrue(exhausted.rows().getColumns().get(0).getStringVal().getValues().isEmpty());
        assertFalse(exhausted.hasMoreRows());
        SqlOperation.ResultPage rewind = operation.fetch(TFetchOrientation.FETCH_FIRST, Long.MAX_VALUE);
        assertEquals(0, rewind.rows().getStartRowOffset());
        assertEquals(List.of("one", "two"), rewind.rows().getColumns().get(0).getStringVal().getValues());
        assertFalse(rewind.hasMoreRows());
    }

    @Test
    void metadataWithNoRowsReturnsEmptyPagesForBothOrientations() {
        SqlOperation operation = SqlOperation.metadata(SqlProcessResult.of(
                null, DataTypeUtils.getStringTypeField("name")), "query");
        for (TFetchOrientation orientation : new TFetchOrientation[]{
                TFetchOrientation.FETCH_NEXT, TFetchOrientation.FETCH_FIRST}) {
            SqlOperation.ResultPage page = operation.fetch(orientation, 1);
            assertEquals(0, page.rows().getStartRowOffset());
            assertTrue(page.rows().getColumns().get(0).getStringVal().getValues().isEmpty());
            assertFalse(page.hasMoreRows());
        }
    }

    private static void assertConversionFailurePreservesRows(boolean metadata) {
        Object[] row = {1};
        var fields = new ArrayList<>(List.of(DataTypeUtils.getRelDataTypeField("id", 1, SqlTypeName.INTEGER)));
        SqlProcessResult result = SqlProcessResult.of(Linq4j.asEnumerable(List.<Object[]>of(row)), fields);
        SqlOperation operation = metadata ? SqlOperation.metadata(result, "query") : new SqlOperation(result, "query");
        assertThrows(RuntimeException.class, () -> operation.fetch(TFetchOrientation.FETCH_NEXT, 1));
        fields.set(0, DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        SqlOperation.ResultPage page = operation.fetch(TFetchOrientation.FETCH_NEXT, 1);
        assertEquals(0, page.rows().getStartRowOffset());
        assertEquals(List.of(1), page.rows().getColumns().get(0).getI32Val().getValues());
    }

    private static List<SqlOperation.ResultPage> fetchConcurrently(SqlOperation operation) throws Exception {
        var workers = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        try {
            var futures = new ArrayList<java.util.concurrent.Future<SqlOperation.ResultPage>>();
            for (int i = 0; i < 3; i++) {
                futures.add(workers.submit(() -> {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    return operation.fetch(TFetchOrientation.FETCH_NEXT, 1);
                }));
            }
            start.countDown();
            List<SqlOperation.ResultPage> pages = new ArrayList<>();
            for (var future : futures) {
                pages.add(future.get(5, TimeUnit.SECONDS));
            }
            return pages;
        } finally {
            start.countDown();
            workers.shutdownNow();
        }
    }
}
