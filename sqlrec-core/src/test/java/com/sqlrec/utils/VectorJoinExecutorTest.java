package com.sqlrec.utils;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.VectorSearchRequest;
import com.sqlrec.common.schema.VectorSearchResult;
import com.sqlrec.common.schema.VectorSearchable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VectorJoinExecutorTest {
    private boolean originalIgnoreJoinQueryException;

    @BeforeEach
    public void setUp() {
        originalIgnoreJoinQueryException =
                SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.getDefaultValue();
    }

    @AfterEach
    public void tearDown() {
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION
                .setDefaultValue(originalIgnoreJoinQueryException);
    }

    @Test
    public void joinsFullRightRowAndScore() {
        Object[] leftRow = new Object[]{1, Arrays.asList(0.1f, 0.2f)};
        VectorSearchable rightTable = mock(VectorSearchable.class);
        when(rightTable.searchByEmbedding(any())).thenReturn(Collections.singletonList(
                new VectorSearchResult(new Object[]{100, "book"}, 0.95d)));

        Enumerable<Object[]> result = VectorJoinExecutor.execute(
                Linq4j.asEnumerable(Collections.singletonList(leftRow)),
                rightTable,
                null,
                1,
                "embedding",
                10);

        assertArrayEquals(
                new Object[]{1, leftRow[1], 100, "book", 0.95d},
                result.single());
    }

    @Test
    public void passesFilterAndTopKToEveryLookup() {
        RexNode filterMarker = mock(RexNode.class);
        VectorSearchable rightTable = mock(VectorSearchable.class);
        when(rightTable.searchByEmbedding(any())).thenReturn(Collections.emptyList());
        Enumerable<Object[]> left = Linq4j.asEnumerable(Arrays.asList(
                new Object[]{1, Arrays.asList(0.1f, 0.2f)},
                new Object[]{2, Arrays.asList(0.3f, 0.4f)}));

        VectorJoinExecutor.execute(
                left, rightTable, filterMarker, 1, "item_embedding", 7).count();

        ArgumentCaptor<VectorSearchRequest> captor =
                ArgumentCaptor.forClass(VectorSearchRequest.class);
        verify(rightTable, org.mockito.Mockito.times(2)).searchByEmbedding(captor.capture());
        for (VectorSearchRequest request : captor.getAllValues()) {
            assertEquals("item_embedding", request.getVectorField());
            assertEquals(7, request.getTopK());
            assertEquals(filterMarker, request.getFilterCondition());
        }
        assertEquals(1, captor.getAllValues().get(0).getLeftRow()[0]);
        assertEquals(2, captor.getAllValues().get(1).getLeftRow()[0]);
    }

    @Test
    public void appliesTopKPerLeftRowInsteadOfGlobally() {
        VectorSearchable rightTable = mock(VectorSearchable.class);
        when(rightTable.searchByEmbedding(any())).thenAnswer(invocation -> {
            VectorSearchRequest request = invocation.getArgument(0);
            Object leftId = request.getLeftRow()[0];
            return Arrays.asList(
                    new VectorSearchResult(new Object[]{leftId + "-first"}, 0.9d),
                    new VectorSearchResult(new Object[]{leftId + "-second"}, 0.8d));
        });
        Enumerable<Object[]> left = Linq4j.asEnumerable(Arrays.asList(
                new Object[]{1, Arrays.asList(0.1f, 0.2f)},
                new Object[]{2, Arrays.asList(0.3f, 0.4f)}));

        List<Object[]> rows = VectorJoinExecutor.execute(
                left, rightTable, null, 1, "embedding", 2).toList();

        assertEquals(4, rows.size());
        assertArrayEquals(
                new Object[]{1, Arrays.asList(0.1f, 0.2f), "1-first", 0.9d},
                rows.get(0));
        assertArrayEquals(
                new Object[]{2, Arrays.asList(0.3f, 0.4f), "2-second", 0.8d},
                rows.get(3));
    }

    @Test
    public void usesConfiguredDefaultWhenLimitIsAbsent() {
        VectorSearchable rightTable = mock(VectorSearchable.class);
        when(rightTable.searchByEmbedding(any())).thenReturn(Collections.emptyList());

        VectorJoinExecutor.execute(
                Linq4j.asEnumerable(Collections.singletonList(
                        new Object[]{1, Arrays.asList(0.1f, 0.2f)})),
                rightTable,
                null,
                1,
                "embedding",
                0).count();

        ArgumentCaptor<VectorSearchRequest> captor =
                ArgumentCaptor.forClass(VectorSearchRequest.class);
        verify(rightTable).searchByEmbedding(captor.capture());
        assertEquals(
                SqlRecConfigs.DEFAULT_VECTOR_SEARCH_LIMIT.getValue(),
                captor.getValue().getTopK());
    }

    @Test
    public void skipsNullEmbeddingWithoutCallingRightTable() {
        VectorSearchable rightTable = mock(VectorSearchable.class);

        Enumerable<Object[]> result = VectorJoinExecutor.execute(
                Linq4j.asEnumerable(Collections.singletonList(new Object[]{1, null})),
                rightTable,
                null,
                1,
                "embedding",
                10);

        assertEquals(0, result.count());
        verify(rightTable, never()).searchByEmbedding(any());
    }

    @Test
    public void treatsNullSearchResultAsNoMatch() {
        VectorSearchable rightTable = mock(VectorSearchable.class);
        when(rightTable.searchByEmbedding(any())).thenReturn(null);

        Enumerable<Object[]> result = VectorJoinExecutor.execute(
                Linq4j.asEnumerable(Collections.singletonList(
                        new Object[]{1, Arrays.asList(0.1f, 0.2f)})),
                rightTable,
                null,
                1,
                "embedding",
                10);

        assertEquals(0, result.count());
    }

    @Test
    public void ignoresLookupFailureWhenConfigured() {
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(true);
        VectorSearchable rightTable = mock(VectorSearchable.class);
        doThrow(new RuntimeException("search failed"))
                .when(rightTable).searchByEmbedding(any());

        Enumerable<Object[]> result = VectorJoinExecutor.execute(
                Linq4j.asEnumerable(Collections.singletonList(
                        new Object[]{1, Arrays.asList(0.1f, 0.2f)})),
                rightTable,
                null,
                1,
                "embedding",
                10);

        assertEquals(0, result.count());
    }

    @Test
    public void rethrowsLookupFailureWhenConfigured() {
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(false);
        VectorSearchable rightTable = mock(VectorSearchable.class);
        doThrow(new RuntimeException("search failed"))
                .when(rightTable).searchByEmbedding(any());

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                VectorJoinExecutor.execute(
                        Linq4j.asEnumerable(Collections.singletonList(
                                new Object[]{1, Arrays.asList(0.1f, 0.2f)})),
                        rightTable,
                        null,
                        1,
                        "embedding",
                        10).count());

        assertEquals("search failed", exception.getMessage());
    }

    @Test
    public void rejectsMissingLeftInput() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VectorJoinExecutor.execute(null, null, null, 0, "embedding", 10));

        assertEquals("left input is null", exception.getMessage());
    }

    @Test
    public void rejectsMissingRightTable() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VectorJoinExecutor.execute(
                        Linq4j.emptyEnumerable(), null, null, 0, "embedding", 10));

        assertEquals("right table is null", exception.getMessage());
    }

    @Test
    public void rejectsEmbeddingIndexOutsideLeftRow() {
        VectorSearchable rightTable = mock(VectorSearchable.class);
        Enumerable<Object[]> left = Linq4j.asEnumerable(Collections.singletonList(
                new Object[]{1, Arrays.asList(0.1f, 0.2f)}));

        IllegalArgumentException negative = assertThrows(
                IllegalArgumentException.class,
                () -> VectorJoinExecutor.execute(
                        left, rightTable, null, -1, "embedding", 10).count());
        assertEquals("leftEmbeddingIndex is out of bounds: -1", negative.getMessage());

        IllegalArgumentException tooLarge = assertThrows(
                IllegalArgumentException.class,
                () -> VectorJoinExecutor.execute(
                        left, rightTable, null, 2, "embedding", 10).count());
        assertEquals("leftEmbeddingIndex is out of bounds: 2", tooLarge.getMessage());
        verify(rightTable, never()).searchByEmbedding(any());
    }

    @Test
    public void supportsScalarEnumerableRows() {
        VectorSearchable rightTable = mock(VectorSearchable.class);
        when(rightTable.searchByEmbedding(any())).thenReturn(Collections.singletonList(
                new VectorSearchResult(new Object[]{100}, 0.95d)));
        List<Float> embedding = Arrays.asList(0.1f, 0.2f);

        Object[] row = VectorJoinExecutor.execute(
                Linq4j.asEnumerable(Collections.singletonList(embedding)),
                rightTable,
                null,
                0,
                "embedding",
                1).single();

        assertArrayEquals(new Object[]{embedding, 100, 0.95d}, row);
    }
}
