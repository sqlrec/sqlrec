package com.sqlrec.connectors.milvus.calcite;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.handler.MilvusHandler;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

class MilvusCollectionTest {
    private MilvusCalciteTable.MilvusCollection collection(MilvusHandler handler, Integer batchSize) {
        MilvusConfig config = new MilvusConfig();
        config.batchSize = batchSize;
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Collections.singletonList(new FieldSchema("id", "BIGINT"));
        MilvusCalciteTable table = new MilvusCalciteTable(config);
        table.setTableName("batch_vectors");
        return new MilvusCalciteTable.MilvusCollection(table, handler);
    }

    @Test
    void bulkInsertUsesBoundedBatchesAndCountsAllRows() {
        MilvusHandler handler = mock(MilvusHandler.class);
        when(handler.addBatch(anyList())).thenReturn(true);
        MilvusCalciteTable.MilvusCollection collection = collection(handler, 2);
        List<Object[]> rows = Arrays.asList(new Object[]{1L}, new Object[]{2L}, new Object[]{3L});
        assertTrue(collection.addAll(rows));
        assertEquals(3, collection.size());
        ArgumentCaptor<List<Object[]>> batches = ArgumentCaptor.forClass(List.class);
        verify(handler, times(2)).addBatch(batches.capture());
        assertEquals(rows.subList(0, 2), batches.getAllValues().get(0));
        assertEquals(rows.subList(2, 3), batches.getAllValues().get(1));
        verify(handler, never()).add(any());
    }

    @Test
    void emptyInsertDoesNotSendARequest() {
        MilvusHandler handler = mock(MilvusHandler.class);
        MilvusCalciteTable.MilvusCollection collection = collection(handler, 2);
        assertFalse(collection.addAll(Collections.emptyList()));
        assertEquals(0, collection.size());
        verifyNoInteractions(handler);
    }

    @Test
    void failedRemoteWritePropagatesAndDoesNotReportSuccess() {
        MilvusHandler handler = mock(MilvusHandler.class);
        when(handler.addBatch(anyList())).thenThrow(new IllegalStateException("Remote write failed"));
        MilvusCalciteTable.MilvusCollection collection = collection(handler, null);
        assertThrows(IllegalStateException.class,
                () -> collection.addAll(Collections.singletonList(new Object[]{1L})));
        assertEquals(0, collection.size());
        verify(handler).addBatch(anyList());
    }
}
