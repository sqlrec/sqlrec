package com.sqlrec.connectors.milvus.flink;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.handler.MilvusHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Mock unit tests for the async-flushing Milvus Flink sink: verify buffering
 * until batch-size, flush on checkpoint / close, RowKind routing and async
 * error propagation.
 */
@ExtendWith(MockitoExtension.class)
class MilvusSinkFunctionTest {

    @Mock
    MilvusHandler mockHandler;

    private MilvusSinkFunction<RowData> sink;

    @BeforeEach
    void setUp() throws Exception {
        MilvusConfig config = new MilvusConfig();
        config.url = "http://localhost:19530";
        config.database = "db";
        config.collection = "t";
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "string"),
                new FieldSchema("name", "string"));
        config.batchSize = 2;
        // 1 hour: keeps the interval-based flush out of these tests
        config.flushInterval = 3600L;

        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("id", DataTypes.STRING()),
                Column.physical("name", DataTypes.STRING()));

        sink = new MilvusSinkFunction<>(config, schema);
        sink.setMilvusHandlerForTest(mockHandler);
        sink.open(new Configuration());
    }

    @AfterEach
    void tearDown() throws Exception {
        // best effort cleanup: swallow failures from intentionally-broken sinks
        try {
            sink.close();
        } catch (Exception ignored) {
            // expected in failure-propagation tests
        }
    }

    private RowData row(RowKind kind, String id, String name) {
        GenericRowData rowData = new GenericRowData(kind, 2);
        rowData.setField(0, StringData.fromString(id));
        rowData.setField(1, StringData.fromString(name));
        return rowData;
    }

    @Test
    void testUpsertBufferFlushedOnBatchSize() throws Exception {
        sink.invoke(row(RowKind.INSERT, "k1", "v1"), null);
        // below batch size: nothing submitted yet
        verify(mockHandler, never()).addBatch(anyList());

        sink.invoke(row(RowKind.INSERT, "k2", "v2"), null);
        // snapshotState waits for the async flush to finish, making the
        // verification deterministic
        sink.snapshotState(null);
        verify(mockHandler).addBatch(argThat(rows -> rows.size() == 2
                && "k1".equals(rows.get(0)[0]) && "k2".equals(rows.get(1)[0])));
    }

    @Test
    void testDeleteBufferFlushedOnBatchSize() throws Exception {
        sink.invoke(row(RowKind.DELETE, "k1", "v1"), null);
        sink.invoke(row(RowKind.DELETE, "k2", "v2"), null);
        sink.snapshotState(null);

        verify(mockHandler).removeBatch(argThat(rows -> rows.size() == 2));
        verify(mockHandler, never()).addBatch(anyList());
    }

    @Test
    void testUpdateAfterGoesToUpsertBuffer() throws Exception {
        sink.invoke(row(RowKind.UPDATE_AFTER, "k1", "v1"), null);
        sink.invoke(row(RowKind.UPDATE_AFTER, "k2", "v2"), null);
        sink.snapshotState(null);

        verify(mockHandler).addBatch(argThat(rows -> rows.size() == 2));
    }

    @Test
    void testUpdateBeforeIgnored() throws Exception {
        sink.invoke(row(RowKind.UPDATE_BEFORE, "k1", "v1"), null);
        sink.snapshotState(null);

        verify(mockHandler, never()).addBatch(anyList());
        verify(mockHandler, never()).removeBatch(anyList());
    }

    @Test
    void testCheckpointFlushesPartialBuffers() throws Exception {
        sink.invoke(row(RowKind.INSERT, "k1", "v1"), null);
        sink.invoke(row(RowKind.DELETE, "k2", "v2"), null);

        sink.snapshotState(null);
        verify(mockHandler).addBatch(argThat(rows -> rows.size() == 1));
        verify(mockHandler).removeBatch(argThat(rows -> rows.size() == 1));
    }

    @Test
    void testCloseFlushesRemainingBuffer() throws Exception {
        sink.invoke(row(RowKind.INSERT, "k1", "v1"), null);

        sink.close();
        verify(mockHandler).addBatch(argThat(rows -> rows.size() == 1));
    }

    @Test
    void testAsyncFlushFailurePropagatesToCheckpoint() throws Exception {
        doThrow(new RuntimeException("milvus down")).when(mockHandler).addBatch(anyList());

        sink.invoke(row(RowKind.INSERT, "k1", "v1"), null);
        sink.invoke(row(RowKind.INSERT, "k2", "v2"), null);

        // the async failure must surface at the next synchronous point
        Exception ex = assertThrows(Exception.class, () -> sink.snapshotState(null));
        // unwrap the execution wrapper to check the original cause
        Throwable cause = ex instanceof ExecutionException && ex.getCause() != null ? ex.getCause() : ex;
        assertTrue(cause.getMessage().contains("milvus down"));
    }
}
